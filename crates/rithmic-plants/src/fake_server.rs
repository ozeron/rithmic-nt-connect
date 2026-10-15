//! A fake Rithmic R|Protocol venue for offline tests (feature `testing`).
//!
//! It listens on `ws://127.0.0.1:<port>` and speaks the same frames as Rithmic:
//! a 4-byte big-endian length followed by a protobuf message routed by its
//! `template_id`. The unmodified [`crate::Plants`] / [`crate::RithmicVenue`]
//! and `rithmic-rs` connect to it, so tests run the whole direct path, including
//! the venue decode, without credentials.
//!
//! `rithmic-rs` builds its responses itself and keeps them `#[non_exhaustive]`, so
//! nothing outside that crate can construct one. Speaking the wire protocol is
//! the one seam that reaches it.
//!
//! Every request gets an ok reply (template `request + 1`, `rp_code = ["0"]`)
//! unless a test scripts something else with [`FakeRithmicServer::script`]. Replies echo
//! the request's `user_msg`, which is how `rithmic-rs` correlates them.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rithmic_rs::prost::Message as _;
use rithmic_rs::rti;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

use crate::config::SessionConfig;
use rithmic_host::Env;

const TEMPLATE_ID: u32 = 154_467;
const USER_MSG: u32 = 132_760;
const RQ_HANDLER_RP_CODE: u32 = 132_764;
const RP_CODE: u32 = 132_766;

/// Request templates.
pub mod template {
    pub const LOGIN: i32 = 10;
    pub const LOGOUT: i32 = 12;
    pub const REFERENCE_DATA: i32 = 14;
    pub const HEARTBEAT: i32 = 18;
    pub const MARKET_DATA_UPDATE: i32 = 100;
    pub const FRONT_MONTH: i32 = 113;
    pub const ORDER_BOOK: i32 = 115;
    pub const TIME_BAR_UPDATE: i32 = 200;
    pub const TIME_BAR_REPLAY: i32 = 202;
    pub const TICK_BAR_REPLAY: i32 = 206;
    pub const ACCOUNT_LIST: i32 = 302;
    pub const SUBSCRIBE_ORDER_UPDATES: i32 = 308;
    pub const TRADE_ROUTES: i32 = 310;
    pub const NEW_ORDER: i32 = 312;
    pub const MODIFY_ORDER: i32 = 314;
    pub const CANCEL_ORDER: i32 = 316;
    pub const SHOW_ORDER_HISTORY_DATES: i32 = 318;
    pub const SHOW_ORDERS: i32 = 320;
    pub const SHOW_ORDER_HISTORY_SUMMARY: i32 = 324;
    pub const BRACKET_ORDER: i32 = 330;
    pub const SHOW_FILL_HISTORY: i32 = 3512;
    pub const UPDATE_TARGET_BRACKET_LEVEL: i32 = 332;
    pub const UPDATE_STOP_BRACKET_LEVEL: i32 = 334;
    pub const SUBSCRIBE_BRACKET_UPDATES: i32 = 336;
    pub const CANCEL_ALL_ORDERS: i32 = 346;
    pub const PNL_POSITION_UPDATES: i32 = 400;
    pub const PNL_POSITION_SNAPSHOT: i32 = 402;
}

/// The plant a connection logged in as (`RequestLogin.infra_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Plant {
    Ticker,
    Order,
    History,
    Pnl,
    Repository,
}

impl Plant {
    fn from_infra(infra: i32) -> Option<Self> {
        Some(match infra {
            1 => Plant::Ticker,
            2 => Plant::Order,
            3 => Plant::History,
            4 => Plant::Pnl,
            5 => Plant::Repository,
            _ => return None,
        })
    }
}

/// One request the venue received.
#[derive(Debug, Clone)]
pub struct Received {
    /// `None` before the connection logged in.
    pub plant: Option<Plant>,
    pub template_id: i32,
    pub body: Vec<u8>,
}

impl Received {
    /// Decode the request as its `rti` type.
    pub fn decode<M: rithmic_rs::prost::Message + Default>(&self) -> M {
        M::decode(self.body.as_slice()).expect("fake venue: request decodes as the given type")
    }
}

/// What the venue answers to one request.
#[derive(Debug, Clone)]
pub struct Reply {
    /// Encoded response messages, without `user_msg` (the venue appends it).
    frames: Vec<Vec<u8>>,
    delay: Duration,
    action: Action,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Action {
    Send,
    /// Never answer: the caller's request times out.
    Silent,
    /// Close the connection abruptly instead of answering.
    Drop,
}

impl Reply {
    /// An ok answer: template `request + 1` with `rp_code = ["0"]`.
    pub fn ok(request_template: i32) -> Self {
        Self::rp_code(request_template, &["0"])
    }

    /// A reject: template `request + 1` with `rp_code = [code, message]`.
    pub fn reject(request_template: i32, code: &str, message: &str) -> Self {
        Self::rp_code(request_template, &[code, message])
    }

    fn rp_code(request_template: i32, rp_code: &[&str]) -> Self {
        let mut frame = Vec::new();
        put_int32(&mut frame, TEMPLATE_ID, request_template + 1);
        for code in rp_code {
            put_string(&mut frame, RP_CODE, code);
        }
        Self::frames(vec![frame])
    }

    /// One response message, sent as built (set its `template_id` and `rp_code`).
    pub fn message<M: rithmic_rs::prost::Message>(message: &M) -> Self {
        Self::frames(vec![message.encode_to_vec()])
    }

    /// A multi-part replay: each row gets `rq_handler_rp_code = ["0"]`, then a
    /// final frame of `response_template` with `rp_code = ["0"]` ends it.
    pub fn rows<M: rithmic_rs::prost::Message>(response_template: i32, rows: &[M]) -> Self {
        let mut frames: Vec<Vec<u8>> = rows
            .iter()
            .map(|row| {
                let mut frame = row.encode_to_vec();
                put_string(&mut frame, RQ_HANDLER_RP_CODE, "0");
                frame
            })
            .collect();
        let mut end = Vec::new();
        put_int32(&mut end, TEMPLATE_ID, response_template);
        put_string(&mut end, RP_CODE, "0");
        frames.push(end);
        Self::frames(frames)
    }

    /// A replay whose rows arrive and whose last frame then reports `rp_code`
    /// (a venue error part-way through a reply).
    pub fn rows_then<M: rithmic_rs::prost::Message>(
        response_template: i32,
        rows: &[M],
        rp_code: &[&str],
    ) -> Self {
        let mut reply = Self::rows(response_template, rows);
        let mut end = Vec::new();
        put_int32(&mut end, TEMPLATE_ID, response_template);
        for code in rp_code {
            put_string(&mut end, RP_CODE, code);
        }
        *reply
            .frames
            .last_mut()
            .expect("rows end with a final frame") = end;
        reply
    }

    /// An ok answer to `request_template` followed by replayed messages (how
    /// `show_orders` / the order-history summary deliver their rows).
    pub fn ok_then_replay<M: rithmic_rs::prost::Message>(
        request_template: i32,
        replay: &[M],
    ) -> Self {
        let mut reply = Self::ok(request_template);
        reply
            .frames
            .extend(replay.iter().map(|m| m.encode_to_vec()));
        reply
    }

    /// Raw frames, each completed with the request's `user_msg`.
    pub fn frames(frames: Vec<Vec<u8>>) -> Self {
        Self {
            frames,
            delay: Duration::ZERO,
            action: Action::Send,
        }
    }

    /// Never answer.
    pub fn silent() -> Self {
        Self {
            frames: Vec::new(),
            delay: Duration::ZERO,
            action: Action::Silent,
        }
    }

    /// Drop the connection instead of answering.
    pub fn drop_connection() -> Self {
        Self {
            frames: Vec::new(),
            delay: Duration::ZERO,
            action: Action::Drop,
        }
    }

    /// Answer after `delay` (other requests on the connection are not held up).
    pub fn after(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

enum Out {
    Frame(Vec<u8>),
    Close,
}

struct Conn {
    plant: Option<Plant>,
    tx: mpsc::UnboundedSender<Out>,
    kill: Arc<Notify>,
}

impl Conn {
    fn kill(&self) {
        let _ = self.tx.send(Out::Close);
        self.kill.notify_one();
    }
}

#[derive(Default)]
struct State {
    /// One-shot replies, consumed in order, before `standing`.
    queued: HashMap<i32, VecDeque<Reply>>,
    /// Replies used for every matching request.
    standing: HashMap<i32, Reply>,
    received: Vec<Received>,
    conns: HashMap<u64, Conn>,
    next_conn: u64,
    /// Plants whose next connections are refused at the TCP accept.
    refuse: HashMap<Plant, usize>,
}

/// A running fake venue. Dropping it stops the listener and every connection.
pub struct FakeRithmicServer {
    addr: SocketAddr,
    state: Arc<Mutex<State>>,
    accept: JoinHandle<()>,
}

impl Drop for FakeRithmicServer {
    fn drop(&mut self) {
        self.accept.abort();
        let state = self.state.lock().expect("fake venue state");
        for conn in state.conns.values() {
            conn.kill();
        }
    }
}

impl FakeRithmicServer {
    /// Start listening on an ephemeral loopback port.
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake venue binds loopback");
        let addr = listener.local_addr().expect("fake venue address");
        let state = Arc::new(Mutex::new(State::default()));
        let accept_state = Arc::clone(&state);
        let accept = tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                tokio::spawn(serve(tcp, Arc::clone(&accept_state)));
            }
        });
        Self {
            addr,
            state,
            accept,
        }
    }

    /// `ws://127.0.0.1:<port>`.
    pub fn url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// A session config pointed at this venue, with an explicit account so no
    /// account discovery runs.
    pub fn session_config(&self) -> SessionConfig {
        SessionConfig::builder()
            .user("fake_user")
            .password("fake_password")
            .system_name("Rithmic Test")
            .url(self.url())
            .beta_url(self.url())
            .env(Env::Test)
            .fcm_id("FCM")
            .ib_id("IB")
            .account_id("ACCOUNT")
            .build()
            .expect("fake venue session config")
    }

    /// Answer the next matching request with `reply` (queued replies are used in
    /// order, before any standing reply).
    pub fn script(&self, request_template: i32, reply: Reply) {
        self.lock()
            .queued
            .entry(request_template)
            .or_default()
            .push_back(reply);
    }

    /// Answer every matching request with `reply`.
    pub fn always(&self, request_template: i32, reply: Reply) {
        self.lock().standing.insert(request_template, reply);
    }

    /// Answer every front-month request with `trading_symbol` on `exchange`.
    pub fn front_month(&self, root: &str, exchange: &str, trading_symbol: &str) {
        let mut m = rti::ResponseFrontMonthContract::default();
        m.template_id = template::FRONT_MONTH + 1;
        m.rp_code = vec!["0".into()];
        m.symbol = Some(root.into());
        m.exchange = Some(exchange.into());
        m.trading_symbol = Some(trading_symbol.into());
        m.trading_exchange = Some(exchange.into());
        m.is_front_month_symbol = Some(true);
        self.always(template::FRONT_MONTH, Reply::message(&m));
    }

    /// Push an unsolicited message (a notification, a tick) to every connection
    /// logged in as `plant`.
    pub fn push<M: rithmic_rs::prost::Message>(&self, plant: Plant, message: &M) {
        let frame = message.encode_to_vec();
        for conn in self.lock().conns.values() {
            if conn.plant == Some(plant) {
                let _ = conn.tx.send(Out::Frame(frame.clone()));
            }
        }
    }

    /// Close every connection logged in as `plant` without a goodbye.
    pub fn drop_plant(&self, plant: Plant) {
        let mut state = self.lock();
        state.conns.retain(|_, conn| {
            if conn.plant == Some(plant) {
                conn.kill();
                false
            } else {
                true
            }
        });
    }

    /// Refuse the next `count` logins as `plant` (the venue closes the socket on
    /// the login request).
    pub fn refuse_logins(&self, plant: Plant, count: usize) {
        *self.lock().refuse.entry(plant).or_default() += count;
    }

    /// Every request received so far, in arrival order.
    pub fn received(&self) -> Vec<Received> {
        self.lock().received.clone()
    }

    /// Requests of one template, in arrival order.
    pub fn received_of(&self, template_id: i32) -> Vec<Received> {
        self.received()
            .into_iter()
            .filter(|r| r.template_id == template_id)
            .collect()
    }

    /// Wait until `count` requests of `template_id` arrived (bounded).
    pub async fn wait_for(&self, template_id: i32, count: usize) -> Vec<Received> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let seen = self.received_of(template_id);
            if seen.len() >= count {
                return seen;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "fake venue: waited for {count} x template {template_id}, saw {}",
                seen.len()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Wait until exactly `count` connections are logged in as `plant` (bounded).
    pub async fn wait_connections(&self, plant: Plant, count: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while self.connections(plant) != count {
            assert!(
                tokio::time::Instant::now() < deadline,
                "fake venue: waited for {count} {plant:?} connections, have {}",
                self.connections(plant)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Number of live connections logged in as `plant`.
    pub fn connections(&self, plant: Plant) -> usize {
        self.lock()
            .conns
            .values()
            .filter(|c| c.plant == Some(plant))
            .count()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("fake venue state")
    }
}

async fn serve(tcp: TcpStream, state: Arc<Mutex<State>>) {
    let Ok(ws) = tokio_tungstenite::accept_async(tcp).await else {
        return;
    };
    let (mut sink, mut stream) = ws.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Out>();
    let kill = Arc::new(Notify::new());
    let id = {
        let mut s = state.lock().expect("fake venue state");
        s.next_conn += 1;
        let id = s.next_conn;
        s.conns.insert(
            id,
            Conn {
                plant: None,
                tx: tx.clone(),
                kill: Arc::clone(&kill),
            },
        );
        id
    };

    let writer = tokio::spawn(async move {
        while let Some(out) = rx.recv().await {
            match out {
                Out::Frame(body) => {
                    let mut buf = Vec::with_capacity(body.len() + 4);
                    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
                    buf.extend_from_slice(&body);
                    if sink.send(Message::Binary(buf.into())).await.is_err() {
                        break;
                    }
                }
                // Abrupt: drop the socket without a close handshake.
                Out::Close => break,
            }
        }
    });

    loop {
        let message = tokio::select! {
            next = stream.next() => match next {
                Some(Ok(message)) => message,
                _ => break,
            },
            _ = kill.notified() => break,
        };
        let data = match message {
            Message::Binary(data) => data,
            Message::Close(_) => break,
            _ => continue,
        };
        if data.len() < 4 {
            continue;
        }
        let body = data[4..].to_vec();
        let Ok(envelope) = rti::RequestLogout::decode(body.as_slice()) else {
            continue;
        };
        let template_id = envelope.template_id;
        let user_msg = envelope.user_msg;

        let reply = {
            let mut s = state.lock().expect("fake venue state");
            if template_id == template::LOGIN {
                let login = rti::RequestLogin::decode(body.as_slice()).unwrap_or_default();
                let plant = login.infra_type.and_then(Plant::from_infra);
                if let Some(plant) = plant {
                    if let Some(left) = s.refuse.get_mut(&plant).filter(|n| **n > 0) {
                        *left -= 1;
                        s.received.push(Received {
                            plant: Some(plant),
                            template_id,
                            body,
                        });
                        s.conns.remove(&id);
                        break;
                    }
                }
                if let Some(conn) = s.conns.get_mut(&id) {
                    conn.plant = plant;
                }
            }
            let plant = s.conns.get(&id).and_then(|c| c.plant);
            s.received.push(Received {
                plant,
                template_id,
                body,
            });
            next_reply(&mut s, template_id)
        };

        match reply.action {
            Action::Silent => continue,
            Action::Drop => break,
            Action::Send => {}
        }
        let frames: Vec<Vec<u8>> = reply
            .frames
            .into_iter()
            .map(|mut frame| {
                for msg in &user_msg {
                    put_string(&mut frame, USER_MSG, msg);
                }
                frame
            })
            .collect();
        if reply.delay.is_zero() {
            for frame in frames {
                let _ = tx.send(Out::Frame(frame));
            }
        } else {
            let tx = tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(reply.delay).await;
                for frame in frames {
                    let _ = tx.send(Out::Frame(frame));
                }
            });
        }
    }

    state.lock().expect("fake venue state").conns.remove(&id);
    drop(tx);
    writer.abort();
}

fn next_reply(state: &mut State, template_id: i32) -> Reply {
    if let Some(reply) = state
        .queued
        .get_mut(&template_id)
        .and_then(VecDeque::pop_front)
    {
        return reply;
    }
    if let Some(reply) = state.standing.get(&template_id) {
        return reply.clone();
    }
    default_reply(template_id)
}

/// The ok answer for requests nobody scripted: `request + 1` with
/// `rp_code = ["0"]` (for a replay that is an empty end-of-replay frame). Trade
/// routes answer with one default route per CME Group exchange, as the venue
/// does after an order-plant login, so orders route without scripting.
fn default_reply(template_id: i32) -> Reply {
    if template_id == template::TRADE_ROUTES {
        let routes: Vec<rti::ResponseTradeRoutes> = ["CME", "CBOT", "NYMEX", "COMEX"]
            .iter()
            .map(|exchange| {
                let mut m = rti::ResponseTradeRoutes::default();
                m.template_id = template::TRADE_ROUTES + 1;
                m.fcm_id = Some("FCM".into());
                m.ib_id = Some("IB".into());
                m.exchange = Some((*exchange).into());
                m.trade_route = Some("simulator".into());
                m.status = Some("UP".into());
                m.is_default = Some(true);
                m
            })
            .collect();
        return Reply::rows(template::TRADE_ROUTES + 1, &routes);
    }
    Reply::ok(template_id)
}

fn put_int32(buf: &mut Vec<u8>, tag: u32, value: i32) {
    rithmic_rs::prost::encoding::int32::encode(tag, &value, buf);
}

fn put_string(buf: &mut Vec<u8>, tag: u32, value: &str) {
    rithmic_rs::prost::encoding::string::encode(tag, &value.to_string(), buf);
}

/// Venue-shaped messages for tests, built from the real `rti` types.
pub mod frames {
    use rithmic_rs::rti;

    pub const LAST_TRADE: i32 = 150;
    pub const BEST_BID_OFFER: i32 = 151;
    pub const ORDER_BOOK: i32 = 156;
    pub const RITHMIC_ORDER_NOTIFICATION: i32 = 351;
    pub const EXCHANGE_ORDER_NOTIFICATION: i32 = 352;
    pub const INSTRUMENT_PNL: i32 = 450;
    pub const ACCOUNT_PNL: i32 = 451;

    pub fn last_trade(
        symbol: &str,
        exchange: &str,
        price: f64,
        size: i32,
        ssboe: i32,
    ) -> rti::LastTrade {
        let mut m = rti::LastTrade::default();
        m.template_id = LAST_TRADE;
        m.symbol = Some(symbol.into());
        m.exchange = Some(exchange.into());
        m.presence_bits = Some(1);
        m.trade_price = Some(price);
        m.trade_size = Some(size);
        m.aggressor = Some(1);
        m.ssboe = Some(ssboe);
        m.usecs = Some(0);
        m
    }

    /// A Rithmic-side order status notification (`notify_type`, `status` as the venue sends them).
    pub fn rithmic_order(
        basket_id: &str,
        user_tag: &str,
        notify_type: i32,
        status: &str,
    ) -> rti::RithmicOrderNotification {
        let mut m = rti::RithmicOrderNotification::default();
        m.template_id = RITHMIC_ORDER_NOTIFICATION;
        m.basket_id = Some(basket_id.into());
        m.user_tag = Some(user_tag.into());
        m.notify_type = Some(notify_type);
        m.status = Some(status.into());
        m.fcm_id = Some("FCM".into());
        m.ib_id = Some("IB".into());
        m.account_id = Some("ACCOUNT".into());
        m.symbol = Some("ESZ6".into());
        m.exchange = Some("CME".into());
        m.quantity = Some(1);
        m.ssboe = Some(1_790_000_000);
        m.usecs = Some(0);
        m
    }

    pub fn account_pnl(
        account_id: &str,
        day_pnl: &str,
        snapshot: bool,
    ) -> rti::AccountPnLPositionUpdate {
        let mut m = rti::AccountPnLPositionUpdate::default();
        m.template_id = ACCOUNT_PNL;
        m.is_snapshot = Some(snapshot);
        m.fcm_id = Some("FCM".into());
        m.ib_id = Some("IB".into());
        m.account_id = Some(account_id.into());
        m.day_pnl = Some(day_pnl.into());
        m.ssboe = Some(1_790_000_000);
        m.usecs = Some(0);
        m
    }

    /// One fill-history row.
    pub fn fill_row(
        fill_id: &str,
        basket_id: &str,
        price: f64,
        size: u64,
    ) -> rti::ResponseShowFillHistory {
        let mut m = rti::ResponseShowFillHistory::default();
        m.template_id = super::template::SHOW_FILL_HISTORY + 1;
        m.fill_id = Some(fill_id.into());
        m.basket_id = Some(basket_id.into());
        m.account_id = Some("ACCOUNT".into());
        m.symbol = Some("ESZ6".into());
        m.exchange = Some("CME".into());
        m.fill_price = Some(price);
        m.fill_size = Some(size);
        m.fill_date = Some("20261014".into());
        m.fill_time = Some("10:00:00".into());
        m
    }

    /// One tick row of a tick-bar replay (open/close time pair as the venue sends it).
    pub fn tick_row(
        symbol: &str,
        price: f64,
        volume: u64,
        ssboe: i32,
        usecs: i32,
    ) -> rti::ResponseTickBarReplay {
        let mut m = rti::ResponseTickBarReplay::default();
        m.template_id = super::template::TICK_BAR_REPLAY + 1;
        m.symbol = Some(symbol.into());
        m.exchange = Some("CME".into());
        m.open_price = Some(price);
        m.high_price = Some(price);
        m.low_price = Some(price);
        m.close_price = Some(price);
        m.volume = Some(volume);
        m.num_trades = Some(1);
        m.data_bar_ssboe = vec![ssboe, ssboe];
        m.data_bar_usecs = vec![usecs, usecs];
        m
    }
}
