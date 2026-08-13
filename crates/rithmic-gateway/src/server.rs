//! Accept loop: Handshake + Ready, gated RPC dispatch backed by
//! [`rithmic_plants::RithmicSession`], and per-client fan-out of pushed
//! events. Client disconnect is detach-only (refcount decrement); it never
//! tears down the parent's Rithmic plants.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, Mutex as TokioMutex};
use tokio::task::JoinHandle;

use rithmic_plants::history::parse_time_bar_type;
use rithmic_plants::{PlantSet, RithmicSession};

use crate::convert::{
    front_month_to_pb, history_bar_to_pb, history_tick_to_pb, order_key, pnl_key,
    reference_data_to_pb,
};
use crate::framing::encode_frame;
use crate::pb::{self, frame::Body, Ack, ErrorResponse, Frame, Handshake, Ready};
use crate::reconnect::ReconnectController;
use crate::subscriptions::{ClientId, FanoutHub, ParentGates, SharedFanout, SubKey};

/// Internal sentinel keys are not real venue symbols; a client detaching
/// from one never triggers a `RithmicSession::unsubscribe` call.
fn is_internal_key(key: &SubKey) -> bool {
    key.symbol.starts_with("__")
}

/// Shared gateway runtime state, held for the process lifetime.
pub struct GatewayState {
    pub gates: ParentGates,
    pub hub: SharedFanout,
    pub fingerprint: Fingerprint,
    pub ready: bool,
    /// `None` lets the accept loop and gate tests run without a live
    /// Rithmic connection; RPCs that need a plant just Ack as a no-op.
    pub session: Option<Arc<TokioMutex<RithmicSession>>>,
    pub reconnect: Arc<ReconnectController>,
}

/// Credential fingerprint advertised/checked at Handshake (consistency
/// check only — never authentication; see [`crate::pb::Handshake::auth_token`]).
#[derive(Debug, Clone)]
pub struct Fingerprint {
    pub user: String,
    pub system_name: String,
    pub url: String,
    pub env: String,
    pub account_id: String,
    pub fcm_id: String,
    pub ib_id: String,
}

impl Fingerprint {
    pub fn matches(&self, hs: &Handshake) -> bool {
        hs.user == self.user
            && hs.system_name == self.system_name
            && hs.url == self.url
            && (hs.env.is_empty()
                || hs.env.eq_ignore_ascii_case(&self.env)
                || env_aliases_match(&hs.env, &self.env))
    }
}

fn env_aliases_match(a: &str, b: &str) -> bool {
    fn canon(s: &str) -> &'static str {
        match s.to_ascii_lowercase().as_str() {
            "live" | "production" => "live",
            "demo" | "development" => "demo",
            "test" => "test",
            _ => "",
        }
    }
    let ca = canon(a);
    let cb = canon(b);
    !ca.is_empty() && ca == cb
}

/// Accept connections on `listener` forever, spawning one task per client.
pub async fn serve(listener: UnixListener, state: Arc<GatewayState>) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, state).await {
                eprintln!("gateway client error: {e}");
            }
        });
    }
}

async fn read_frame_bytes(stream: &mut UnixStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_bytes = [0u8; 4];
    match stream.read_exact(&mut len_bytes).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > crate::framing::MAX_FRAME_LEN as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok(Some(payload))
}

async fn write_frame(stream: &mut UnixStream, frame: &Frame) -> Result<(), String> {
    let payload = frame.encode_to_vec();
    let wire = encode_frame(&payload).map_err(|e| e.to_string())?;
    stream.write_all(&wire).await.map_err(|e| format!("write: {e}"))
}

/// Same-UID check for the v1 local-unix empty-token trust policy (R15 /
/// KTD16): unix `SO_PEERCRED` is a kernel fact, not client-supplied data.
fn same_uid_peer(stream: &UnixStream) -> bool {
    match stream.peer_cred() {
        Ok(cred) => {
            // SAFETY: getuid() has no preconditions and never fails.
            let our_uid = unsafe { libc::getuid() };
            cred.uid() == our_uid
        }
        Err(_) => false,
    }
}

enum OutMsg {
    Frame(Bytes),
    Overflow,
}

/// Per-connection fan-out bookkeeping: which topics this client forwards,
/// and the tasks pumping each topic's broadcast receiver into `out_tx`.
struct ClientCtx {
    #[allow(dead_code)]
    id: ClientId,
    subscribed: HashSet<SubKey>,
    forwarders: HashMap<SubKey, JoinHandle<()>>,
    out_tx: mpsc::Sender<OutMsg>,
}

impl ClientCtx {
    fn new(out_tx: mpsc::Sender<OutMsg>) -> Self {
        Self {
            id: ClientId::new(),
            subscribed: HashSet::new(),
            forwarders: HashMap::new(),
            out_tx,
        }
    }

    /// Attach this client to `key`'s fan-out topic (idempotent). The topic
    /// must already exist in the hub (caller subscribes hub interest first).
    async fn ensure_forwarder(&mut self, hub: &FanoutHub, key: &SubKey) {
        if self.subscribed.contains(key) {
            return;
        }
        if let Some(rx) = hub.subscribe_receiver(key).await {
            let handle = spawn_forwarder(rx, self.out_tx.clone());
            self.forwarders.insert(key.clone(), handle);
            self.subscribed.insert(key.clone());
        }
    }

    fn drop_forwarder(&mut self, key: &SubKey) {
        if let Some(handle) = self.forwarders.remove(key) {
            handle.abort();
        }
        self.subscribed.remove(key);
    }

    /// Detach-only disconnect: decrement refcounts for everything this
    /// client touched; only issue a venue unsubscribe when this was the
    /// last interested peer, and never for internal (pnl/order/other)
    /// sentinel topics, which have no per-symbol venue unsubscribe.
    async fn teardown(&mut self, state: &GatewayState) {
        for handle in self.forwarders.values() {
            handle.abort();
        }
        for key in self.subscribed.iter() {
            let last = state.reconnect.on_unsubscribe(key).await;
            if last && !is_internal_key(key) {
                if let Some(session) = &state.session {
                    let guard = session.lock().await;
                    let _ = guard.unsubscribe(&key.symbol, &key.exchange).await;
                }
            }
        }
    }
}

fn spawn_forwarder(
    mut rx: broadcast::Receiver<Bytes>,
    out_tx: mpsc::Sender<OutMsg>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(bytes) => {
                    if out_tx.send(OutMsg::Frame(bytes)).await.is_err() {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // Bounded queue overflow: signal disconnect for this
                    // client only; other subscribers are unaffected.
                    let _ = out_tx.send(OutMsg::Overflow).await;
                    return;
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    })
}

/// Encode a plant event as a wire frame and publish it to `key`'s topic.
pub async fn publish_event(hub: &FanoutHub, key: &SubKey, event: pb::Event) -> usize {
    let frame = Frame {
        request_id: 0,
        body: Some(Body::Event(event)),
    };
    let payload = frame.encode_to_vec();
    let wire = match encode_frame(&payload) {
        Ok(w) => w,
        Err(_) => return 0,
    };
    hub.publish(key, Bytes::from(wire)).await
}

async fn handle_client(mut stream: UnixStream, state: Arc<GatewayState>) -> Result<(), String> {
    let same_uid = same_uid_peer(&stream);

    let payload = read_frame_bytes(&mut stream)
        .await
        .map_err(|e| format!("read handshake: {e}"))?
        .ok_or_else(|| "client closed before handshake".to_string())?;
    let req = Frame::decode(payload.as_slice()).map_err(|e| format!("proto: {e}"))?;
    let hs = match req.body {
        Some(Body::Handshake(hs)) => hs,
        _ => return Err("first frame must be Handshake".into()),
    };
    if !state.fingerprint.matches(&hs) {
        let _ = write_frame(&mut stream, &error_frame(0, "fingerprint_mismatch", "handshake does not match this gateway's credential set")).await;
        return Err("fingerprint mismatch".into());
    }
    // v1 local policy (KTD16 / R15): empty auth_token is only trusted from a
    // same-UID unix peer; a non-empty token is accepted for future/remote use.
    if hs.auth_token.is_empty() && !same_uid {
        let _ = write_frame(&mut stream, &error_frame(0, "auth_required", "empty auth_token is only accepted from a same-UID unix peer")).await;
        return Err("auth required for non-local peer".into());
    }
    if !state.ready {
        let _ = write_frame(&mut stream, &error_frame(0, "not_ready", "gateway not Ready (plants not connected)")).await;
        return Err("gateway not ready".into());
    }

    write_frame(
        &mut stream,
        &Frame {
            request_id: 0,
            body: Some(Body::Ready(Ready {
                scopes: state.gates.scopes(),
                trading_enabled: state.gates.trading_enabled,
                cancel_all_enabled: state.gates.cancel_all_enabled,
            })),
        },
    )
    .await?;

    let (out_tx, mut out_rx) = mpsc::channel::<OutMsg>(crate::subscriptions::DEFAULT_QUEUE_CAP);
    let mut client = ClientCtx::new(out_tx);

    let loop_result: Result<(), String> = loop {
        tokio::select! {
            biased;
            frame = read_frame_bytes(&mut stream) => {
                match frame {
                    Ok(Some(payload)) => {
                        let req = match Frame::decode(payload.as_slice()) {
                            Ok(f) => f,
                            Err(e) => break Err(format!("proto: {e}")),
                        };
                        let request_id = req.request_id;
                        let Some(body) = req.body else { continue };
                        let close_after = matches!(body, Body::Disconnect(_));
                        let resp = dispatch(&state, &mut client, request_id, body).await;
                        if write_frame(&mut stream, &resp).await.is_err() {
                            break Ok(());
                        }
                        if close_after {
                            break Ok(());
                        }
                    }
                    Ok(None) => break Ok(()),
                    Err(e) => break Err(format!("read: {e}")),
                }
            }
            msg = out_rx.recv() => {
                match msg {
                    Some(OutMsg::Frame(bytes)) => {
                        if stream.write_all(&bytes).await.is_err() {
                            break Ok(());
                        }
                    }
                    Some(OutMsg::Overflow) => {
                        eprintln!("gateway: client overflowed its outbound queue; disconnecting");
                        break Ok(());
                    }
                    None => {}
                }
            }
        }
    };

    client.teardown(&state).await;
    loop_result
}

fn ack_frame(request_id: u64) -> Frame {
    Frame {
        request_id,
        body: Some(Body::Ack(Ack {})),
    }
}

fn error_frame(request_id: u64, code: &str, message: &str) -> Frame {
    Frame {
        request_id,
        body: Some(Body::Error(ErrorResponse {
            code: code.into(),
            message: message.into(),
        })),
    }
}

fn err_to_frame(request_id: u64, code: &str, err: impl std::fmt::Display) -> Frame {
    error_frame(request_id, code, &err.to_string())
}

/// Dispatch one request frame body to a response frame, applying parent
/// gates and (when a session is attached) calling through to
/// `rithmic_plants::RithmicSession`. Subscribe/unsubscribe bodies also
/// update this client's fan-out registration in `client`.
async fn dispatch(state: &GatewayState, client: &mut ClientCtx, request_id: u64, body: Body) -> Frame {
    match body {
        Body::Subscribe(req) => {
            let key = SubKey { symbol: req.symbol.clone(), exchange: req.exchange.clone() };
            let is_first = state.reconnect.on_subscribe(key.clone()).await;
            client.ensure_forwarder(&state.hub, &key).await;
            if is_first {
                if let Some(session) = &state.session {
                    let guard = session.lock().await;
                    if let Err(e) = guard.subscribe(&req.symbol, &req.exchange).await {
                        drop(guard);
                        client.drop_forwarder(&key);
                        state.reconnect.on_unsubscribe(&key).await;
                        return err_to_frame(request_id, "subscribe_failed", e);
                    }
                }
            }
            ack_frame(request_id)
        }
        Body::Unsubscribe(req) => {
            let key = SubKey { symbol: req.symbol.clone(), exchange: req.exchange.clone() };
            client.drop_forwarder(&key);
            let last = state.reconnect.on_unsubscribe(&key).await;
            if last {
                if let Some(session) = &state.session {
                    let guard = session.lock().await;
                    if let Err(e) = guard.unsubscribe(&req.symbol, &req.exchange).await {
                        return err_to_frame(request_id, "unsubscribe_failed", e);
                    }
                }
            }
            ack_frame(request_id)
        }
        Body::SubscribeBook(req) => {
            let key = SubKey { symbol: req.symbol.clone(), exchange: req.exchange.clone() };
            // No dedicated unsubscribe message for book summaries (v1); the
            // hub topic is created (or reused) so any client subscribed to
            // this symbol also receives the resulting OrderBook events.
            state.hub.add_interest(key.clone()).await;
            client.ensure_forwarder(&state.hub, &key).await;
            if let Some(session) = &state.session {
                let guard = session.lock().await;
                if let Err(e) = guard.subscribe_order_book_summary(&req.symbol, &req.exchange).await {
                    return err_to_frame(request_id, "subscribe_book_failed", e);
                }
            }
            ack_frame(request_id)
        }
        Body::RequestPlants(req) => match PlantSet::parse(&req.plants) {
            Ok(extra) => {
                if let Some(session) = &state.session {
                    let mut guard = session.lock().await;
                    if let Err(e) = guard.request_plants(extra).await {
                        return err_to_frame(request_id, "request_plants_failed", e);
                    }
                }
                ack_frame(request_id)
            }
            Err(e) => err_to_frame(request_id, "invalid_plants", e),
        },
        Body::GetFrontMonth(req) => {
            let Some(session) = &state.session else {
                return ack_frame(request_id);
            };
            let guard = session.lock().await;
            match guard.get_front_month(&req.symbol, &req.exchange).await {
                Ok(dto) => Frame {
                    request_id,
                    body: Some(Body::FrontMonthResponse(front_month_to_pb(dto))),
                },
                Err(e) => err_to_frame(request_id, "get_front_month_failed", e),
            }
        }
        Body::GetReferenceData(req) => {
            let Some(session) = &state.session else {
                return ack_frame(request_id);
            };
            let guard = session.lock().await;
            match guard.get_reference_data(&req.symbol, &req.exchange).await {
                Ok(dto) => Frame {
                    request_id,
                    body: Some(Body::ReferenceDataResponse(reference_data_to_pb(dto))),
                },
                Err(e) => err_to_frame(request_id, "get_reference_data_failed", e),
            }
        }
        Body::LoadTicks(req) => {
            let Some(session) = &state.session else {
                return Frame { request_id, body: Some(Body::LoadTicksResponse(pb::LoadTicksResponse::default())) };
            };
            let guard = session.lock().await;
            match guard
                .load_ticks_all(&req.symbol, &req.exchange, req.start_time_sec, req.end_time_sec)
                .await
            {
                Ok(ticks) => Frame {
                    request_id,
                    body: Some(Body::LoadTicksResponse(pb::LoadTicksResponse {
                        ticks: ticks.into_iter().map(history_tick_to_pb).collect(),
                    })),
                },
                Err(e) => err_to_frame(request_id, "load_ticks_failed", e),
            }
        }
        Body::LoadTimeBars(req) => {
            let Some(session) = &state.session else {
                return Frame { request_id, body: Some(Body::LoadTimeBarsResponse(pb::LoadTimeBarsResponse::default())) };
            };
            let bar_type = match parse_time_bar_type(req.bar_type) {
                Ok(bt) => bt,
                Err(e) => return err_to_frame(request_id, "invalid_bar_type", e),
            };
            let guard = session.lock().await;
            match guard
                .load_time_bars_all(&req.symbol, &req.exchange, bar_type, req.period, req.start_time_sec, req.end_time_sec)
                .await
            {
                Ok(bars) => Frame {
                    request_id,
                    body: Some(Body::LoadTimeBarsResponse(pb::LoadTimeBarsResponse {
                        bars: bars.into_iter().map(history_bar_to_pb).collect(),
                    })),
                },
                Err(e) => err_to_frame(request_id, "load_time_bars_failed", e),
            }
        }
        Body::SubscribeTimeBars(req) => {
            let key = SubKey { symbol: req.symbol.clone(), exchange: req.exchange.clone() };
            // bar_type/period are not part of SubKey; the (symbol, exchange)
            // topic is shared with tick MD (documented v1 simplification).
            state.hub.add_interest(key.clone()).await;
            client.ensure_forwarder(&state.hub, &key).await;
            if let Some(session) = &state.session {
                let guard = session.lock().await;
                if let Err(e) = guard.subscribe_time_bars(&req.symbol, &req.exchange, req.bar_type, req.period).await {
                    return err_to_frame(request_id, "subscribe_time_bars_failed", e);
                }
            }
            ack_frame(request_id)
        }
        Body::UnsubscribeTimeBars(req) => {
            if let Some(session) = &state.session {
                let guard = session.lock().await;
                if let Err(e) = guard.unsubscribe_time_bars(&req.symbol, &req.exchange, req.bar_type, req.period).await {
                    return err_to_frame(request_id, "unsubscribe_time_bars_failed", e);
                }
            }
            ack_frame(request_id)
        }
        Body::SubscribePnl(_) => {
            let key = pnl_key();
            let is_first = state.reconnect.on_subscribe(key.clone()).await;
            client.ensure_forwarder(&state.hub, &key).await;
            if is_first {
                if let Some(session) = &state.session {
                    let mut guard = session.lock().await;
                    if let Err(e) = guard.ensure_pnl_plant().await {
                        return err_to_frame(request_id, "subscribe_pnl_failed", e);
                    }
                    if let Err(e) = guard.subscribe_pnl().await {
                        return err_to_frame(request_id, "subscribe_pnl_failed", e);
                    }
                }
            }
            ack_frame(request_id)
        }
        Body::SubscribeOrderUpdates(_) => {
            // Order-plant login is a trading capability (KTD12): gate it the
            // same as EnsureOrder even though this RPC only asks for
            // notifications, so `RITHMIC_ENABLE_TRADING=0` cannot be
            // bypassed by subscribing instead of calling EnsureOrder.
            if !state.gates.trading_enabled {
                return error_frame(request_id, "trading_disabled", "subscribe_order_updates denied: parent trading disabled");
            }
            let key = order_key();
            let is_first = state.reconnect.on_subscribe(key.clone()).await;
            client.ensure_forwarder(&state.hub, &key).await;
            if is_first {
                if let Some(session) = &state.session {
                    let mut guard = session.lock().await;
                    if let Err(e) = guard.subscribe_order_updates().await {
                        return err_to_frame(request_id, "subscribe_order_updates_failed", e);
                    }
                }
            }
            ack_frame(request_id)
        }
        Body::EnsurePnl(_) => {
            if let Some(session) = &state.session {
                let mut guard = session.lock().await;
                if let Err(e) = guard.ensure_pnl_plant().await {
                    return err_to_frame(request_id, "ensure_pnl_failed", e);
                }
            }
            ack_frame(request_id)
        }
        Body::EnsureOrder(_) => {
            if !state.gates.trading_enabled {
                return error_frame(request_id, "trading_disabled", "ensure_order denied: parent trading disabled");
            }
            if let Some(session) = &state.session {
                let mut guard = session.lock().await;
                if let Err(e) = guard.ensure_order_plant().await {
                    return err_to_frame(request_id, "ensure_order_failed", e);
                }
            }
            ack_frame(request_id)
        }
        Body::PlaceOrder(req) => {
            if !state.gates.allow_place() {
                return error_frame(request_id, "trading_disabled", "place_order denied: parent trading disabled");
            }
            let Some(session) = &state.session else { return ack_frame(request_id) };
            let mut guard = session.lock().await;
            match guard
                .place_order(
                    &req.symbol,
                    &req.exchange,
                    &req.side,
                    &req.price_type,
                    req.quantity,
                    &req.user_tag,
                    req.price,
                    req.trigger_price,
                    &req.duration,
                    req.trail_by_ticks,
                    req.trail_by_price_id,
                )
                .await
            {
                Ok(()) => ack_frame(request_id),
                Err(e) => err_to_frame(request_id, "place_failed", e),
            }
        }
        Body::CancelOrder(req) => {
            if !state.gates.allow_place() {
                return error_frame(request_id, "trading_disabled", "cancel_order denied: parent trading disabled");
            }
            let Some(session) = &state.session else { return ack_frame(request_id) };
            let mut guard = session.lock().await;
            match guard.cancel_order(&req.basket_id).await {
                Ok(()) => ack_frame(request_id),
                Err(e) => err_to_frame(request_id, "cancel_failed", e),
            }
        }
        Body::ModifyOrder(req) => {
            if !state.gates.allow_place() {
                return error_frame(request_id, "trading_disabled", "modify_order denied: parent trading disabled");
            }
            let Some(session) = &state.session else { return ack_frame(request_id) };
            let mut guard = session.lock().await;
            match guard
                .modify_order(
                    &req.basket_id,
                    &req.symbol,
                    &req.exchange,
                    req.quantity,
                    &req.price_type,
                    req.price,
                    req.trigger_price,
                    req.trail_by_ticks,
                )
                .await
            {
                Ok(()) => ack_frame(request_id),
                Err(e) => err_to_frame(request_id, "modify_failed", e),
            }
        }
        Body::CancelAllOrders(_) => {
            if !state.gates.allow_cancel_all() {
                return error_frame(request_id, "cancel_all_denied", "cancel_all_orders denied: parent cancel_all disabled");
            }
            let Some(session) = &state.session else { return ack_frame(request_id) };
            let mut guard = session.lock().await;
            match guard.cancel_all_orders().await {
                Ok(()) => ack_frame(request_id),
                Err(e) => err_to_frame(request_id, "cancel_all_failed", e),
            }
        }
        Body::DisconnectPnl(_) => {
            client.drop_forwarder(&pnl_key());
            ack_frame(request_id)
        }
        Body::DisconnectOrder(_) => {
            client.drop_forwarder(&order_key());
            ack_frame(request_id)
        }
        Body::Disconnect(_) => ack_frame(request_id),
        // Responses / events are gateway→client only; a client sending one
        // back is a protocol misuse, not a crash.
        _ => error_frame(request_id, "unsupported", "unsupported request body"),
    }
}

/// Test helper: process one RPC against `gates` (no session, no socket) and
/// return the response body. Used by `tests/gates.rs`.
pub fn gate_rpc_for_test(gates: &ParentGates, body: Body) -> Body {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    rt.block_on(async {
        let hub: SharedFanout = Arc::new(FanoutHub::new(8));
        let state = GatewayState {
            gates: *gates,
            hub: hub.clone(),
            fingerprint: Fingerprint {
                user: String::new(),
                system_name: String::new(),
                url: String::new(),
                env: String::new(),
                account_id: String::new(),
                fcm_id: String::new(),
                ib_id: String::new(),
            },
            ready: true,
            session: None,
            reconnect: Arc::new(ReconnectController::new(hub)),
        };
        let (out_tx, _out_rx) = mpsc::channel::<OutMsg>(8);
        let mut client = ClientCtx::new(out_tx);
        dispatch(&state, &mut client, 1, body).await.body.expect("body")
    })
}

/// Rebroadcast a plant event to all interested fan-out subscribers, keyed by
/// symbol/exchange (or the internal pnl/order sentinel keys). Also feeds the
/// [`ReconnectController`]-visible hub so late subscribers still see the
/// venue-declared topic.
pub async fn publish_plant_event(hub: &FanoutHub, event: rithmic_plants::dto::PlantEvent) {
    let (key, wire_event) = crate::convert::plant_event_to_routed(event);
    publish_event(hub, &key, wire_event).await;
}
