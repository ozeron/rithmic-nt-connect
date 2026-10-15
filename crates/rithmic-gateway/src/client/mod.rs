//! `GatewaySession`: the Rust gateway client behind the [`Session`] trait.
//!
//! Attach-only (it never starts the parent) and credential-free (a client has
//! an auth token and a fingerprint, never the Rithmic password). All sessions in
//! a process for one fingerprint share one socket and one reader (`runtime`);
//! each `GatewaySession` is a facade with its own event queues.
//!
//! Outcome classes: a failure before send is a plain error; a failure after a
//! state-changing request was written is [`Error::OutcomeUnknown`] and is never
//! retried; read-only requests are retried once after a re-dial.

mod attach;
pub mod convert;
mod runtime;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use rithmic_host::dto::{
    AccountRmsInfoDto, FillHistoryDto, FrontMonthDto, HistoryBarDto, HistoryTickDto,
    OrderNotificationDto, PlantEvent, ProductRmsInfoDto, ReferenceDataDto, TimeBarProbeRow,
};
use rithmic_host::history::{bar_slice_secs, parse_time_bar_type, window_slices};
use rithmic_host::{
    AccountTriple, Error, ModifyOrder, PlaceBracketOrder, PlaceOrder, PlantSet, Result, Session,
};

use tokio::sync::broadcast;
use tokio::time::Instant;

use self::convert::Channel;
use self::runtime::{CallError, IntentKey, Queues, Release, Runtime, Take};
pub use self::runtime::{ReadyInfo, RecoveryFailed, TransportEvent, EVENT_QUEUE_CAP};
use crate::listen::{default_unix_path, ListenEndpoint};
use crate::pb::{self, frame::Body, Frame};

/// The typed body of `$frame` (`Body::$variant`), mapped by `$map`; any other
/// body is a protocol error naming the variant expected.
macro_rules! reply {
    ($frame:expr, $variant:ident, |$r:pat_param| $map:expr) => {
        match $frame.body {
            Some(Body::$variant($r)) => Ok($map),
            other => Err(unexpected(stringify!($variant), other)),
        }
    };
}

const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(30);
/// One plant history slice can exceed the normal dial timeout.
const DEFAULT_HISTORY_RPC_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_ATTACH_TIMEOUT: Duration = Duration::from_secs(90);

/// Automatic recovery after a transport loss (the Python `TransportRecovery` budget).
const DEFAULT_RECOVERY_ATTEMPTS: u32 = 8;
const DEFAULT_RECOVERY_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const DEFAULT_RECOVERY_MAX_BACKOFF: Duration = Duration::from_secs(15);

/// How the gateway client identifies the parent it attaches to. There is
/// deliberately no password field: only the parent holds Rithmic credentials.
#[derive(Clone)]
pub struct GatewayConfig {
    pub user: String,
    pub system_name: String,
    pub url: String,
    /// `Live` | `Demo` | `Test`.
    pub env: String,
    pub account_id: String,
    pub fcm_id: String,
    pub ib_id: String,
    auth_token: String,
    socket: PathBuf,
    /// Bounded wait for an already-starting parent and for a silent parent's Ready.
    pub attach_timeout: Duration,
    pub rpc_timeout: Duration,
    pub history_rpc_timeout: Duration,
    /// Require the credential flock to be held before accepting `Ready`
    /// (disable only for mock-parent tests).
    pub attest_flock: bool,
    /// Re-dial attempts after a transport loss before `RecoveryFailed`
    /// (0 disables automatic recovery; callers then recover via
    /// `ensure_transport_live`).
    pub recovery_attempts: u32,
    pub recovery_initial_backoff: Duration,
    pub recovery_max_backoff: Duration,
}

impl std::fmt::Debug for GatewayConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayConfig")
            .field("user", &self.user)
            .field("system_name", &self.system_name)
            .field("url", &self.url)
            .field("env", &self.env)
            .field("socket", &self.socket)
            .finish_non_exhaustive()
    }
}

impl GatewayConfig {
    /// A client config; the listen path defaults to the fingerprint's path.
    pub fn new(user: &str, system_name: &str, url: &str, env: &str) -> Self {
        Self {
            user: user.into(),
            system_name: system_name.into(),
            url: url.into(),
            env: env.into(),
            account_id: String::new(),
            fcm_id: String::new(),
            ib_id: String::new(),
            auth_token: String::new(),
            socket: default_unix_path(user, system_name, url, env),
            attach_timeout: DEFAULT_ATTACH_TIMEOUT,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
            history_rpc_timeout: DEFAULT_HISTORY_RPC_TIMEOUT,
            attest_flock: true,
            recovery_attempts: DEFAULT_RECOVERY_ATTEMPTS,
            recovery_initial_backoff: DEFAULT_RECOVERY_INITIAL_BACKOFF,
            recovery_max_backoff: DEFAULT_RECOVERY_MAX_BACKOFF,
        }
    }

    /// Use an explicit listen URL. Only `unix://` (or a bare absolute path) is
    /// supported; `tcp://` / `tls://` fail with a not-implemented error.
    pub fn with_listen(mut self, listen: &str) -> Result<Self> {
        let ListenEndpoint::Unix(path) =
            ListenEndpoint::parse(listen).map_err(|e| Error::Config(e.to_string()))?;
        self.socket = path;
        Ok(self)
    }

    pub fn with_auth_token(mut self, token: &str) -> Self {
        self.auth_token = token.into();
        self
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket
    }

    pub(crate) fn auth_token(&self) -> &str {
        &self.auth_token
    }
}

/// Take an event out of its shared handle: moved when this is the last holder,
/// cloned only when another facade's queue still references it.
fn own(event: Arc<PlantEvent>) -> PlantEvent {
    Arc::try_unwrap(event).unwrap_or_else(|shared| (*shared).clone())
}

/// How a request treats failure and retry.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Idempotent read: retried once after a re-dial.
    Read,
    /// State-changing command: never retried; post-send failure is unknown.
    Command,
    /// Subscription / lifecycle intent: not retried by the client (the parent
    /// refcounts intent and the adapter replays it after a transport re-arm).
    Intent,
}

/// A facade over the shared gateway runtime.
pub struct GatewaySession {
    runtime: Arc<Runtime>,
    queues: Arc<Queues>,
    facade_id: AtomicU64,
    connected: AtomicBool,
    /// The runtime `connect` ran on; `Drop` (which may run outside any runtime,
    /// e.g. from Python) spawns the facade's last releases there.
    rt_handle: StdMutex<Option<tokio::runtime::Handle>>,
}

impl GatewaySession {
    /// Create a session facade. No I/O happens until [`Session::connect`].
    pub fn new(cfg: GatewayConfig) -> Self {
        Self {
            runtime: Runtime::for_config(&cfg),
            queues: Arc::new(Queues::new()),
            facade_id: AtomicU64::new(0),
            connected: AtomicBool::new(false),
            rt_handle: StdMutex::new(None),
        }
    }

    /// Whether the parent has trading enabled (read-only; clients cannot elevate).
    pub fn trading_enabled(&self) -> bool {
        self.runtime.ready().trading_enabled
    }

    pub fn cancel_all_enabled(&self) -> bool {
        self.runtime.ready().cancel_all_enabled
    }

    pub fn scopes(&self) -> Vec<String> {
        self.runtime.ready().scopes
    }

    pub fn gateway_instance_id(&self) -> u64 {
        self.runtime.ready().gateway_instance_id
    }

    /// Incarnation of this process's connection; bumps on every (re)connect.
    pub fn transport_generation(&self) -> u64 {
        self.runtime.generation()
    }

    /// True while the shared socket to the parent is up.
    pub fn transport_connected(&self) -> bool {
        !self.runtime.is_down()
    }

    /// Down / Up notifications, so an adapter can re-arm intent after a re-dial.
    pub fn subscribe_transport(&self) -> broadcast::Receiver<TransportEvent> {
        self.runtime.subscribe_transport()
    }

    /// Reported when automatic recovery spent its attempt budget.
    pub fn subscribe_recovery_failed(&self) -> broadcast::Receiver<RecoveryFailed> {
        self.runtime.subscribe_recovery_failed()
    }

    /// Recover the transport if it is down (single-flight across all sessions)
    /// and return the current generation.
    pub async fn ensure_transport_live(&self) -> Result<u64> {
        self.runtime.ensure_live().await?;
        Ok(self.runtime.generation())
    }

    fn require_connected(&self) -> Result<()> {
        if self.connected.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(Error::NotConnected { plant: "gateway" })
        }
    }

    fn timeout_for(&self, history: bool) -> Duration {
        if history {
            self.runtime.cfg.history_rpc_timeout
        } else {
            self.runtime.cfg.rpc_timeout
        }
    }

    /// Send `body`, applying the retry / outcome rules for `mode`.
    async fn call(&self, body: Body, mode: Mode, history: bool) -> Result<Frame> {
        self.require_connected()?;
        let timeout = self.timeout_for(history);
        // At most one retry: a read after a lost reply, or any request that
        // never left (the connection closed before it was written).
        let mut retried = false;
        loop {
            // A state-changing request that never left is a definitive local
            // failure (`NotSent`), the same class `RithmicVenue` reports.
            let conn = match self.runtime.ensure_live().await {
                Ok(conn) => conn,
                Err(err) if mode == Mode::Command => return Err(Error::NotSent(err.to_string())),
                Err(err) => return Err(err),
            };
            match conn.call(body.clone(), timeout).await {
                Ok(frame) => return map_reply(frame),
                Err(CallError::NotSent(err)) => {
                    if retried {
                        return Err(match mode {
                            Mode::Command => Error::NotSent(err.to_string()),
                            _ => err,
                        });
                    }
                    retried = true;
                }
                Err(CallError::Sent(err)) => {
                    if mode == Mode::Read && !retried {
                        retried = true;
                        continue;
                    }
                    return Err(match mode {
                        Mode::Command => Error::OutcomeUnknown(err.to_string()),
                        _ => err,
                    });
                }
            }
        }
    }

    async fn ack(&self, body: Body, mode: Mode) -> Result<()> {
        reply!(self.call(body, mode, false).await?, Ack, |_| ())
    }

    fn facade(&self) -> u64 {
        self.facade_id.load(Ordering::SeqCst)
    }

    /// Send a subscription intent and record this facade's hold once the parent
    /// acknowledged it. Always sent, even when another facade holds the intent:
    /// a repeat subscribe is how a client retries a join a rebuild left degraded,
    /// and how it re-joins a parent that lost the connection's intents.
    async fn hold_intent(&self, key: IntentKey, body: Body) -> Result<()> {
        let _ordered = self.runtime.intent_lock(&key).await;
        self.ack(body, Mode::Intent).await?;
        self.runtime.intents().hold(self.facade(), key);
        Ok(())
    }

    /// Drop this facade's hold on an intent; the parent is told only when no
    /// other facade on the shared socket still holds it.
    async fn release_intent(&self, key: IntentKey, body: Body) -> Result<()> {
        let _ordered = self.runtime.intent_lock(&key).await;
        let facade = self.facade();
        let held_here = {
            let mut intents = self.runtime.intents();
            let held_here = intents.holds(facade, &key);
            if intents.release(facade, &key) == Release::Keep {
                return Ok(());
            }
            held_here
        };
        let sent = self.ack(body, Mode::Intent).await;
        if sent.is_err() && held_here {
            self.runtime.intents().hold(facade, key);
        }
        sent
    }

    /// Release this facade's order and bracket intents. The parent drops both
    /// together (`DisconnectOrder`), so it is told only when no other facade
    /// holds either; otherwise the order plant, and any bracket subscription on
    /// it, stay for the others.
    async fn release_order(&self) -> Result<()> {
        let _ordered = self.runtime.intent_lock(&IntentKey::Order).await;
        let facade = self.facade();
        let had = {
            let mut intents = self.runtime.intents();
            let had = [IntentKey::Order, IntentKey::Brackets]
                .into_iter()
                .filter(|k| intents.holds(facade, k))
                .collect::<Vec<_>>();
            intents.release(facade, &IntentKey::Order);
            intents.release(facade, &IntentKey::Brackets);
            if intents.order_group_held() {
                return Ok(());
            }
            had
        };
        let sent = self
            .ack(IntentKey::Order.release_body(), Mode::Intent)
            .await;
        if sent.is_err() {
            let mut intents = self.runtime.intents();
            for key in had {
                intents.hold(facade, key);
            }
        }
        sent
    }

    /// This facade is leaving: drop its holds and return whether it was the
    /// last facade, plus the releases the parent still needs (the intents only
    /// this facade held). Shared by `disconnect` and `Drop`.
    fn leave(&self) -> Option<(bool, Vec<IntentKey>)> {
        let id = self.facade_id.swap(0, Ordering::SeqCst);
        if id == 0 {
            return None;
        }
        let freed = self.runtime.intents().drop_facade(id);
        Some((self.runtime.unregister(id), freed))
    }

    /// Compatibility poll: a gap reads as [`Error::ChannelLagged`].
    async fn poll(&self, channel: Channel, timeout: Duration) -> Result<Option<PlantEvent>> {
        match self.poll_raw(channel, timeout).await? {
            Some(PlantEvent::Gap(g)) => Err(Error::ChannelLagged {
                plant: g.stream.plant(),
                skipped: g.skipped,
            }),
            other => Ok(other),
        }
    }

    /// Wait for the first event on `channel`; a gap (from the parent or from
    /// this client's own queue overflow) is returned as `PlantEvent::Gap`.
    async fn poll_raw(&self, channel: Channel, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.require_connected()?;
        self.queues.get(channel).arm();
        let queue = self.queues.get(channel);
        let deadline = Instant::now() + timeout;
        loop {
            // Arm the wakeup before checking, so a push between the check and
            // the wait cannot be missed.
            let notified = queue.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match queue.take() {
                Take::Event(event) => return Ok(Some(own(event))),
                Take::Lagged(skipped) => {
                    return Ok(Some(PlantEvent::Gap(rithmic_host::GapDto {
                        stream: channel.stream(),
                        skipped,
                    })))
                }
                Take::Empty => {}
            }
            if self.runtime.is_down() {
                return Err(Error::ChannelClosed {
                    plant: channel.plant(),
                });
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let _ = tokio::time::timeout(deadline - now, notified).await;
        }
    }

    async fn load_bar_slice(
        &self,
        symbol: &str,
        exchange: &str,
        start: i32,
        end: i32,
        bar_type: i32,
        period: i32,
    ) -> Result<Vec<HistoryBarDto>> {
        let body = Body::LoadTimeBars(pb::LoadTimeBarsRequest {
            symbol: symbol.into(),
            exchange: exchange.into(),
            bar_type,
            period,
            start_time_sec: start,
            end_time_sec: end,
        });
        // One call: the read retry in `call` and the parent's own slice retries
        // are the budget. Retrying here as well multiplied every timeout, and
        // each abandoned attempt kept a slow-lane slot busy on the parent.
        reply!(
            self.call(body, Mode::Read, true).await?,
            LoadTimeBarsResponse,
            |r| r
                .bars
                .into_iter()
                .map(convert::history_bar_from_pb)
                .collect()
        )
    }
}

impl Drop for GatewaySession {
    /// A facade dropped without `disconnect` still releases what only it held,
    /// or the parent would keep those subscriptions (and keep fanning them to
    /// this socket) until the last facade closed it. The releases run on the
    /// runtime the facade connected on.
    fn drop(&mut self) {
        let Some((last, freed)) = self.leave() else {
            return;
        };
        if last || freed.is_empty() {
            return;
        }
        let handle = self.rt_handle.lock().expect("rt handle").take();
        if let Some(handle) = handle.or_else(|| tokio::runtime::Handle::try_current().ok()) {
            let runtime = Arc::clone(&self.runtime);
            handle.spawn(async move { runtime.send_releases(freed).await });
        }
    }
}

fn unexpected(want: &str, got: Option<Body>) -> Error {
    Error::Protocol(format!("expected {want}, got {got:?}"))
}

/// Map an error frame to a typed error; pass every other reply through.
fn map_reply(frame: Frame) -> Result<Frame> {
    match frame.body {
        Some(Body::Error(e)) if e.code == "reconciliation_unavailable" => {
            Err(Error::ReconciliationUnavailable(e.message))
        }
        // Classes assigned at the source survive the wire as typed errors, so a
        // gateway client reports exactly what a direct session would.
        Some(Body::Error(e)) if e.code == "venue_rejected" => Err(Error::Rejected(e.message)),
        Some(Body::Error(e)) if e.code == "not_sent" => Err(Error::NotSent(e.message)),
        // The parent could not read the request, so it dispatched nothing.
        Some(Body::Error(e)) if e.code == crate::server::BAD_REQUEST => {
            Err(Error::NotSent(e.message))
        }
        // Older parents refuse requests with `not_ready` while rebuilding; nothing
        // reached the venue. (The handshake matches `not_ready` itself in runtime.rs.)
        Some(Body::Error(e)) if e.code == "not_ready" => Err(Error::NotSent(e.message)),
        Some(Body::Error(e)) if e.code == "venue_unknown" => Err(Error::OutcomeUnknown(e.message)),
        // The parent's handler failed part-way: whatever it did is unknown.
        Some(Body::Error(e)) if e.code == crate::server::INTERNAL => {
            Err(Error::OutcomeUnknown(e.message))
        }
        Some(Body::Error(e)) => Err(Error::Gateway {
            code: e.code,
            message: e.message,
        }),
        _ => Ok(frame),
    }
}

#[async_trait]
impl Session for GatewaySession {
    async fn connect(&self) -> Result<()> {
        if self.connected.load(Ordering::SeqCst) {
            return Err(Error::AlreadyConnected);
        }
        let id = self.runtime.register(Arc::clone(&self.queues));
        self.facade_id.store(id, Ordering::SeqCst);
        *self.rt_handle.lock().expect("rt handle") = tokio::runtime::Handle::try_current().ok();
        self.runtime.allow_recovery();
        match self.runtime.ensure_live().await {
            Ok(_) => {
                self.connected.store(true, Ordering::SeqCst);
                Ok(())
            }
            Err(err) => {
                // Partial connect tears down what started.
                self.runtime.unregister(id);
                self.facade_id.store(0, Ordering::SeqCst);
                Err(err)
            }
        }
    }

    async fn disconnect(&self) -> Result<()> {
        self.connected.store(false, Ordering::SeqCst);
        match self.leave() {
            // Last facade in the process: close the socket; the parent drops its
            // intents and stays up.
            Some((true, _)) => self.runtime.close().await,
            // Other facades keep the socket: release what only this one held.
            Some((false, freed)) => self.runtime.send_releases(freed).await,
            None => {}
        }
        Ok(())
    }

    async fn request_plants(&self, extra: PlantSet) -> Result<()> {
        self.ack(
            Body::RequestPlants(pb::RequestPlantsRequest {
                plants: extra.wire_name().into(),
            }),
            Mode::Intent,
        )
        .await
    }

    async fn ensure_order_plant(&self) -> Result<()> {
        self.ack(Body::EnsureOrder(pb::EnsureOrderRequest {}), Mode::Intent)
            .await
    }

    async fn ensure_pnl_plant(&self) -> Result<()> {
        self.ack(Body::EnsurePnl(pb::EnsurePnlRequest {}), Mode::Intent)
            .await
    }

    async fn disconnect_order_plant(&self) -> Result<()> {
        self.release_order().await
    }

    async fn disconnect_pnl_plant(&self) -> Result<()> {
        self.release_intent(
            IntentKey::Pnl,
            Body::DisconnectPnl(pb::DisconnectPnlRequest {}),
        )
        .await
    }

    async fn reset_ticker(&self) -> Result<()> {
        self.ack(
            Body::ResetTickerPlant(pb::ResetTickerPlantRequest {}),
            Mode::Intent,
        )
        .await
    }

    async fn resolved_account(&self) -> Option<AccountTriple> {
        let frame = self
            .call(
                Body::ResolvedAccount(pb::ResolvedAccountRequest {}),
                Mode::Read,
                false,
            )
            .await
            .ok()?;
        match frame.body {
            Some(Body::ResolvedAccountResponse(r)) => convert::resolved_account_from_pb(r),
            _ => None,
        }
    }

    async fn subscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.queues.get(Channel::Market).arm();
        self.hold_intent(
            IntentKey::Ticker(symbol.into(), exchange.into()),
            Body::Subscribe(pb::SubscribeRequest {
                symbol: symbol.into(),
                exchange: exchange.into(),
            }),
        )
        .await
    }

    async fn unsubscribe(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.release_intent(
            IntentKey::Ticker(symbol.into(), exchange.into()),
            Body::Unsubscribe(pb::UnsubscribeRequest {
                symbol: symbol.into(),
                exchange: exchange.into(),
            }),
        )
        .await
    }

    async fn subscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.queues.get(Channel::Market).arm();
        self.hold_intent(
            IntentKey::Book(symbol.into(), exchange.into()),
            Body::SubscribeBook(pb::SubscribeBookRequest {
                symbol: symbol.into(),
                exchange: exchange.into(),
            }),
        )
        .await
    }

    async fn unsubscribe_order_book_summary(&self, symbol: &str, exchange: &str) -> Result<()> {
        self.release_intent(
            IntentKey::Book(symbol.into(), exchange.into()),
            Body::UnsubscribeBook(pb::UnsubscribeBookRequest {
                symbol: symbol.into(),
                exchange: exchange.into(),
            }),
        )
        .await
    }

    async fn subscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()> {
        self.queues.get(Channel::History).arm();
        self.hold_intent(
            IntentKey::TimeBars(symbol.into(), exchange.into(), bar_type, period),
            Body::SubscribeTimeBars(pb::SubscribeTimeBarsRequest {
                symbol: symbol.into(),
                exchange: exchange.into(),
                bar_type,
                period,
            }),
        )
        .await
    }

    async fn unsubscribe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        bar_type: i32,
        period: i32,
    ) -> Result<()> {
        self.release_intent(
            IntentKey::TimeBars(symbol.into(), exchange.into(), bar_type, period),
            Body::UnsubscribeTimeBars(pb::UnsubscribeTimeBarsRequest {
                symbol: symbol.into(),
                exchange: exchange.into(),
                bar_type,
                period,
            }),
        )
        .await
    }

    async fn subscribe_pnl(&self) -> Result<()> {
        self.queues.get(Channel::Pnl).arm();
        self.hold_intent(
            IntentKey::Pnl,
            Body::SubscribePnl(pb::SubscribePnlRequest {}),
        )
        .await
    }

    async fn subscribe_order_updates(&self) -> Result<()> {
        self.queues.get(Channel::Order).arm();
        self.hold_intent(
            IntentKey::Order,
            Body::SubscribeOrderUpdates(pb::SubscribeOrderUpdatesRequest {}),
        )
        .await
    }

    async fn subscribe_bracket_updates(&self) -> Result<()> {
        self.queues.get(Channel::Order).arm();
        self.hold_intent(
            IntentKey::Brackets,
            Body::SubscribeBracketUpdates(pb::SubscribeBracketUpdatesRequest {}),
        )
        .await
    }

    async fn get_front_month(&self, symbol: &str, exchange: &str) -> Result<FrontMonthDto> {
        let body = Body::GetFrontMonth(pb::GetFrontMonthRequest {
            symbol: symbol.into(),
            exchange: exchange.into(),
        });
        reply!(
            self.call(body, Mode::Read, false).await?,
            FrontMonthResponse,
            |r| convert::front_month_from_pb(r)
        )
    }

    async fn get_reference_data(&self, symbol: &str, exchange: &str) -> Result<ReferenceDataDto> {
        let body = Body::GetReferenceData(pb::GetReferenceDataRequest {
            symbol: symbol.into(),
            exchange: exchange.into(),
        });
        reply!(
            self.call(body, Mode::Read, false).await?,
            ReferenceDataResponse,
            |r| convert::reference_data_from_pb(r)
        )
    }

    async fn load_ticks(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
    ) -> Result<Vec<HistoryTickDto>> {
        let body = Body::LoadTicks(pb::LoadTicksRequest {
            symbol: symbol.into(),
            exchange: exchange.into(),
            start_time_sec,
            end_time_sec,
        });
        reply!(
            self.call(body, Mode::Read, true).await?,
            LoadTicksResponse,
            |r| r
                .ticks
                .into_iter()
                .map(convert::history_tick_from_pb)
                .collect()
        )
    }

    /// Wide windows are split into calendar slices (the same lengths the direct
    /// path uses), each fetched with its own timeout, so one RPC never holds the
    /// parent's session lock for a whole multi-month window. The merged result
    /// is sorted by time with duplicate markers removed.
    async fn load_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
        bar_type: i32,
        period: i32,
    ) -> Result<Vec<HistoryBarDto>> {
        let period = period.max(1);
        let step = bar_slice_secs(parse_time_bar_type(bar_type)?, period);
        let mut merged: Vec<HistoryBarDto> = Vec::new();
        for (start, end) in window_slices(start_time_sec, end_time_sec, step) {
            merged.extend(
                self.load_bar_slice(symbol, exchange, start, end, bar_type, period)
                    .await?,
            );
        }
        merged.sort_by_key(|b| b.marker.unwrap_or(i32::MAX));
        // Adjacent slices share their boundary second: keep the first bar per marker.
        merged.dedup_by(|later, first| later.marker.is_some() && later.marker == first.marker);
        Ok(merged)
    }

    async fn probe_time_bars(
        &self,
        symbol: &str,
        exchange: &str,
        start_time_sec: i32,
        end_time_sec: i32,
        bar_type: i32,
        period: i32,
    ) -> Result<Vec<TimeBarProbeRow>> {
        let body = Body::ProbeTimeBars(pb::ProbeTimeBarsRequest {
            symbol: symbol.into(),
            exchange: exchange.into(),
            bar_type,
            period: period.max(1),
            start_time_sec,
            end_time_sec,
        });
        reply!(
            self.call(body, Mode::Read, true).await?,
            ProbeTimeBarsResponse,
            |r| r
                .rows
                .into_iter()
                .map(convert::time_bar_probe_row_from_pb)
                .collect()
        )
    }

    async fn load_orders(&self, start_sec: i32, end_sec: i32) -> Result<Vec<OrderNotificationDto>> {
        let body = Body::LoadOrders(pb::LoadOrdersRequest {
            start_time_sec: start_sec,
            end_time_sec: end_sec,
        });
        reply!(
            self.call(body, Mode::Read, false).await?,
            LoadOrdersResponse,
            |r| r
                .events
                .into_iter()
                .map(convert::order_notification_from_pb)
                .collect()
        )
    }

    async fn load_product_rms_info(&self) -> Result<Vec<ProductRmsInfoDto>> {
        let body = Body::LoadProductRmsInfo(pb::LoadProductRmsInfoRequest {});
        reply!(
            self.call(body, Mode::Read, false).await?,
            LoadProductRmsInfoResponse,
            |r| r
                .rows
                .into_iter()
                .map(convert::product_rms_info_from_pb)
                .collect()
        )
    }

    async fn load_account_rms_info(&self) -> Result<Vec<AccountRmsInfoDto>> {
        let body = Body::LoadAccountRmsInfo(pb::LoadAccountRmsInfoRequest {});
        reply!(
            self.call(body, Mode::Read, false).await?,
            LoadAccountRmsInfoResponse,
            |r| r
                .rows
                .into_iter()
                .map(convert::account_rms_info_from_pb)
                .collect()
        )
    }

    async fn load_fill_history(&self, start: i32, finish: i32) -> Result<Vec<FillHistoryDto>> {
        let body = Body::LoadFillHistory(pb::LoadFillHistoryRequest {
            start_date: start,
            finish_date: finish,
        });
        reply!(
            self.call(body, Mode::Read, false).await?,
            LoadFillHistoryResponse,
            |r| r
                .rows
                .into_iter()
                .map(convert::fill_history_from_pb)
                .collect()
        )
    }

    async fn load_order_history(&self, date: &str) -> Result<Vec<OrderNotificationDto>> {
        let body = Body::LoadOrderHistory(pb::LoadOrderHistoryRequest {
            date: date.to_string(),
        });
        reply!(
            self.call(body, Mode::Read, false).await?,
            LoadOrderHistoryResponse,
            |r| r
                .events
                .into_iter()
                .map(convert::order_notification_from_pb)
                .collect()
        )
    }

    async fn load_order_history_dates(&self) -> Result<Vec<String>> {
        let body = Body::LoadOrderHistoryDates(pb::LoadOrderHistoryDatesRequest {});
        reply!(
            self.call(body, Mode::Read, false).await?,
            LoadOrderHistoryDatesResponse,
            |r| r.dates
        )
    }

    async fn place_order(&self, o: &PlaceOrder) -> Result<()> {
        self.ack(
            Body::PlaceOrder(pb::PlaceOrderRequest {
                symbol: o.symbol.clone(),
                exchange: o.exchange.clone(),
                side: o.side.clone(),
                price_type: o.price_type.clone(),
                quantity: o.quantity,
                user_tag: o.user_tag.clone(),
                price: o.price,
                trigger_price: o.trigger_price,
                duration: o.duration.clone(),
                trail_by_ticks: o.trail_by_ticks,
                trail_by_price_id: o.trail_by_price_id,
            }),
            Mode::Command,
        )
        .await
    }

    async fn place_bracket_order(&self, o: &PlaceBracketOrder) -> Result<()> {
        self.ack(
            Body::PlaceBracketOrder(pb::PlaceBracketOrderRequest {
                symbol: o.symbol.clone(),
                exchange: o.exchange.clone(),
                side: o.side.clone(),
                price_type: o.price_type.clone(),
                quantity: o.quantity,
                localid: o.localid.clone(),
                price: o.price,
                trigger_price: o.trigger_price,
                duration: o.duration.clone(),
                stop_ticks: o.stop_ticks,
                target_ticks: o.target_ticks,
            }),
            Mode::Command,
        )
        .await
    }

    async fn adjust_bracket_stop(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        self.ack(
            Body::AdjustBracketStop(pb::AdjustBracketStopRequest {
                basket_id: basket_id.into(),
                ticks,
                level,
            }),
            Mode::Command,
        )
        .await
    }

    async fn adjust_bracket_target(
        &self,
        basket_id: &str,
        ticks: i32,
        level: Option<i32>,
    ) -> Result<()> {
        self.ack(
            Body::AdjustBracketTarget(pb::AdjustBracketTargetRequest {
                basket_id: basket_id.into(),
                ticks,
                level,
            }),
            Mode::Command,
        )
        .await
    }

    async fn cancel_order(&self, basket_id: &str) -> Result<()> {
        self.ack(
            Body::CancelOrder(pb::CancelOrderRequest {
                basket_id: basket_id.into(),
            }),
            Mode::Command,
        )
        .await
    }

    async fn modify_order(&self, o: &ModifyOrder) -> Result<()> {
        self.ack(
            Body::ModifyOrder(pb::ModifyOrderRequest {
                basket_id: o.basket_id.clone(),
                symbol: o.symbol.clone(),
                exchange: o.exchange.clone(),
                quantity: o.quantity,
                price_type: o.price_type.clone(),
                price: o.price,
                trigger_price: o.trigger_price,
                trail_by_ticks: o.trail_by_ticks,
            }),
            Mode::Command,
        )
        .await
    }

    async fn cancel_all_orders(&self) -> Result<()> {
        self.ack(
            Body::CancelAllOrders(pb::CancelAllOrdersRequest {}),
            Mode::Command,
        )
        .await
    }

    async fn poll_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.poll(Channel::Market, timeout).await
    }

    async fn poll_history_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.poll(Channel::History, timeout).await
    }

    async fn poll_pnl_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.poll(Channel::Pnl, timeout).await
    }

    async fn poll_order_event(&self, timeout: Duration) -> Result<Option<PlantEvent>> {
        self.poll(Channel::Order, timeout).await
    }

    async fn next_events(
        &self,
        stream: rithmic_host::Stream,
        max: usize,
        timeout: Duration,
    ) -> Result<Vec<PlantEvent>> {
        let channel = Channel::of_stream(stream);
        let Some(first) = self.poll_raw(channel, timeout).await? else {
            return Ok(Vec::new());
        };
        let mut out = vec![first];
        let queue = self.queues.get(channel);
        while out.len() < max.max(1) {
            match queue.take() {
                Take::Event(event) => out.push(own(event)),
                Take::Lagged(skipped) => {
                    out.push(PlantEvent::Gap(rithmic_host::GapDto { stream, skipped }))
                }
                Take::Empty => break,
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod reply_class_tests {
    use super::*;

    fn error(code: &str) -> Frame {
        Frame {
            request_id: 1,
            body: Some(Body::Error(pb::ErrorResponse {
                code: code.into(),
                message: "m".into(),
            })),
        }
    }

    #[test]
    fn an_unreadable_request_was_not_sent() {
        assert!(matches!(
            map_reply(error(crate::server::BAD_REQUEST)),
            Err(Error::NotSent(_))
        ));
    }

    #[test]
    fn source_classes_survive_the_wire() {
        assert!(matches!(
            map_reply(error("venue_rejected")),
            Err(Error::Rejected(_))
        ));
        assert!(matches!(
            map_reply(error("venue_unknown")),
            Err(Error::OutcomeUnknown(_))
        ));
        assert!(matches!(
            map_reply(error("not_sent")),
            Err(Error::NotSent(_))
        ));
    }
}
