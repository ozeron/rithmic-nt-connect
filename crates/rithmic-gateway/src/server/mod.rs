//! Accept loop: Handshake + Ready, gated RPC dispatch backed by
//! [`rithmic_plants::RithmicSession`], and per-client fan-out of pushed
//! events. Client disconnect is detach-only (refcount decrement) while peers
//! remain; optional idle-exit shuts down the parent after the last Ready
//! peer leaves (see [`crate::idle_exit`]).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, watch, Mutex as TokioMutex, RwLock};
use tokio::task::JoinHandle;

use rithmic_plants::Session;

use crate::codec;
use crate::idle_exit::{IdleExit, IdleExitPolicy};
use crate::pb::{self, frame::Body, Frame, Handshake, Ready};
use crate::reconnect::{ReconnectController, TimeBarIntent};
use crate::subscriptions::{ClientId, Fan, FanoutHub, Kind, ParentGates, SharedFanout, SubKey};

mod dispatch;
pub mod pump;
use dispatch::TopicIntent;
pub use dispatch::{
    gate_rpc_for_test, history_rpc_with_live_ticker_intent_for_test, rpc_sequence_with_gates,
};

/// Metadata advertised in every ``Ready`` frame for this parent process.
///
/// Returns `(gateway_instance_id, transport_generation, transport_epoch)`.
/// `transport_epoch` notifies live clients when generation bumps so they
/// re-handshake instead of keeping a stale Ready generation.
pub fn boot_gateway_metadata() -> (u64, AtomicU64, watch::Sender<u64>) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        ^ (std::process::id() as u64);
    let (epoch_tx, _) = watch::channel(1u64);
    (id, AtomicU64::new(1), epoch_tx)
}

/// The parent's plant session behind the [`Session`] trait, guarded by one gate.
///
/// The gate only separates *using* the plants from *rebuilding* them. RPCs take it
/// shared ([`SessionHandle::lock`]) and run concurrently, so no call waits behind
/// another; reconnect and shutdown take it exclusively
/// ([`SessionHandle::lock_exclusive`]), which waits for in-flight calls and keeps
/// new ones out. Waiting for events needs no gate at all: the pump uses
/// [`SessionHandle::checkout`] once and then runs on the shared session.
#[derive(Clone)]
pub struct SessionHandle {
    gate: Arc<RwLock<()>>,
    session: Arc<dyn Session>,
}

/// Shared guard returned by [`SessionHandle::lock`]; derefs to the session. Any
/// number of RPCs hold it at once, so a slow call never queues the others.
pub struct SessionGuard<'a> {
    _gate: tokio::sync::RwLockReadGuard<'a, ()>,
    session: &'a Arc<dyn Session>,
}

impl std::ops::Deref for SessionGuard<'_> {
    type Target = dyn Session;

    fn deref(&self) -> &Self::Target {
        &**self.session
    }
}

/// Exclusive guard for rebuilding the plants (reconnect, shutdown): it waits for
/// in-flight RPCs to finish and keeps new ones out while it is held.
pub struct ExclusiveSessionGuard<'a> {
    _gate: tokio::sync::RwLockWriteGuard<'a, ()>,
    session: &'a Arc<dyn Session>,
}

impl std::ops::Deref for ExclusiveSessionGuard<'_> {
    type Target = dyn Session;

    fn deref(&self) -> &Self::Target {
        &**self.session
    }
}

impl SessionHandle {
    pub fn new(session: Arc<dyn Session>) -> Self {
        Self {
            gate: Arc::new(RwLock::new(())),
            session,
        }
    }

    /// Shared access: RPCs run concurrently and only wait while plants are being
    /// rebuilt.
    pub async fn lock(&self) -> SessionGuard<'_> {
        SessionGuard {
            _gate: self.gate.read().await,
            session: &self.session,
        }
    }

    /// Exclusive access, for disconnect / connect / restore.
    pub async fn lock_exclusive(&self) -> ExclusiveSessionGuard<'_> {
        ExclusiveSessionGuard {
            _gate: self.gate.write().await,
            session: &self.session,
        }
    }

    /// Wait out any rebuild, then return the session without holding the gate.
    pub async fn checkout(&self) -> Arc<dyn Session> {
        drop(self.gate.read().await);
        Arc::clone(&self.session)
    }
}

/// Shared gateway runtime state, held for the process lifetime.
pub struct GatewayState {
    pub gates: ParentGates,
    pub hub: SharedFanout,
    pub fingerprint: Fingerprint,
    /// Cleared while plants reconnect so new Handshakes get `not_ready`.
    pub ready: AtomicBool,
    /// Set when a plant must be rebuilt outside the poll path (e.g. failed
    /// order-plant reseat). Event pump treats this like a connection issue.
    pub force_reconnect: AtomicBool,
    /// Wakes the event pump when `force_reconnect` is set (a stored permit, so
    /// a wake raised while the pump is busy is not lost).
    pub reconnect_wake: tokio::sync::Notify,
    /// Parent secret for non-empty Handshake `auth_token` (from
    /// `RITHMIC_GATEWAY_AUTH_TOKEN`). Empty means only same-UID empty-token
    /// peers are accepted.
    pub expected_auth_token: String,
    /// `None` only in gate / RPC-sequence unit tests without plants.
    /// Plant RPCs return `Error(no_session)` — never Ack / empty success.
    pub session: Option<SessionHandle>,
    pub reconnect: Arc<ReconnectController>,
    /// Serialize first venue-subscribe per topic so a failing peer cannot
    /// leave concurrent Ack'd clients without a live venue join.
    pub topic_locks: TokioMutex<HashMap<SubKey, Arc<TokioMutex<()>>>>,
    /// Serialize the complete `show_orders` + bounded-drain reconciliation so
    /// concurrent `LoadOrders` requests cannot interleave their replays on the
    /// shared order-updates subscription channel (each drain would otherwise
    /// observe the other's working-order replay and return a mixed snapshot).
    /// Kept separate from the session lock so the drain can run without
    /// blocking the event pump.
    pub recon_lock: Arc<TokioMutex<()>>,
    /// Serializes history admission vs live MD intent creation (ticker /
    /// book / time bars). Held from history refuse-check through session-lock
    /// acquisition (and for the whole Load*), and across MD subscribe note +
    /// venue join, so history cannot race a concurrent subscribe (RC2.3).
    pub md_history_gate: Arc<TokioMutex<()>>,
    /// Ready peer count + optional idle-exit after last client.
    pub idle: IdleExit,
    /// Stable for the parent process lifetime (advertised in Ready).
    pub gateway_instance_id: u64,
    /// Bumps when plants / transport epoch is recreated (advertised in Ready).
    pub transport_generation: AtomicU64,
    /// Notifies connected clients when [`Self::transport_generation`] bumps so
    /// they drop and re-handshake (otherwise they keep a stale Ready frame).
    pub transport_epoch: watch::Sender<u64>,
}

impl GatewayState {
    /// Ask the event pump to rebuild the plants (outside the poll path).
    pub fn request_reconnect(&self) {
        self.force_reconnect.store(true, Ordering::SeqCst);
        self.reconnect_wake.notify_one();
    }
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
            && account_field_ok(&self.account_id, &hs.account_id)
            && account_field_ok(&self.fcm_id, &hs.fcm_id)
            && account_field_ok(&self.ib_id, &hs.ib_id)
    }
}

/// When the parent has a non-empty account field, the client must match it.
fn account_field_ok(parent: &str, client: &str) -> bool {
    parent.is_empty() || parent == client
}

/// Constant-time equality for auth tokens (length mismatch → false).
fn auth_token_eq(a: &str, b: &str) -> bool {
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    if ab.len() != bb.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in ab.iter().zip(bb.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Handshake gates shared by accept path and unit tests.
fn authorize_handshake(
    state: &GatewayState,
    hs: &Handshake,
    same_uid: bool,
) -> Result<(), (&'static str, &'static str)> {
    if !state.fingerprint.matches(hs) {
        return Err((
            "fingerprint_mismatch",
            "handshake does not match this gateway's credential set",
        ));
    }
    // v1 local policy (KTD16 / R15): empty auth_token is only trusted from a
    // same-UID unix peer. Non-empty tokens must match the parent secret.
    if hs.auth_token.is_empty() {
        if !same_uid {
            return Err((
                "auth_required",
                "empty auth_token is only accepted from a same-UID unix peer",
            ));
        }
    } else if state.expected_auth_token.is_empty()
        || !auth_token_eq(&hs.auth_token, &state.expected_auth_token)
    {
        return Err(("auth_failed", "auth_token rejected"));
    }
    if !state.ready.load(Ordering::SeqCst) {
        return Err(("not_ready", "gateway not Ready (plants not connected)"));
    }
    Ok(())
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

/// Everything `run_gateway` needs besides the already-connected session.
pub struct GatewayParams {
    pub listener: UnixListener,
    /// Socket path to remove on shutdown.
    pub path: std::path::PathBuf,
    pub fingerprint: Fingerprint,
    pub expected_auth_token: String,
    pub idle_policy: IdleExitPolicy,
    pub gates: ParentGates,
    pub session: SessionHandle,
}

/// Serve the gateway over an already-connected session until the listener
/// ends: build state, run the event pump, serve clients, then disconnect the
/// session and remove the socket. The production binary and the test-only
/// fake-plant binary both run exactly this.
pub async fn run_gateway(params: GatewayParams) -> Result<(), String> {
    let GatewayParams {
        listener,
        path,
        fingerprint,
        expected_auth_token,
        idle_policy,
        gates,
        session,
    } = params;
    let hub = Arc::new(FanoutHub::new(crate::subscriptions::DEFAULT_QUEUE_CAP));
    let reconnect = Arc::new(ReconnectController::new(hub.clone()));
    let (gateway_instance_id, transport_generation, transport_epoch) = boot_gateway_metadata();
    let state = Arc::new(GatewayState {
        gates,
        hub: hub.clone(),
        fingerprint,
        // Ready only after plants are up (KTD5): the caller connected already.
        ready: AtomicBool::new(true),
        force_reconnect: AtomicBool::new(false),
        reconnect_wake: tokio::sync::Notify::new(),
        expected_auth_token,
        session: Some(session.clone()),
        reconnect: reconnect.clone(),
        topic_locks: TokioMutex::new(HashMap::new()),
        recon_lock: Arc::new(TokioMutex::new(())),
        md_history_gate: Arc::new(TokioMutex::new(())),
        idle: IdleExit::new(idle_policy),
        gateway_instance_id,
        transport_generation,
        transport_epoch,
    });

    let pump = tokio::spawn(pump::run_event_pump(
        session.clone(),
        hub,
        reconnect,
        Arc::clone(&state),
    ));

    let serve_result = serve(listener, state).await.map_err(|e| e.to_string());
    pump.abort();
    let _ = pump.await;
    {
        let guard = session.lock_exclusive().await;
        let _ = guard.disconnect().await;
    }
    let _ = std::fs::remove_file(&path);
    serve_result
}

/// Accept connections until idle-exit fires (or forever when policy is Never).
pub async fn serve(listener: UnixListener, state: Arc<GatewayState>) -> std::io::Result<()> {
    loop {
        tokio::select! {
            biased;
            _ = state.idle.wait_until_should_exit() => {
                match state.idle.policy() {
                    IdleExitPolicy::After(grace) => {
                        eprintln!(
                            "rithmic-gateway: idle-exit after {}s with 0 peers",
                            grace.as_secs()
                        );
                    }
                    IdleExitPolicy::Never => {
                        eprintln!("rithmic-gateway: idle-exit");
                    }
                }
                return Ok(());
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, state).await {
                        eprintln!("gateway client error: {e}");
                    }
                });
            }
        }
    }
}

async fn read_frame_bytes<R: tokio::io::AsyncRead + Unpin>(
    stream: &mut R,
) -> std::io::Result<Option<Vec<u8>>> {
    // Idle wait for the next frame length has no short timeout (clients may
    // pause between RPCs). Once a length is known, bound the payload read so a
    // peer cannot allocate MAX_FRAME_LEN and stall forever.
    const PAYLOAD_READ_TIMEOUT: Duration = Duration::from_secs(5);
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
    match tokio::time::timeout(PAYLOAD_READ_TIMEOUT, stream.read_exact(&mut payload)).await {
        Ok(Ok(_)) => Ok(Some(payload)),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "payload read timeout",
        )),
    }
}

async fn write_frame(stream: &mut UnixStream, frame: &Frame) -> Result<(), String> {
    let wire = codec::encode(frame).map_err(|e| e.to_string())?;
    tokio::time::timeout(Duration::from_secs(5), stream.write_all(&wire))
        .await
        .map_err(|_| "write timeout".to_string())?
        .map_err(|e| format!("write: {e}"))
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

pub(super) enum OutMsg {
    Frame(Bytes),
}

/// Per-connection fan-out bookkeeping: which topics this client forwards,
/// typed venue joins this client asked for, and forwarder tasks.
pub(super) struct ClientCtx {
    subscribed: HashSet<SubKey>,
    ticker: HashSet<SubKey>,
    book: HashSet<SubKey>,
    time_bars: HashSet<TimeBarIntent>,
    pnl: bool,
    order: bool,
    brackets: bool,
    forwarders: HashMap<SubKey, JoinHandle<()>>,
    /// Per forwarded topic: which frame kinds this client asked for (bit mask of
    /// [`Kind::bit`]). A forwarder drops frames of other kinds.
    kinds: HashMap<SubKey, Arc<AtomicU8>>,
    out_tx: mpsc::Sender<OutMsg>,
}

impl ClientCtx {
    fn new(out_tx: mpsc::Sender<OutMsg>) -> Self {
        Self {
            subscribed: HashSet::new(),
            ticker: HashSet::new(),
            book: HashSet::new(),
            time_bars: HashSet::new(),
            pnl: false,
            order: false,
            brackets: false,
            forwarders: HashMap::new(),
            kinds: HashMap::new(),
            out_tx,
        }
    }

    /// Recompute and publish the frame kinds this client wants on `key`'s topic,
    /// after one of its ticker / book / time-bar intents changed.
    pub(super) fn sync_kinds(&mut self, key: &SubKey) {
        let mut mask = 0u8;
        if self.ticker.contains(key) {
            mask |= Kind::Ticker.bit();
        }
        if self.book.contains(key) {
            mask |= Kind::Book.bit();
        }
        if self
            .time_bars
            .iter()
            .any(|tb| tb.symbol == key.symbol && tb.exchange == key.exchange)
        {
            mask |= Kind::Bars.bit();
        }
        if mask == 0 && !self.forwarders.contains_key(key) {
            // Nothing forwarded and nothing wanted: keep no empty entry around.
            self.kinds.remove(key);
            return;
        }
        self.kinds
            .entry(key.clone())
            .or_insert_with(|| Arc::new(AtomicU8::new(0)))
            .store(mask, Ordering::SeqCst);
    }

    fn wants_md(&self, key: &SubKey) -> bool {
        self.ticker.contains(key)
            || self.book.contains(key)
            || self
                .time_bars
                .iter()
                .any(|tb| tb.symbol == key.symbol && tb.exchange == key.exchange)
    }

    /// Attach this client to `key`'s fan-out topic (idempotent). The topic
    /// must already exist in the hub (caller subscribes hub interest first).
    async fn ensure_forwarder(&mut self, hub: &FanoutHub, key: &SubKey) {
        if self.subscribed.contains(key) {
            return;
        }
        if let Some(rx) = hub.subscribe_receiver(key).await {
            let kinds = Arc::clone(
                self.kinds
                    .entry(key.clone())
                    .or_insert_with(|| Arc::new(AtomicU8::new(0))),
            );
            let handle = spawn_forwarder(rx, self.out_tx.clone(), stream_of_topic(key), kinds);
            self.forwarders.insert(key.clone(), handle);
            self.subscribed.insert(key.clone());
        }
    }

    fn drop_forwarder(&mut self, key: &SubKey) {
        if let Some(handle) = self.forwarders.remove(key) {
            handle.abort();
        }
        self.kinds.remove(key);
        self.subscribed.remove(key);
    }

    /// Forget one typed intent and leave the venue topic when this was the
    /// last peer. Shared by all MD teardown drains via [`TopicIntent`].
    async fn teardown_topic(state: &GatewayState, intent: dispatch::TopicIntent) {
        let key = intent.key();
        let lock = dispatch::topic_lock(state, &key).await;
        let _guard = lock.lock().await;
        if intent.forget(&state.reconnect).await {
            if let Some(session) = &state.session {
                let guard = session.lock().await;
                let _ = intent.venue_leave(&*guard).await;
            }
        }
    }

    /// Detach-only disconnect: clear typed intents + hub interest; venue
    /// unsubscribe only when the last peer of that typed join leaves.
    async fn teardown(&mut self, state: &GatewayState) {
        for handle in self.forwarders.values() {
            handle.abort();
        }
        self.forwarders.clear();

        // Fire-and-forget per topic: the client is dying, so unlike
        // `unsubscribe_topic` there is no re-note on failure. The topic_lock
        // is still held across forget + venue leave so a concurrent
        // first-peer subscribe cannot have its join undone.
        for key in self.ticker.drain() {
            Self::teardown_topic(state, TopicIntent::Ticker(key)).await;
        }
        for key in self.book.drain() {
            Self::teardown_topic(state, TopicIntent::Book(key)).await;
        }
        for intent in self.time_bars.drain() {
            Self::teardown_topic(state, TopicIntent::TimeBars(intent)).await;
        }
        self.teardown_pnl(state).await;
        self.teardown_order_plants(state).await;

        for key in self.subscribed.drain() {
            let _ = state.reconnect.remove_hub_interest(&key).await;
        }
    }

    async fn teardown_pnl(&mut self, state: &GatewayState) {
        if !self.pnl {
            return;
        }
        self.pnl = false;
        if state.reconnect.forget_pnl().await {
            if let Some(session) = &state.session {
                let guard = session.lock().await;
                let _ = guard.disconnect_pnl_plant().await;
            }
        }
    }

    /// Order-plant teardown: brackets and order share one plant, so the last
    /// peer of either drives the disconnect; when only brackets peers leave,
    /// re-seat order-only to drop venue brackets (no brackets unsubscribe API).
    async fn teardown_order_plants(&mut self, state: &GatewayState) {
        if !self.brackets && !self.order {
            return;
        }
        if self.brackets {
            let key = crate::convert::order_key();
            let lock = dispatch::topic_lock(state, &key).await;
            let _guard = lock.lock().await;
            let last_brackets = dispatch::clear_client_brackets(state, self).await;
            let last_order = if self.order {
                self.order = false;
                state.reconnect.forget_order().await
            } else {
                false
            };
            if last_order {
                if let Some(session) = &state.session {
                    let guard = session.lock().await;
                    let _ = guard.disconnect_order_plant().await;
                }
            } else if last_brackets && state.reconnect.restore_plan().await.order {
                if let Err(e) = dispatch::reseat_order_plant_without_brackets(state).await {
                    eprintln!("rithmic-gateway: teardown reseat failed: {e}");
                    state.ready.store(false, Ordering::SeqCst);
                    state.request_reconnect();
                }
            }
        } else {
            self.order = false;
            if state.reconnect.forget_order().await {
                if let Some(session) = &state.session {
                    let guard = session.lock().await;
                    let _ = guard.disconnect_order_plant().await;
                }
            }
        }
    }
}

/// Which plant stream a topic's events come from, for a gap frame.
fn stream_of_topic(key: &SubKey) -> rithmic_plants::Stream {
    match key.symbol.as_str() {
        "__pnl__" => rithmic_plants::Stream::Pnl,
        "__order__" => rithmic_plants::Stream::Order,
        _ => rithmic_plants::Stream::Ticker,
    }
}

/// Forward one topic to one client. A receiver that falls behind the topic's
/// ring is not disconnected: it is told exactly how many frames it missed (a
/// `Gap` in order with the rest) and keeps going. A client that stops reading
/// altogether is cut by the writer's timeout.
fn spawn_forwarder(
    rx: broadcast::Receiver<Fan>,
    out_tx: mpsc::Sender<OutMsg>,
    stream: rithmic_plants::Stream,
    kinds: Arc<AtomicU8>,
) -> JoinHandle<()> {
    let mut queue = crate::subscriptions::ClientQueue::from_receiver(ClientId::new(), rx);
    tokio::spawn(async move {
        loop {
            match queue.recv().await {
                Ok(fan) => {
                    // A frame of a kind this client never asked for (a book
                    // update for a ticker-only client) is dropped here, before
                    // it costs a socket write or a decode on the far side.
                    let wanted = fan.kind == Kind::Any
                        || kinds.load(Ordering::Relaxed) & fan.kind.bit() != 0;
                    if wanted && out_tx.send(OutMsg::Frame(fan.bytes)).await.is_err() {
                        return;
                    }
                }
                Err(crate::subscriptions::ClientQueueError::Overflow { skipped }) => {
                    let gap = Frame {
                        request_id: 0,
                        body: Some(Body::Event(pb::Event {
                            body: Some(pb::event::Body::Gap(pb::Gap {
                                stream: stream.plant().into(),
                                skipped,
                            })),
                        })),
                    };
                    let Ok(wire) = codec::encode(&gap) else {
                        return;
                    };
                    if out_tx.send(OutMsg::Frame(Bytes::from(wire))).await.is_err() {
                        return;
                    }
                }
                Err(crate::subscriptions::ClientQueueError::Closed) => return,
            }
        }
    })
}

/// Encode one event as a wire frame (a single allocation) ready to fan out.
fn encode_event(event: pb::Event, kind: Kind) -> Option<Fan> {
    let frame = Frame {
        request_id: 0,
        body: Some(Body::Event(event)),
    };
    let wire = codec::encode(&frame).ok()?;
    Some(Fan {
        kind,
        bytes: Bytes::from(wire),
    })
}

/// Encode a wire event and publish it to `key`'s topic (a gap on the ticker or
/// history stream goes to every symbol topic).
pub async fn publish_event(hub: &FanoutHub, key: &SubKey, event: pb::Event) -> usize {
    let Some(fan) = encode_event(event, Kind::Any) else {
        return 0;
    };
    if key.symbol == crate::convert::GAP_BROADCAST {
        return hub.publish_symbols(fan).await;
    }
    hub.publish(key, fan).await
}

async fn handle_client(mut stream: UnixStream, state: Arc<GatewayState>) -> Result<(), String> {
    let same_uid = same_uid_peer(&stream);

    let payload = read_frame_bytes(&mut stream)
        .await
        .map_err(|e| format!("read handshake: {e}"))?
        .ok_or_else(|| "client closed before handshake".to_string())?;
    // Use codec path (framed buffer) so production shares decode with tests.
    let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
    framed.extend_from_slice(&payload);
    let (req, _) = codec::decode::<Frame>(&framed).map_err(|e| format!("proto: {e}"))?;
    let hs = match req.body {
        Some(Body::Handshake(hs)) => hs,
        _ => return Err("first frame must be Handshake".into()),
    };
    if state.idle.is_shutting_down() {
        let _ = write_frame(
            &mut stream,
            &dispatch::error_frame(0, "shutting_down", "gateway idle-exit in progress"),
        )
        .await;
        return Err("shutting_down".into());
    }
    if let Err((code, msg)) = authorize_handshake(&state, &hs, same_uid) {
        let _ = write_frame(&mut stream, &dispatch::error_frame(0, code, msg)).await;
        return Err(format!("{code}: {msg}"));
    }

    write_frame(
        &mut stream,
        &Frame {
            request_id: 0,
            body: Some(Body::Ready(Ready {
                scopes: state.gates.scopes(),
                trading_enabled: state.gates.trading_enabled,
                cancel_all_enabled: state.gates.cancel_all_enabled,
                gateway_instance_id: state.gateway_instance_id,
                transport_generation: state.transport_generation.load(Ordering::Relaxed),
            })),
        },
    )
    .await?;

    if !state.idle.peer_attached().await {
        let _ = write_frame(
            &mut stream,
            &dispatch::error_frame(0, "shutting_down", "gateway idle-exit in progress"),
        )
        .await;
        return Err("shutting_down".into());
    }

    let (out_tx, out_rx) = mpsc::channel::<OutMsg>(crate::subscriptions::DEFAULT_QUEUE_CAP);
    let client = ClientCtx::new(out_tx);
    let mut epoch_rx = state.transport_epoch.subscribe();
    // Ignore the current value; only react to subsequent generation bumps.
    epoch_rx.borrow_and_update();

    // One connection is four tasks so no request can hold up another:
    //   reader   owns the read half, decodes frames and hands each to a lane;
    //   commands runs state-changing / subscription requests in arrival order;
    //   slow     runs read RPCs (history loads) a few at a time, off that lane;
    //   writer   owns the write half; replies go out ahead of fan-out events.
    let (reply_tx, reply_rx) = mpsc::channel::<Frame>(COMMAND_QUEUE + SLOW_QUEUE);
    let (cmd_tx, cmd_rx) = mpsc::channel::<Request>(COMMAND_QUEUE);
    let (slow_tx, slow_rx) = mpsc::channel::<Request>(SLOW_QUEUE);
    let (stop_tx, stop_rx) = watch::channel(false);
    let (read_half, write_half) = stream.into_split();

    let mut reader = tokio::spawn(read_requests(read_half, cmd_tx, slow_tx, reply_tx.clone()));
    let slow = tokio::spawn(run_slow_lane(Arc::clone(&state), slow_rx, reply_tx.clone()));
    let mut commands = tokio::spawn(run_commands(
        Arc::clone(&state),
        client,
        cmd_rx,
        reply_tx,
        stop_rx,
    ));
    let mut writer = tokio::spawn(write_replies(write_half, reply_rx, out_rx));

    let mut result: Result<(), String> = Ok(());
    let mut reader_done = false;
    let mut writer_done = false;
    let client = loop {
        tokio::select! {
            // The peer closed or the stream broke: let the command lane finish
            // what was already queued, then it ends on its own.
            r = &mut reader, if !reader_done => {
                reader_done = true;
                if let Ok(Err(e)) = r {
                    result = Err(e);
                }
            }
            // The command lane ended: a Disconnect, or the connection is done.
            done = &mut commands => break done.ok(),
            // The client stopped draining its socket (or it died).
            _ = &mut writer => {
                writer_done = true;
                let _ = stop_tx.send(true);
                break (&mut commands).await.ok();
            }
            // Plants rebuilt — force re-handshake so Ready carries the new
            // transport_generation (Macroscope).
            _ = epoch_rx.changed() => {
                let _ = stop_tx.send(true);
                break (&mut commands).await.ok();
            }
        }
    };

    reader.abort();
    slow.abort();
    if let Some(mut client) = client {
        client.teardown(&state).await;
    }
    // With every sender gone the writer flushes what is queued (a Disconnect
    // ack) and ends; bound the wait so a dead peer cannot hold the teardown.
    if !writer_done {
        let _ = tokio::time::timeout(Duration::from_secs(2), &mut writer).await;
        writer.abort();
    }
    state.idle.peer_detached().await;
    result
}

/// Requests a connection may have queued for each lane before the reader waits
/// (command) or answers `busy` (slow).
const COMMAND_QUEUE: usize = 256;
const SLOW_QUEUE: usize = 64;
/// Slow read RPCs one connection runs at once.
const SLOW_CONCURRENCY: usize = 4;
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

struct Request {
    id: u64,
    body: Body,
}

/// Read and decode frames on their own task: nothing selects over this read, so
/// a partly received frame is never abandoned, and a frame is decoded straight
/// from its payload with no intermediate copy.
async fn read_requests(
    mut read: tokio::net::unix::OwnedReadHalf,
    commands: mpsc::Sender<Request>,
    slow: mpsc::Sender<Request>,
    replies: mpsc::Sender<Frame>,
) -> Result<(), String> {
    use prost::Message;
    loop {
        match read_frame_bytes(&mut read).await {
            Ok(Some(payload)) => {
                let frame = Frame::decode(payload.as_slice()).map_err(|e| format!("proto: {e}"))?;
                let Some(body) = frame.body else { continue };
                let req = Request {
                    id: frame.request_id,
                    body,
                };
                match crate::rpc_table::lane_of(&req.body) {
                    crate::rpc_table::Lane::Command => {
                        if commands.send(req).await.is_err() {
                            return Ok(());
                        }
                    }
                    crate::rpc_table::Lane::Slow => match slow.try_send(req) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(req)) => {
                            let busy = dispatch::error_frame(
                                req.id,
                                "busy",
                                "too many slow requests in flight on this connection",
                            );
                            if replies.send(busy).await.is_err() {
                                return Ok(());
                            }
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                    },
                }
            }
            Ok(None) => return Ok(()),
            Err(e) => return Err(format!("read: {e}")),
        }
    }
}

/// Serve state-changing and subscription requests one at a time, in the order
/// they arrived. A dispatch in progress is never cancelled (a half-applied
/// subscription would leak intent); a stop is noticed between requests. Returns
/// the client context for teardown.
async fn run_commands(
    state: Arc<GatewayState>,
    mut client: ClientCtx,
    mut requests: mpsc::Receiver<Request>,
    replies: mpsc::Sender<Frame>,
    mut stop: watch::Receiver<bool>,
) -> ClientCtx {
    loop {
        let req = tokio::select! {
            biased;
            _ = stop.changed() => break,
            r = requests.recv() => match r {
                Some(r) => r,
                None => break,
            },
        };
        let close_after = matches!(req.body, Body::Disconnect(_));
        // During plant reconnect, refuse new RPCs (except detach) so callers do
        // not observe Ack against a half-rebuilt venue session.
        let resp = if !close_after && !state.ready.load(Ordering::SeqCst) {
            not_ready_frame(req.id)
        } else {
            dispatch::dispatch(&state, &mut client, req.id, req.body).await
        };
        if replies.send(resp).await.is_err() || close_after {
            break;
        }
    }
    client
}

fn not_ready_frame(request_id: u64) -> Frame {
    dispatch::error_frame(
        request_id,
        "not_ready",
        "gateway not Ready (plants reconnecting)",
    )
}

/// Serve slow read RPCs concurrently (bounded), replying as each finishes.
async fn run_slow_lane(
    state: Arc<GatewayState>,
    mut requests: mpsc::Receiver<Request>,
    replies: mpsc::Sender<Frame>,
) {
    let permits = Arc::new(tokio::sync::Semaphore::new(SLOW_CONCURRENCY));
    let mut running = tokio::task::JoinSet::new();
    while let Some(req) = requests.recv().await {
        let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
            return;
        };
        let (state, replies) = (Arc::clone(&state), replies.clone());
        running.spawn(async move {
            let _permit = permit;
            let resp = if !state.ready.load(Ordering::SeqCst) {
                not_ready_frame(req.id)
            } else {
                dispatch::dispatch_read(&state, req.id, req.body).await
            };
            let _ = replies.send(resp).await;
        });
        while running.try_join_next().is_some() {}
    }
    while running.join_next().await.is_some() {}
}

/// Own the write half. Replies are written ahead of fan-out events; a write that
/// takes longer than [`WRITE_TIMEOUT`] means the client stopped draining, and
/// the connection ends. The task ends once every reply sender is gone and the
/// queue is flushed.
async fn write_replies(
    mut write: tokio::net::unix::OwnedWriteHalf,
    mut replies: mpsc::Receiver<Frame>,
    mut events: mpsc::Receiver<OutMsg>,
) {
    let mut events_open = true;
    // Ends when the reply channel has closed and drained (that flushes a final
    // ack); fan-out events are best effort and never keep the socket open.
    let mut replies_open = true;
    while replies_open {
        let wire: Option<Bytes> = tokio::select! {
            biased;
            frame = replies.recv(), if replies_open => match frame {
                Some(frame) => match codec::encode(&frame) {
                    Ok(wire) => Some(Bytes::from(wire)),
                    Err(_) => None,
                },
                None => {
                    replies_open = false;
                    None
                }
            },
            msg = events.recv(), if events_open => match msg {
                Some(OutMsg::Frame(bytes)) => Some(bytes),
                None => {
                    events_open = false;
                    None
                }
            },
        };
        if let Some(wire) = wire {
            match tokio::time::timeout(WRITE_TIMEOUT, write.write_all(&wire)).await {
                Ok(Ok(())) => {}
                _ => return, // slow / dead client
            }
        }
    }
}

/// Rebroadcast a plant event to all interested fan-out subscribers, keyed by
/// symbol/exchange (or the internal pnl/order sentinel keys). Also feeds the
/// [`ReconnectController`]-visible hub so late subscribers still see the
/// venue-declared topic.
pub async fn publish_plant_event(hub: &FanoutHub, event: rithmic_plants::dto::PlantEvent) {
    use crate::convert::{plant_event_to_pb, route_of, Route};

    match route_of(&event) {
        // A symbol event: look the topic up from borrowed strings and, when nobody
        // is attached to it, drop the event before converting or encoding it.
        Route::Topic {
            symbol,
            exchange,
            kind,
        } => {
            let Some(tx) = hub.live_sender(symbol, exchange).await else {
                return;
            };
            if let Some(fan) = encode_event(plant_event_to_pb(event), kind) {
                let _ = tx.send(fan);
            }
        }
        Route::Gap(rithmic_plants::Stream::Ticker | rithmic_plants::Stream::History) => {
            if let Some(fan) = encode_event(plant_event_to_pb(event), Kind::Any) {
                hub.publish_symbols(fan).await;
            }
        }
        // Account-level events and order/PnL gaps ride their sentinel topics.
        Route::Pnl | Route::Order | Route::Other | Route::Gap(_) => {
            let (key, wire_event) = crate::convert::plant_event_to_routed(event);
            publish_event(hub, &key, wire_event).await;
        }
    }
}

#[cfg(test)]
mod auth_tests {
    use super::*;
    use crate::pb::Handshake;

    // Test fixture builder: maps a `Handshake`'s fields 1:1, so the arity is
    // inherent to the wire type. Allowed rather than boxing the helper.
    #[allow(clippy::too_many_arguments)]
    fn hs(
        user: &str,
        system: &str,
        url: &str,
        env: &str,
        account: &str,
        fcm: &str,
        ib: &str,
        token: &str,
    ) -> Handshake {
        Handshake {
            user: user.into(),
            system_name: system.into(),
            url: url.into(),
            env: env.into(),
            account_id: account.into(),
            fcm_id: fcm.into(),
            ib_id: ib.into(),
            auth_token: token.into(),
        }
    }

    #[test]
    fn fingerprint_matches_env_aliases() {
        let fp = Fingerprint {
            user: "alice".into(),
            system_name: "LucidTrading".into(),
            url: "wss://example".into(),
            env: "Live".into(),
            account_id: "A1".into(),
            fcm_id: "F1".into(),
            ib_id: "I1".into(),
        };
        assert!(fp.matches(&hs(
            "alice",
            "LucidTrading",
            "wss://example",
            "production",
            "A1",
            "F1",
            "I1",
            ""
        )));
        assert!(!fp.matches(&hs(
            "bob",
            "LucidTrading",
            "wss://example",
            "Live",
            "A1",
            "F1",
            "I1",
            ""
        )));
        assert!(!fp.matches(&hs(
            "alice",
            "LucidTrading",
            "wss://example",
            "Live",
            "OTHER",
            "F1",
            "I1",
            ""
        )));
    }

    #[test]
    fn auth_token_eq_constant_time_length_mismatch() {
        assert!(auth_token_eq("abc", "abc"));
        assert!(!auth_token_eq("abc", "abcd"));
        assert!(!auth_token_eq("abc", "abx"));
    }

    #[test]
    fn authorize_handshake_matrix() {
        let hub: SharedFanout = Arc::new(FanoutHub::new(8));
        let mut state = dispatch::test_state(ParentGates::default(), hub);
        state.expected_auth_token = "secret".into();
        state.ready.store(true, Ordering::SeqCst);

        let good = hs(
            "alice",
            "LucidTrading",
            "wss://example",
            "Live",
            "A1",
            "F1",
            "I1",
            "secret",
        );
        assert!(authorize_handshake(&state, &good, false).is_ok());

        let empty = hs(
            "alice",
            "LucidTrading",
            "wss://example",
            "Live",
            "A1",
            "F1",
            "I1",
            "",
        );
        assert_eq!(
            authorize_handshake(&state, &empty, false).unwrap_err().0,
            "auth_required"
        );
        assert!(authorize_handshake(&state, &empty, true).is_ok());

        let bad = hs(
            "alice",
            "LucidTrading",
            "wss://example",
            "Live",
            "A1",
            "F1",
            "I1",
            "nope",
        );
        assert_eq!(
            authorize_handshake(&state, &bad, true).unwrap_err().0,
            "auth_failed"
        );

        state.expected_auth_token.clear();
        assert_eq!(
            authorize_handshake(&state, &good, true).unwrap_err().0,
            "auth_failed"
        );

        state.ready.store(false, Ordering::SeqCst);
        assert_eq!(
            authorize_handshake(&state, &empty, true).unwrap_err().0,
            "not_ready"
        );
    }
}
