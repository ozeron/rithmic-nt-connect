//! The per-process shared gateway runtime.
//!
//! For one credential fingerprint a process holds **one** unix socket and **one**
//! reader task. The reader routes inbound frames to pending RPC replies (by
//! request id) and fans pushed events out to every attached facade's bounded
//! per-channel queues. A busy or slow channel never blocks another.
//!
//! Transport loss is recovered by a single-flight re-dial that bumps a
//! generation counter; state-changing RPCs in flight at the moment of loss are
//! never retried (their outcome is unknown).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::Duration;

use prost::Message;
use rithmic_host::dto::PlantEvent;
use rithmic_host::events::{SessionEvent, SESSION_EVENT_CAPACITY};
use rithmic_host::{Error, Result};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{broadcast, oneshot, Mutex as TokioMutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::convert::{plant_event_from_pb, Channel};
use super::transport::{Source, Stream};
use super::GatewayConfig;
use crate::framing::MAX_FRAME_LEN;
use crate::pb::{self, frame::Body, Frame};

/// Capacity of each facade's per-channel event queue.
pub const EVENT_QUEUE_CAP: usize = 16_384;
const HANDSHAKE_RETRY_PAUSE: Duration = Duration::from_millis(100);
const DISCONNECT_GRACE: Duration = Duration::from_secs(1);

/// Transport state changes, for adapters that re-arm intent on recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportEvent {
    /// The socket to the parent was lost (`generation` is the one that dropped).
    Down { generation: u64 },
    /// A *re*connect completed; `generation` is the new transport generation.
    /// The first connect of a process emits no `Up`.
    Up { generation: u64 },
}

/// Automatic recovery ran out of attempts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryFailed {
    pub generation: u64,
    pub attempts: u32,
    pub cause: String,
}

/// What the parent advertised in `Ready` (read-only: clients cannot elevate).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadyInfo {
    pub scopes: Vec<String>,
    pub trading_enabled: bool,
    pub cancel_all_enabled: bool,
    pub gateway_instance_id: u64,
}

/// Why an RPC failed, split by whether the request may have reached the parent.
pub(super) enum CallError {
    /// Failed before any byte was written: safe to retry on a fresh connection.
    NotSent(Error),
    /// The request was (or may have been) written: the outcome is unknown.
    Sent(Error),
}

fn transport(msg: impl Into<String>) -> Error {
    Error::Transport(msg.into())
}

// --- bounded per-channel event queues ----------------------------------------

#[derive(Default)]
struct QueueState {
    /// Events are shared by reference between every facade's queue: a decoded
    /// event is allocated once however many facades read it.
    items: VecDeque<Arc<PlantEvent>>,
    /// Events dropped (oldest first) since the consumer last looked.
    lagged: u64,
}

/// What a non-blocking look at a queue found.
pub(super) enum Take {
    Event(Arc<PlantEvent>),
    /// The consumer fell behind by this many events; it must resync.
    Lagged(u64),
    Empty,
}

pub(super) struct EventQueue {
    state: StdMutex<QueueState>,
    pub(super) notify: Notify,
    /// Set by the first subscribe or poll of this channel. An unarmed queue is
    /// never filled, so a facade that does not use a channel pays nothing for it.
    armed: AtomicBool,
}

impl EventQueue {
    fn new() -> Self {
        Self {
            state: StdMutex::new(QueueState::default()),
            notify: Notify::new(),
            armed: AtomicBool::new(false),
        }
    }

    pub(super) fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    fn is_armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }

    fn push(&self, event: Arc<PlantEvent>) {
        {
            let mut s = self.state.lock().expect("event queue");
            if s.items.len() >= EVENT_QUEUE_CAP {
                s.items.pop_front();
                s.lagged += 1;
            }
            s.items.push_back(event);
        }
        self.notify.notify_waiters();
    }

    /// Report a lag first (once), then drain remaining events in order.
    pub(super) fn take(&self) -> Take {
        let mut s = self.state.lock().expect("event queue");
        if s.lagged > 0 {
            let skipped = s.lagged;
            s.lagged = 0;
            return Take::Lagged(skipped);
        }
        match s.items.pop_front() {
            Some(event) => Take::Event(event),
            None => Take::Empty,
        }
    }
}

/// One facade's four channel queues.
pub(super) struct Queues {
    market: EventQueue,
    history: EventQueue,
    pnl: EventQueue,
    order: EventQueue,
}

impl Queues {
    pub(super) fn new() -> Self {
        Self {
            market: EventQueue::new(),
            history: EventQueue::new(),
            pnl: EventQueue::new(),
            order: EventQueue::new(),
        }
    }

    pub(super) fn get(&self, channel: Channel) -> &EventQueue {
        match channel {
            Channel::Market => &self.market,
            Channel::History => &self.history,
            Channel::Pnl => &self.pnl,
            Channel::Order => &self.order,
        }
    }

    pub(super) fn wake_all(&self) {
        for channel in Channel::ALL {
            self.get(channel).notify.notify_waiters();
        }
    }
}

// --- one connection ----------------------------------------------------------

pub(super) struct Connection {
    id: u64,
    writer: TokioMutex<WriteHalf<Stream>>,
    pending: StdMutex<HashMap<u64, oneshot::Sender<Frame>>>,
    next_id: AtomicU64,
    closed: AtomicBool,
    joined: AtomicBool,
    loss_reported: AtomicBool,
    runtime: Weak<Runtime>,
    /// Set when we close on purpose, so the reader does not report a loss.
    intentional: AtomicBool,
    pub(super) ready: ReadyInfo,
    reader: StdMutex<Option<JoinHandle<()>>>,
}

impl Connection {
    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Mark closed and fail every pending call (their senders drop).
    fn fail_all(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.pending.lock().expect("pending").clear();
    }

    /// A write failed or timed out part-way, so the stream may hold a truncated
    /// frame: no later frame may follow it. Fail every call, refuse new ones, and
    /// report loss immediately, then close the write half so the parent detaches.
    async fn break_connection(&self, writer: &mut WriteHalf<Stream>) {
        self.fail_all();
        self.report_loss();
        let _ = tokio::time::timeout(DISCONNECT_GRACE, writer.shutdown()).await;
    }

    fn report_loss(&self) {
        if !self.intentional.load(Ordering::SeqCst)
            && !self.loss_reported.swap(true, Ordering::SeqCst)
        {
            if let Some(runtime) = self.runtime.upgrade() {
                runtime.on_connection_lost(self.id);
            }
        }
    }

    /// Send one request and await its reply.
    pub(super) async fn call(
        &self,
        body: Body,
        timeout: Duration,
    ) -> std::result::Result<Frame, CallError> {
        if self.is_closed() {
            return Err(CallError::NotSent(transport("connection closed")));
        }
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let bytes = crate::codec::encode(&Frame {
            request_id: id,
            body: Some(body),
        })
        .map_err(|e| CallError::NotSent(Error::Protocol(e.to_string())))?;
        let (tx, rx) = oneshot::channel();
        self.pending.lock().expect("pending").insert(id, tx);

        {
            let mut writer = self.writer.lock().await;
            if self.is_closed() {
                self.pending.lock().expect("pending").remove(&id);
                return Err(CallError::NotSent(transport("connection closed")));
            }
            match tokio::time::timeout(timeout, writer.write_all(&bytes)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    self.pending.lock().expect("pending").remove(&id);
                    self.break_connection(&mut writer).await;
                    return Err(CallError::Sent(transport(format!("write failed: {e}"))));
                }
                Err(_) => {
                    self.pending.lock().expect("pending").remove(&id);
                    self.break_connection(&mut writer).await;
                    return Err(CallError::Sent(transport("write timed out")));
                }
            }
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(frame)) => Ok(frame),
            Ok(Err(_)) => Err(CallError::Sent(transport("connection lost before reply"))),
            Err(_) => {
                self.pending.lock().expect("pending").remove(&id);
                Err(CallError::Sent(transport(format!(
                    "timed out after {timeout:?} waiting for a reply"
                ))))
            }
        }
    }

    /// Detach cleanly: tell the parent, close the socket, stop the reader.
    async fn shutdown(&self) {
        self.intentional.store(true, Ordering::SeqCst);
        if !self.is_closed() {
            let id = self.next_id.fetch_add(1, Ordering::SeqCst);
            if let Ok(bytes) = crate::codec::encode(&Frame {
                request_id: id,
                body: Some(Body::Disconnect(pb::DisconnectRequest {})),
            }) {
                let mut writer = self.writer.lock().await;
                let _ = tokio::time::timeout(DISCONNECT_GRACE, writer.write_all(&bytes)).await;
                let _ = tokio::time::timeout(DISCONNECT_GRACE, writer.shutdown()).await;
            }
        }
        self.fail_all();
        self.abort_reader();
    }

    fn abort_reader(&self) {
        if let Some(handle) = self.reader.lock().expect("reader").take() {
            handle.abort();
        }
    }
}

async fn read_frame<R: AsyncRead + Unpin>(read: &mut R) -> std::io::Result<Frame> {
    let mut len = [0u8; 4];
    read.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len);
    if n > MAX_FRAME_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame of {n} bytes exceeds the {MAX_FRAME_LEN} byte limit"),
        ));
    }
    let mut buf = vec![0u8; n as usize];
    read.read_exact(&mut buf).await?;
    Frame::decode(buf.as_slice())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
}

async fn write_frame<W: AsyncWrite + Unpin>(
    stream: &mut W,
    frame: &Frame,
    timeout: Duration,
) -> std::io::Result<()> {
    let bytes = crate::codec::encode(frame)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    tokio::time::timeout(timeout, stream.write_all(&bytes))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "write timed out"))?
}

async fn reader_loop(conn: Arc<Connection>, mut read: ReadHalf<Stream>, runtime: Weak<Runtime>) {
    while let Ok(frame) = read_frame(&mut read).await {
        if frame.request_id != 0 {
            let waiter = conn
                .pending
                .lock()
                .expect("pending")
                .remove(&frame.request_id);
            if let Some(tx) = waiter {
                let _ = tx.send(frame);
            }
            continue;
        }
        if let Some(Body::Event(event)) = frame.body {
            if let Some(rt) = runtime.upgrade() {
                match &event.body {
                    Some(pb::event::Body::TransportInterrupted(_)) => {
                        rt.on_parent_interrupted();
                        continue;
                    }
                    Some(pb::event::Body::TransportResumed(_)) => {
                        rt.on_parent_resumed();
                        continue;
                    }
                    _ => {}
                }
            }
            if let (Some(rt), Some(event)) = (runtime.upgrade(), plant_event_from_pb(event)) {
                rt.deliver(event);
            }
        }
    }
    conn.fail_all();
    conn.report_loss();
}

// --- handshake ----------------------------------------------------------------

enum HandshakeError {
    /// Parent not ready / silent: re-dial until the attach deadline.
    Retry(Error),
    Fatal(Error),
}

async fn handshake(
    stream: &mut Stream,
    cfg: &GatewayConfig,
) -> std::result::Result<ReadyInfo, HandshakeError> {
    // Identity only: fingerprint + auth token. The Rithmic password is never
    // sent and clients do not have one.
    let hello = Frame {
        request_id: 0,
        body: Some(Body::Handshake(pb::Handshake {
            user: cfg.user.clone(),
            system_name: cfg.system_name.clone(),
            url: cfg.url.clone(),
            env: cfg.env.clone(),
            account_id: cfg.account_id.clone(),
            fcm_id: cfg.fcm_id.clone(),
            ib_id: cfg.ib_id.clone(),
            auth_token: cfg.auth_token().to_string(),
        })),
    };
    write_frame(stream, &hello, cfg.rpc_timeout)
        .await
        .map_err(|e| HandshakeError::Retry(transport(format!("handshake write failed: {e}"))))?;

    let reply = read_frame(stream);
    let frame = match tokio::time::timeout(cfg.rpc_timeout, reply).await {
        Err(_) => return Err(HandshakeError::Retry(transport("handshake timed out"))),
        Ok(Err(e)) => {
            return Err(HandshakeError::Fatal(transport(format!(
                "gateway closed the connection during handshake: {e}"
            ))))
        }
        Ok(Ok(frame)) => frame,
    };
    match frame.body {
        Some(Body::Ready(ready)) => Ok(ReadyInfo {
            scopes: ready.scopes,
            trading_enabled: ready.trading_enabled,
            cancel_all_enabled: ready.cancel_all_enabled,
            gateway_instance_id: ready.gateway_instance_id,
        }),
        Some(Body::Error(e)) if e.code == "not_ready" => {
            Err(HandshakeError::Retry(Error::Gateway {
                code: e.code,
                message: e.message,
            }))
        }
        Some(Body::Error(e)) => Err(HandshakeError::Fatal(Error::Gateway {
            code: e.code,
            message: e.message,
        })),
        other => Err(HandshakeError::Fatal(Error::Protocol(format!(
            "expected Ready, got {other:?}"
        )))),
    }
}

// --- intents shared by the facades on one socket --------------------------------------

/// A subscription intent. The parent counts intents per *socket*, and every
/// facade in the process shares that socket, so the facades' holdings are
/// counted here: only the last holder's release reaches the parent.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) enum IntentKey {
    Ticker(String, String),
    Book(String, String),
    TimeBars(String, String, i32, i32),
    Pnl,
    Order,
    Brackets,
}

/// What a facade's release of an intent asks of the parent.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Release {
    /// Nobody in the process holds it any more: release it on the parent.
    Send,
    /// Another facade still holds it: the parent must keep it.
    Keep,
}

/// Which facades hold which intents. A facade either holds an intent or not
/// (the parent treats a repeat subscribe as a no-op, and the adapter replays
/// its subscriptions after a gap, so this is a set, not a count).
#[derive(Default)]
pub(super) struct Intents {
    held: HashMap<IntentKey, HashSet<u64>>,
}

impl Intents {
    pub(super) fn hold(&mut self, facade: u64, key: IntentKey) {
        self.held.entry(key).or_default().insert(facade);
    }

    /// Drop `facade`'s hold. The parent is told when nobody holds the intent
    /// afterwards, including when nobody held it at all (a release the parent
    /// answers as a no-op, as before facades were counted).
    pub(super) fn release(&mut self, facade: u64, key: &IntentKey) -> Release {
        match self.held.get_mut(key) {
            Some(holders) => {
                holders.remove(&facade);
                if holders.is_empty() {
                    self.held.remove(key);
                    Release::Send
                } else {
                    Release::Keep
                }
            }
            None => Release::Send,
        }
    }

    pub(super) fn is_held(&self, key: &IntentKey) -> bool {
        self.held.contains_key(key)
    }

    pub(super) fn holds(&self, facade: u64, key: &IntentKey) -> bool {
        self.held.get(key).is_some_and(|h| h.contains(&facade))
    }

    /// Every intent `facade` holds.
    pub(super) fn held_by(&self, facade: u64) -> Vec<IntentKey> {
        self.held
            .iter()
            .filter(|(_, holders)| holders.contains(&facade))
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Remove every hold of a facade that is leaving, and return the intents
    /// nobody holds any more (the releases the parent must be told). Order and
    /// brackets are one release (`DisconnectOrder`), returned as
    /// [`IntentKey::Order`] once neither is held.
    pub(super) fn drop_facade(&mut self, facade: u64) -> Vec<IntentKey> {
        let mut freed = Vec::new();
        let mut order_group = false;
        for key in self.held_by(facade) {
            let last = self.release(facade, &key) == Release::Send;
            match key {
                IntentKey::Order | IntentKey::Brackets => order_group = true,
                key if last => freed.push(key),
                _ => {}
            }
        }
        if order_group && !self.order_group_held() {
            freed.push(IntentKey::Order);
        }
        freed
    }

    /// Whether any facade holds order or bracket updates.
    pub(super) fn order_group_held(&self) -> bool {
        self.is_held(&IntentKey::Order) || self.is_held(&IntentKey::Brackets)
    }
}

impl IntentKey {
    /// The key whose lock orders this intent's requests. Order and brackets
    /// share one: the parent releases them together.
    fn lock_key(&self) -> IntentKey {
        match self {
            IntentKey::Brackets => IntentKey::Order,
            other => other.clone(),
        }
    }

    /// Stable recovery order: ticker, book, bars, PnL, orders, brackets;
    /// keys within a family sort by their venue parameters.
    fn subscribe_body(&self) -> Body {
        match self {
            Self::Ticker(symbol, exchange) => Body::Subscribe(pb::SubscribeRequest {
                symbol: symbol.clone(),
                exchange: exchange.clone(),
            }),
            Self::Book(symbol, exchange) => Body::SubscribeBook(pb::SubscribeBookRequest {
                symbol: symbol.clone(),
                exchange: exchange.clone(),
            }),
            Self::TimeBars(symbol, exchange, bar_type, period) => {
                Body::SubscribeTimeBars(pb::SubscribeTimeBarsRequest {
                    symbol: symbol.clone(),
                    exchange: exchange.clone(),
                    bar_type: *bar_type,
                    period: *period,
                })
            }
            Self::Pnl => Body::SubscribePnl(pb::SubscribePnlRequest {}),
            Self::Order => Body::SubscribeOrderUpdates(pb::SubscribeOrderUpdatesRequest {}),
            Self::Brackets => Body::SubscribeBracketUpdates(pb::SubscribeBracketUpdatesRequest {}),
        }
    }

    /// The request that releases this intent on the parent.
    pub(super) fn release_body(&self) -> Body {
        match self {
            IntentKey::Ticker(symbol, exchange) => Body::Unsubscribe(pb::UnsubscribeRequest {
                symbol: symbol.clone(),
                exchange: exchange.clone(),
            }),
            IntentKey::Book(symbol, exchange) => {
                Body::UnsubscribeBook(pb::UnsubscribeBookRequest {
                    symbol: symbol.clone(),
                    exchange: exchange.clone(),
                })
            }
            IntentKey::TimeBars(symbol, exchange, bar_type, period) => {
                Body::UnsubscribeTimeBars(pb::UnsubscribeTimeBarsRequest {
                    symbol: symbol.clone(),
                    exchange: exchange.clone(),
                    bar_type: *bar_type,
                    period: *period,
                })
            }
            IntentKey::Pnl => Body::DisconnectPnl(pb::DisconnectPnlRequest {}),
            IntentKey::Order | IntentKey::Brackets => {
                Body::DisconnectOrder(pb::DisconnectOrderRequest {})
            }
        }
    }
}

// --- the runtime ---------------------------------------------------------------

pub(super) struct Runtime {
    pub(super) cfg: GatewayConfig,
    source: Source,
    conn: StdMutex<Option<Arc<Connection>>>,
    /// Serializes (re)dials: exactly one re-dial for all waiters.
    redial: TokioMutex<()>,
    generation: AtomicU64,
    next_conn_id: AtomicU64,
    ready: StdMutex<ReadyInfo>,
    facades: StdMutex<HashMap<u64, Arc<Queues>>>,
    next_facade_id: AtomicU64,
    /// The facades' intent holdings. Only ever locked for an instant.
    intents: StdMutex<Intents>,
    /// One async lock per intent, held across the request a hold or release
    /// decides on, so a release and a subscribe of the same intent from two
    /// facades reach the parent in the order they were decided. A slow request
    /// for one intent never holds up another.
    intent_locks: StdMutex<HashMap<IntentKey, Arc<TokioMutex<()>>>>,
    transport_tx: broadcast::Sender<TransportEvent>,
    failed_tx: broadcast::Sender<RecoveryFailed>,
    session_tx: broadcast::Sender<SessionEvent>,
    /// Terminal until an explicit connect; RPCs must not start new flights.
    failed: StdMutex<Option<String>>,
    changed: Notify,
    /// Set while the last facade is detaching: no recovery after a deliberate close.
    stopping: AtomicBool,
    /// A recovery flight is running (at most one).
    recovering: AtomicBool,
    self_weak: Weak<Runtime>,
}

fn registry() -> &'static StdMutex<HashMap<String, Weak<Runtime>>> {
    static REGISTRY: OnceLock<StdMutex<HashMap<String, Weak<Runtime>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| StdMutex::new(HashMap::new()))
}

impl Runtime {
    /// Get or create the runtime for this credential fingerprint (one socket
    /// per process). The first creator's timeouts win.
    pub(super) fn for_config(cfg: &GatewayConfig) -> Arc<Self> {
        // The account is part of the handshake, so two accounts never share a
        // socket (the first creator's handshake would win for both).
        let key = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}",
            cfg.user,
            cfg.system_name,
            cfg.url,
            cfg.env,
            cfg.account_id,
            cfg.fcm_id,
            cfg.ib_id,
            cfg.socket_path().display()
        );
        let mut map = registry().lock().expect("registry");
        if let Some(existing) = map.get(&key).and_then(Weak::upgrade) {
            return existing;
        }
        let runtime = Self::with_source(cfg.clone(), Source::Unix);
        map.retain(|_, w| w.strong_count() > 0);
        map.insert(key, Arc::downgrade(&runtime));
        runtime
    }

    pub(super) fn with_source(cfg: GatewayConfig, source: Source) -> Arc<Self> {
        Arc::new_cyclic(|weak| Runtime {
            cfg,
            source,
            conn: StdMutex::new(None),
            redial: TokioMutex::new(()),
            generation: AtomicU64::new(0),
            next_conn_id: AtomicU64::new(1),
            ready: StdMutex::new(ReadyInfo::default()),
            facades: StdMutex::new(HashMap::new()),
            next_facade_id: AtomicU64::new(1),
            intents: StdMutex::new(Intents::default()),
            intent_locks: StdMutex::new(HashMap::new()),
            transport_tx: broadcast::channel(16).0,
            failed_tx: broadcast::channel(4).0,
            session_tx: broadcast::channel(SESSION_EVENT_CAPACITY).0,
            failed: StdMutex::new(None),
            changed: Notify::new(),
            stopping: AtomicBool::new(false),
            recovering: AtomicBool::new(false),
            self_weak: weak.clone(),
        })
    }

    pub(super) fn register(&self, queues: Arc<Queues>) -> u64 {
        let id = self.next_facade_id.fetch_add(1, Ordering::SeqCst);
        self.facades.lock().expect("facades").insert(id, queues);
        id
    }

    /// A late Ack cannot resurrect a facade which detached during the RPC.
    /// Pair this check with unregister-before-drop-holdings in `leave`.
    pub(super) fn hold_live_intent(&self, facade: u64, key: IntentKey) -> bool {
        let facades = self.facades.lock().expect("facades");
        if !facades.contains_key(&facade) {
            return false;
        }
        self.intents().hold(facade, key);
        true
    }

    /// The intent holdings (locked for an instant; never across an await).
    pub(super) fn intents(&self) -> std::sync::MutexGuard<'_, Intents> {
        self.intents.lock().expect("intents")
    }

    /// Take the ordering lock for one intent (see `intent_locks`).
    pub(super) async fn intent_lock(&self, key: &IntentKey) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = self
            .intent_locks
            .lock()
            .expect("intent locks")
            .entry(key.lock_key())
            .or_default()
            .clone();
        lock.lock_owned().await
    }

    /// Tell the parent about intents a leaving facade was the last to hold.
    /// Best effort: each is re-checked under its lock (another facade may have
    /// taken it since) and a failure is dropped, since the parent releases
    /// every intent on the socket when the last facade closes it.
    pub(super) async fn send_releases(&self, freed: Vec<IntentKey>) {
        for key in freed {
            let Ok((_barrier, conn)) = self.intent_connection().await else {
                return;
            };
            let _ordered = self.intent_lock(&key).await;
            let still_free = {
                let intents = self.intents();
                match key {
                    IntentKey::Order | IntentKey::Brackets => !intents.order_group_held(),
                    ref key => !intents.is_held(key),
                }
            };
            if !still_free {
                continue;
            }
            let _ = conn.call(key.release_body(), self.cfg.rpc_timeout).await;
        }
    }

    /// Returns true when no facade remains attached.
    pub(super) fn unregister(&self, id: u64) -> bool {
        let mut facades = self.facades.lock().expect("facades");
        facades.remove(&id);
        facades.is_empty()
    }

    pub(super) fn deliver(&self, event: PlantEvent) {
        let channel = Channel::of(&event);
        let facades = self.facades.lock().expect("facades");
        // The event is wrapped once, and only if some facade reads this channel;
        // every armed queue then holds a reference, never a copy.
        let mut event = Some(event);
        let mut shared: Option<Arc<PlantEvent>> = None;
        for queues in facades.values() {
            let queue = queues.get(channel);
            if !queue.is_armed() {
                continue;
            }
            let arc = shared
                .get_or_insert_with(|| Arc::new(event.take().expect("event delivered once")))
                .clone();
            queue.push(arc);
        }
    }

    fn wake_all(&self) {
        for queues in self.facades.lock().expect("facades").values() {
            queues.wake_all();
        }
        self.changed.notify_waiters();
    }

    pub(super) fn live(&self) -> Option<Arc<Connection>> {
        self.conn
            .lock()
            .expect("conn")
            .as_ref()
            .filter(|c| !c.is_closed() && c.joined.load(Ordering::SeqCst))
            .cloned()
    }

    pub(super) fn is_down(&self) -> bool {
        self.live().is_none()
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    pub(super) fn ready(&self) -> ReadyInfo {
        self.ready.lock().expect("ready").clone()
    }

    pub(super) fn subscribe_transport(&self) -> broadcast::Receiver<TransportEvent> {
        self.transport_tx.subscribe()
    }

    pub(super) fn subscribe_recovery_failed(&self) -> broadcast::Receiver<RecoveryFailed> {
        self.failed_tx.subscribe()
    }

    pub(super) fn session_events(&self) -> broadcast::Receiver<SessionEvent> {
        self.session_tx.subscribe()
    }

    pub(super) fn stream_closed(&self) -> bool {
        let failed = self.failed.lock().expect("failed").is_some();
        self.stopping.load(Ordering::SeqCst)
            || failed
            // A reader can mark the connection closed just before it starts
            // recovery. That detection window must not become a stream error.
            || (self.cfg.recovery_attempts == 0 && self.is_down())
    }

    pub(super) fn allow_recovery(&self) {
        self.stopping.store(false, Ordering::SeqCst);
        *self.failed.lock().expect("failed") = None;
    }

    fn interrupted(&self, cause: &str) {
        let generation = self.generation();
        let _ = self.session_tx.send(SessionEvent::Interrupted {
            generation,
            cause: Some(cause.into()),
        });
        self.wake_all();
    }

    fn on_parent_interrupted(&self) {
        self.interrupted("parent plant interrupted");
    }

    fn resumed(&self, socket: bool) {
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.session_tx.send(SessionEvent::Resumed { generation });
        if socket {
            let _ = self.transport_tx.send(TransportEvent::Up { generation });
        }
        self.wake_all();
    }

    fn on_parent_resumed(&self) {
        // A socket re-join has its own completion barrier. A parent's restore
        // while that join runs cannot announce that our intents are live yet.
        if self.recovering.load(Ordering::SeqCst) {
            return;
        }
        self.resumed(false);
    }

    fn on_connection_lost(&self, conn_id: u64) {
        let is_current = self
            .conn
            .lock()
            .expect("conn")
            .as_ref()
            .is_some_and(|c| c.id == conn_id);
        if is_current && !self.stopping.load(Ordering::SeqCst) {
            // Mark recovery before waking stream consumers or RPC waiters.
            let already_recovering = self.recovering.swap(true, Ordering::SeqCst);
            if !already_recovering {
                self.interrupted("socket lost");
                let _ = self.transport_tx.send(TransportEvent::Down {
                    generation: self.generation(),
                });
                self.start_recovery();
            }
        }
    }

    /// The only automatic flight. RPCs wait for it rather than spending their
    /// own dial budgets. One deadline bounds dialing, backoff and all joins.
    fn start_recovery(&self) {
        if self.cfg.recovery_attempts == 0 {
            self.recovering.store(false, Ordering::SeqCst);
            self.wake_all();
            return;
        }
        let Some(runtime) = self.self_weak.upgrade() else {
            return;
        };
        tokio::spawn(async move {
            let _redial = runtime.redial.lock().await;
            let cfg = &runtime.cfg;
            let mut backoff = cfg.recovery_initial_backoff;
            let mut attempts = 0;
            let mut cause = String::new();
            let deadline = Instant::now() + cfg.attach_timeout;
            while attempts < cfg.recovery_attempts && Instant::now() < deadline {
                if runtime.stopping.load(Ordering::SeqCst) {
                    break;
                }
                attempts += 1;
                match runtime.attach_and_rejoin(Some(deadline)).await {
                    Ok(conn) => {
                        if runtime.stopping.load(Ordering::SeqCst) {
                            conn.shutdown().await;
                            break;
                        }
                        match runtime.finish_attach(&conn) {
                            Ok(()) => return,
                            Err(err) => {
                                cause = err.to_string();
                                conn.shutdown().await;
                            }
                        }
                    }
                    Err(err) => cause = err.to_string(),
                }
                if attempts < cfg.recovery_attempts {
                    tokio::time::sleep(
                        backoff.min(deadline.saturating_duration_since(Instant::now())),
                    )
                    .await;
                    backoff = (backoff * 2).min(cfg.recovery_max_backoff);
                }
            }
            if !runtime.stopping.load(Ordering::SeqCst) {
                *runtime.failed.lock().expect("failed") = Some(cause.clone());
                let generation = runtime.generation();
                let _ = runtime.session_tx.send(SessionEvent::RecoveryFailed {
                    generation,
                    attempts,
                    cause: Some(cause.clone()),
                });
                let _ = runtime.failed_tx.send(RecoveryFailed {
                    generation,
                    attempts,
                    cause,
                });
            }
            runtime.recovering.store(false, Ordering::SeqCst);
            runtime.wake_all();
        });
    }

    fn terminal_error(&self) -> Option<Error> {
        if self.stopping.load(Ordering::SeqCst) {
            Some(transport("session disconnected"))
        } else {
            self.failed
                .lock()
                .expect("failed")
                .as_ref()
                .map(|cause| transport(format!("recovery failed: {cause}")))
        }
    }

    /// A usable connection, only published after all recorded intents re-join.
    pub(super) async fn ensure_live(&self) -> Result<Arc<Connection>> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(err) = self.terminal_error() {
                return Err(err);
            }
            if let Some(conn) = self.live() {
                return Ok(conn);
            }
            let old = self.conn.lock().expect("conn").clone();
            if let Some(conn) = old.filter(|c| c.is_closed()) {
                conn.report_loss();
            }
            if self.recovering.load(Ordering::SeqCst) {
                changed.await;
                continue;
            }
            let _redial = self.redial.lock().await;
            if let Some(err) = self.terminal_error() {
                return Err(err);
            }
            if let Some(conn) = self.live() {
                return Ok(conn);
            }
            if self.recovering.load(Ordering::SeqCst) {
                continue;
            }
            let conn = self.attach_and_rejoin(None).await?;
            self.finish_attach(&conn)?;
            return Ok(conn);
        }
    }

    fn finish_attach(&self, conn: &Connection) -> Result<()> {
        // Pair publication with loss detection: a drop at this boundary either
        // fails this attempt or starts the next flight after Resumed, never
        // disappears behind the current flight's recovering flag.
        let _current = self.conn.lock().expect("conn");
        if let Some(err) = self.terminal_error() {
            return Err(err);
        }
        if conn.is_closed() {
            return Err(transport("socket lost during intent re-join"));
        }
        conn.joined.store(true, Ordering::SeqCst);
        self.recovering.store(false, Ordering::SeqCst);
        if self.generation() == 0 {
            self.generation.store(1, Ordering::SeqCst);
        } else {
            self.resumed(true);
        }
        self.wake_all();
        Ok(())
    }

    /// Intent mutations use the same barrier as re-join. Record an Ack before
    /// recovery can snapshot holdings. Recovery sends directly on the candidate
    /// connection and never waits for an intent operation that is awaiting it.
    pub(super) async fn intent_connection(
        &self,
    ) -> Result<(tokio::sync::MutexGuard<'_, ()>, Arc<Connection>)> {
        loop {
            self.ensure_live().await?;
            let guard = self.redial.lock().await;
            if let Some(err) = self.terminal_error() {
                return Err(err);
            }
            if let Some(conn) = self.live() {
                return Ok((guard, conn));
            }
        }
    }

    async fn attach_and_rejoin(&self, deadline: Option<Instant>) -> Result<Arc<Connection>> {
        let attempt = async {
            let conn = self.dial().await?;
            *self.ready.lock().expect("ready") = conn.ready.clone();
            *self.conn.lock().expect("conn") = Some(conn.clone());
            let mut keys: Vec<_> = self.intents().held.keys().cloned().collect();
            keys.sort();
            for key in keys {
                // Drop/disconnect can remove a facade synchronously mid-join.
                if !self.intents().is_held(&key) {
                    continue;
                }
                let frame = conn
                    .call(key.subscribe_body(), self.cfg.rpc_timeout)
                    .await
                    .map_err(|e| match e {
                        CallError::NotSent(e) | CallError::Sent(e) => e,
                    })?;
                match super::map_reply(frame)?.body {
                    Some(Body::Ack(_)) => {}
                    other => return Err(super::unexpected("Ack", other)),
                }
            }
            if conn.is_closed() {
                return Err(transport("socket lost during intent re-join"));
            }
            Ok(conn)
        };
        // Initial attach retains its specific Unix attachment errors and the
        // handshake budget after waiting for a parent to start. Recovery has a
        // tighter end-to-end deadline, including every intent Ack.
        let timeout = deadline.map_or_else(
            || self.cfg.attach_timeout + self.cfg.rpc_timeout + HANDSHAKE_RETRY_PAUSE,
            |deadline| deadline.saturating_duration_since(Instant::now()),
        );
        let result = tokio::time::timeout(timeout, attempt)
            .await
            .unwrap_or_else(|_| Err(transport("attach/intent re-join timed out")));
        if result.is_err() {
            let conn = self.conn.lock().expect("conn").take();
            if let Some(conn) = conn {
                conn.shutdown().await;
            }
        }
        result
    }

    /// Attach, attest the parent holds the flock, and handshake.
    async fn dial(&self) -> Result<Arc<Connection>> {
        let cfg = &self.cfg;
        let deadline = Instant::now() + cfg.attach_timeout;
        let mut stream = self.source.connect(cfg).await?;
        loop {
            match handshake(&mut stream, cfg).await {
                Ok(ready) => return Ok(self.start_connection(stream, ready)),
                Err(HandshakeError::Fatal(e)) => return Err(e),
                Err(HandshakeError::Retry(e)) if Instant::now() >= deadline => return Err(e),
                Err(HandshakeError::Retry(_)) => {
                    drop(stream);
                    tokio::time::sleep(HANDSHAKE_RETRY_PAUSE).await;
                    stream = self.source.connect(cfg).await?;
                }
            }
        }
    }

    fn start_connection(&self, stream: Stream, ready: ReadyInfo) -> Arc<Connection> {
        let (read, write) = tokio::io::split(stream);
        let conn = Arc::new(Connection {
            id: self.next_conn_id.fetch_add(1, Ordering::SeqCst),
            writer: TokioMutex::new(write),
            pending: StdMutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            joined: AtomicBool::new(false),
            loss_reported: AtomicBool::new(false),
            runtime: self.self_weak.clone(),
            intentional: AtomicBool::new(false),
            ready,
            reader: StdMutex::new(None),
        });
        let handle = tokio::spawn(reader_loop(Arc::clone(&conn), read, self.self_weak.clone()));
        *conn.reader.lock().expect("reader") = Some(handle);
        conn
    }

    /// Close the connection on purpose (the parent keeps running).
    pub(super) async fn close(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        self.wake_all();
        let _redial = self.redial.lock().await;
        let conn = self.conn.lock().expect("conn").take();
        if let Some(conn) = conn {
            conn.shutdown().await;
        }
        self.wake_all();
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.lock().expect("conn").take() {
            conn.intentional.store(true, Ordering::SeqCst);
            conn.fail_all();
            conn.abort_reader();
        }
    }
}

#[cfg(test)]
mod intent_tests {
    use super::*;

    fn nq() -> IntentKey {
        IntentKey::Ticker("NQH6".into(), "CME".into())
    }

    #[test]
    fn the_last_holder_releases_on_the_parent() {
        let mut intents = Intents::default();
        intents.hold(1, nq());
        intents.hold(2, nq());
        assert_eq!(intents.release(1, &nq()), Release::Keep);
        assert_eq!(intents.release(2, &nq()), Release::Send);
        assert!(!intents.is_held(&nq()));
    }

    #[test]
    fn a_repeat_subscribe_is_one_hold() {
        let mut intents = Intents::default();
        intents.hold(1, nq());
        intents.hold(1, nq());
        assert_eq!(intents.release(1, &nq()), Release::Send);
    }

    #[test]
    fn releasing_what_nobody_holds_still_reaches_the_parent() {
        assert_eq!(Intents::default().release(1, &nq()), Release::Send);
    }

    #[test]
    fn a_release_from_a_facade_that_never_held_it_keeps_the_others() {
        let mut intents = Intents::default();
        intents.hold(2, nq());
        assert_eq!(intents.release(1, &nq()), Release::Keep);
        assert!(intents.holds(2, &nq()));
    }

    #[test]
    fn held_by_lists_one_facades_intents() {
        let mut intents = Intents::default();
        intents.hold(1, nq());
        intents.hold(1, IntentKey::Order);
        intents.hold(2, IntentKey::Pnl);
        let mut mine = intents.held_by(1);
        mine.sort_by_key(|k| format!("{k:?}"));
        assert_eq!(mine, vec![IntentKey::Order, nq()]);
    }

    #[test]
    fn a_leaving_facade_frees_only_what_nobody_else_holds() {
        let mut intents = Intents::default();
        intents.hold(1, nq());
        intents.hold(1, IntentKey::Pnl);
        intents.hold(2, IntentKey::Pnl);
        intents.hold(1, IntentKey::Brackets);
        let mut freed = intents.drop_facade(1);
        freed.sort_by_key(|k| format!("{k:?}"));
        assert_eq!(freed, vec![IntentKey::Order, nq()]);
        assert!(intents.holds(2, &IntentKey::Pnl));
        assert!(intents.held_by(1).is_empty());
    }

    #[test]
    fn the_order_group_is_freed_only_when_neither_part_is_held() {
        let mut intents = Intents::default();
        intents.hold(1, IntentKey::Order);
        intents.hold(2, IntentKey::Brackets);
        assert!(intents.drop_facade(1).is_empty());
        assert_eq!(intents.drop_facade(2), vec![IntentKey::Order]);
    }

    #[tokio::test]
    async fn intent_locks_order_one_intent_and_never_block_another() {
        let cfg = GatewayConfig::new("u-intent-locks", "S", "wss://x", "Live");
        let runtime = Runtime::for_config(&cfg);
        let _nq = runtime.intent_lock(&nq()).await;
        let other = IntentKey::Ticker("ESZ6".into(), "CME".into());
        tokio::time::timeout(Duration::from_millis(200), runtime.intent_lock(&other))
            .await
            .expect("another intent is not held up");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), runtime.intent_lock(&nq()))
                .await
                .is_err(),
            "the same intent waits its turn"
        );
        let _order = runtime.intent_lock(&IntentKey::Order).await;
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                runtime.intent_lock(&IntentKey::Brackets)
            )
            .await
            .is_err(),
            "order and brackets share one release, so one lock"
        );
    }

    #[test]
    fn different_accounts_never_share_a_runtime() {
        let mut a = GatewayConfig::new("u-acct-key", "S", "wss://x", "Live");
        a.account_id = "A1".into();
        let mut b = a.clone();
        b.account_id = "B2".into();
        let ra = Runtime::for_config(&a);
        let rb = Runtime::for_config(&b);
        assert!(!Arc::ptr_eq(&ra, &rb));
        assert!(Arc::ptr_eq(&ra, &Runtime::for_config(&a)));
    }
}
