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

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::Duration;

use prost::Message;
use rithmic_plants::dto::PlantEvent;
use rithmic_plants::{Error, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;
use tokio::sync::{broadcast, oneshot, Mutex as TokioMutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::attach::{attach, parent_flock_held};
use super::convert::{plant_event_from_pb, Channel};
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

    fn wake_all(&self) {
        for channel in Channel::ALL {
            self.get(channel).notify.notify_waiters();
        }
    }
}

// --- one connection ----------------------------------------------------------

pub(super) struct Connection {
    id: u64,
    writer: TokioMutex<OwnedWriteHalf>,
    pending: StdMutex<HashMap<u64, oneshot::Sender<Frame>>>,
    next_id: AtomicU64,
    closed: AtomicBool,
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
    /// close the write half so the parent drops the connection and the reader's
    /// end-of-stream reports the loss (the normal re-dial path).
    async fn break_connection(&self, writer: &mut OwnedWriteHalf) {
        self.fail_all();
        let _ = tokio::time::timeout(DISCONNECT_GRACE, writer.shutdown()).await;
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

async fn write_frame(
    stream: &mut UnixStream,
    frame: &Frame,
    timeout: Duration,
) -> std::io::Result<()> {
    let bytes = crate::codec::encode(frame)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    tokio::time::timeout(timeout, stream.write_all(&bytes))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "write timed out"))?
}

async fn reader_loop(conn: Arc<Connection>, mut read: OwnedReadHalf, runtime: Weak<Runtime>) {
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
            // The parent rebuilt its plants and restored this client's
            // subscriptions: the socket stays up, the adapter is told as it would
            // be after a re-dial.
            if let Some(pb::event::Body::TransportResumed(_)) = &event.body {
                if let Some(rt) = runtime.upgrade() {
                    rt.on_resumed();
                }
                continue;
            }
            if let (Some(rt), Some(event)) = (runtime.upgrade(), plant_event_from_pb(event)) {
                rt.deliver(event);
            }
        }
    }
    conn.fail_all();
    if !conn.intentional.load(Ordering::SeqCst) {
        if let Some(rt) = runtime.upgrade() {
            rt.on_connection_lost(conn.id);
        }
    }
}

// --- handshake ----------------------------------------------------------------

enum HandshakeError {
    /// Parent not ready / silent: re-dial until the attach deadline.
    Retry(Error),
    Fatal(Error),
}

async fn handshake(
    stream: &mut UnixStream,
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

    let (mut read, _write) = stream.split();
    let reply = read_frame(&mut read);
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

// --- the runtime ---------------------------------------------------------------

pub(super) struct Runtime {
    pub(super) cfg: GatewayConfig,
    conn: StdMutex<Option<Arc<Connection>>>,
    /// Serializes (re)dials: exactly one re-dial for all waiters.
    redial: TokioMutex<()>,
    generation: AtomicU64,
    next_conn_id: AtomicU64,
    ready: StdMutex<ReadyInfo>,
    facades: StdMutex<HashMap<u64, Arc<Queues>>>,
    next_facade_id: AtomicU64,
    transport_tx: broadcast::Sender<TransportEvent>,
    failed_tx: broadcast::Sender<RecoveryFailed>,
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
        let key = format!(
            "{}|{}|{}|{}|{}",
            cfg.user,
            cfg.system_name,
            cfg.url,
            cfg.env,
            cfg.socket_path().display()
        );
        let mut map = registry().lock().expect("registry");
        if let Some(existing) = map.get(&key).and_then(Weak::upgrade) {
            return existing;
        }
        let runtime = Arc::new_cyclic(|weak| Runtime {
            cfg: cfg.clone(),
            conn: StdMutex::new(None),
            redial: TokioMutex::new(()),
            generation: AtomicU64::new(0),
            next_conn_id: AtomicU64::new(1),
            ready: StdMutex::new(ReadyInfo::default()),
            facades: StdMutex::new(HashMap::new()),
            next_facade_id: AtomicU64::new(1),
            transport_tx: broadcast::channel(16).0,
            failed_tx: broadcast::channel(4).0,
            stopping: AtomicBool::new(false),
            recovering: AtomicBool::new(false),
            self_weak: weak.clone(),
        });
        map.retain(|_, w| w.strong_count() > 0);
        map.insert(key, Arc::downgrade(&runtime));
        runtime
    }

    pub(super) fn register(&self, queues: Arc<Queues>) -> u64 {
        let id = self.next_facade_id.fetch_add(1, Ordering::SeqCst);
        self.facades.lock().expect("facades").insert(id, queues);
        id
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
    }

    pub(super) fn live(&self) -> Option<Arc<Connection>> {
        self.conn
            .lock()
            .expect("conn")
            .as_ref()
            .filter(|c| !c.is_closed())
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

    pub(super) fn allow_recovery(&self) {
        self.stopping.store(false, Ordering::SeqCst);
    }

    /// The parent rebuilt its plants without dropping this connection. Surface it
    /// as the same Down / Up pair a re-dial produces, with a new generation, so the
    /// adapter's recovery (re-arm exec, replay market data) runs unchanged.
    fn on_resumed(&self) {
        let previous = self.generation.fetch_add(1, Ordering::SeqCst);
        let _ = self.transport_tx.send(TransportEvent::Down {
            generation: previous,
        });
        let _ = self.transport_tx.send(TransportEvent::Up {
            generation: previous + 1,
        });
        self.wake_all();
    }

    fn on_connection_lost(&self, conn_id: u64) {
        let is_current = self
            .conn
            .lock()
            .expect("conn")
            .as_ref()
            .is_some_and(|c| c.id == conn_id);
        if is_current {
            let _ = self.transport_tx.send(TransportEvent::Down {
                generation: self.generation(),
            });
            self.wake_all();
            self.start_recovery();
        }
    }

    /// One background flight: re-dial with exponential backoff until the
    /// transport is live again or the attempt budget is spent. Shares the
    /// single-flight re-dial with any RPC that recovers at the same time.
    fn start_recovery(&self) {
        let cfg = &self.cfg;
        if cfg.recovery_attempts == 0 || self.stopping.load(Ordering::SeqCst) {
            return;
        }
        if self.recovering.swap(true, Ordering::SeqCst) {
            return;
        }
        let Some(runtime) = self.self_weak.upgrade() else {
            self.recovering.store(false, Ordering::SeqCst);
            return;
        };
        tokio::spawn(async move {
            let cfg = &runtime.cfg;
            let mut backoff = cfg.recovery_initial_backoff;
            let mut attempts = 0u32;
            let mut last: Option<Error> = None;
            while attempts < cfg.recovery_attempts {
                if runtime.stopping.load(Ordering::SeqCst) {
                    runtime.recovering.store(false, Ordering::SeqCst);
                    return;
                }
                attempts += 1;
                match runtime.ensure_live().await {
                    Ok(_) => {
                        runtime.recovering.store(false, Ordering::SeqCst);
                        return;
                    }
                    Err(err) => {
                        last = Some(err);
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(cfg.recovery_max_backoff);
                    }
                }
            }
            runtime.recovering.store(false, Ordering::SeqCst);
            if !runtime.stopping.load(Ordering::SeqCst) {
                let _ = runtime.failed_tx.send(RecoveryFailed {
                    generation: runtime.generation(),
                    attempts,
                    cause: last.map(|e| e.to_string()).unwrap_or_default(),
                });
            }
        });
    }

    /// A live connection, (re)dialing it if needed. Single-flight: concurrent
    /// callers during a drop share one re-dial and one generation bump.
    pub(super) async fn ensure_live(&self) -> Result<Arc<Connection>> {
        if let Some(conn) = self.live() {
            return Ok(conn);
        }
        let _redial = self.redial.lock().await;
        if let Some(conn) = self.live() {
            return Ok(conn);
        }
        let conn = self.dial().await?;
        *self.ready.lock().expect("ready") = conn.ready.clone();
        *self.conn.lock().expect("conn") = Some(Arc::clone(&conn));
        let previous = self.generation.fetch_add(1, Ordering::SeqCst);
        let generation = previous + 1;
        // The first connect of a process is not a recovery: no Up event.
        if previous > 0 {
            let _ = self.transport_tx.send(TransportEvent::Up { generation });
        }
        self.wake_all();
        Ok(conn)
    }

    /// Attach, attest the parent holds the flock, and handshake.
    async fn dial(&self) -> Result<Arc<Connection>> {
        let cfg = &self.cfg;
        let deadline = Instant::now() + cfg.attach_timeout;
        let mut stream = attach(cfg).await?;
        loop {
            if cfg.attest_flock && !parent_flock_held(cfg) {
                return Err(Error::Gateway {
                    code: "parent_unattested".into(),
                    message:
                        "listen path is up but credential flock is free — refusing impostor parent"
                            .into(),
                });
            }
            match handshake(&mut stream, cfg).await {
                Ok(ready) => return Ok(self.start_connection(stream, ready)),
                Err(HandshakeError::Fatal(e)) => return Err(e),
                Err(HandshakeError::Retry(e)) if Instant::now() >= deadline => return Err(e),
                Err(HandshakeError::Retry(_)) => {
                    drop(stream);
                    tokio::time::sleep(HANDSHAKE_RETRY_PAUSE).await;
                    stream = attach(cfg).await?;
                }
            }
        }
    }

    fn start_connection(&self, stream: UnixStream, ready: ReadyInfo) -> Arc<Connection> {
        let (read, write) = stream.into_split();
        let conn = Arc::new(Connection {
            id: self.next_conn_id.fetch_add(1, Ordering::SeqCst),
            writer: TokioMutex::new(write),
            pending: StdMutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
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
