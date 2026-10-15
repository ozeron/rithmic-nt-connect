//! Host lifecycle and concurrency. No transport, authentication or credentials.
use crate::fanout::{FanoutHub, SharedFanout, SubKey, DEFAULT_QUEUE_CAP};
use crate::gates::ParentGates;
use crate::intents::ReconnectController;
use crate::pump::Timing;
use crate::{Error, LocalSession, PlantSet, Result, Venue};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tokio::sync::{watch, Mutex, RwLock};

/// The parent's plant session behind the [`Venue`] trait, guarded by one gate.
///
/// The gate only separates *using* the plants from *rebuilding* them. RPCs take it
/// shared ([`VenueHandle::lock`]) and run concurrently, so no call waits behind
/// another; reconnect and shutdown take it exclusively
/// ([`VenueHandle::lock_exclusive`]), which waits for in-flight calls and keeps
/// new ones out. Waiting for events needs no gate at all: the pump uses
/// [`VenueHandle::checkout`] once and then runs on the shared session.
pub struct VenueHandle<V: Venue + ?Sized = dyn Venue> {
    gate: Arc<RwLock<()>>,
    session: Arc<V>,
}

/// Shared guard returned by [`VenueHandle::lock`]; derefs to the session. Any
/// number of RPCs hold it at once, so a slow call never queues the others.
pub struct VenueGuard<'a, V: Venue + ?Sized = dyn Venue> {
    _gate: tokio::sync::RwLockReadGuard<'a, ()>,
    session: &'a Arc<V>,
}

impl<V: Venue + ?Sized> std::ops::Deref for VenueGuard<'_, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.session
    }
}

/// Exclusive guard for rebuilding the plants (reconnect, shutdown): it waits for
/// in-flight RPCs to finish and keeps new ones out while it is held.
pub struct ExclusiveVenueGuard<'a, V: Venue + ?Sized = dyn Venue> {
    _gate: tokio::sync::RwLockWriteGuard<'a, ()>,
    session: &'a Arc<V>,
}

impl<V: Venue + ?Sized> std::ops::Deref for ExclusiveVenueGuard<'_, V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.session
    }
}

impl<V: Venue + ?Sized> VenueHandle<V> {
    pub fn new(session: Arc<V>) -> Self {
        Self {
            gate: Arc::new(RwLock::new(())),
            session,
        }
    }

    /// Shared access: RPCs run concurrently and only wait while plants are being
    /// rebuilt.
    pub async fn lock(&self) -> VenueGuard<'_, V> {
        VenueGuard {
            _gate: self.gate.read().await,
            session: &self.session,
        }
    }

    /// Exclusive access, for disconnect / connect / restore.
    pub async fn lock_exclusive(&self) -> ExclusiveVenueGuard<'_, V> {
        ExclusiveVenueGuard {
            _gate: self.gate.write().await,
            session: &self.session,
        }
    }

    /// Wait out any rebuild, then return the session without holding the gate.
    pub async fn checkout(&self) -> Arc<V> {
        drop(self.gate.read().await);
        Arc::clone(&self.session)
    }
}

impl<V: Venue + ?Sized> Clone for VenueHandle<V> {
    fn clone(&self) -> Self {
        Self {
            gate: self.gate.clone(),
            session: self.session.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryPolicy {
    Allow,
    RefuseUnderLiveMd,
}

pub struct HostState<V: Venue + ?Sized + 'static = dyn Venue> {
    pub gates: crate::gates::HostGates,
    pub hub: SharedFanout,
    pub ready: AtomicBool,
    pub force_reconnect: AtomicBool,
    pub recovering: AtomicBool,
    pub failed: AtomicBool,
    pub interruption: watch::Sender<u64>,
    pub(crate) sessions:
        std::sync::Mutex<HashMap<crate::SessionId, std::sync::Weak<crate::local::LocalInner<V>>>>,
    pub reconnect_wake: tokio::sync::Notify,
    pub session: Option<VenueHandle<V>>,
    pub reconnect: Arc<ReconnectController>,
    pub topic_locks: Mutex<HashMap<SubKey, Arc<Mutex<()>>>>,
    pub recon_lock: Mutex<()>,
    pub md_history_gate: Mutex<()>,
    pub history_load_lock: Mutex<()>,
    pub transport_generation: AtomicU64,
    pub transport_epoch: watch::Sender<u64>,
    pub timing: Timing,
    pub history_policy: HistoryPolicy,
}

impl<V: Venue + ?Sized + 'static> HostState<V> {
    pub(crate) async fn notify_sessions(&self, event: crate::SessionEvent) {
        let sessions: Vec<_> = self
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter_map(std::sync::Weak::upgrade)
            .collect();
        let mut tasks = tokio::task::JoinSet::new();
        for inner in sessions {
            let event = event.clone();
            tasks.spawn(async move {
                crate::local::LocalSession::from_inner(inner)
                    .notify_event(event)
                    .await;
            });
        }
        while tasks.join_next().await.is_some() {}
    }

    pub fn request_reconnect(&self) {
        self.recovering.store(true, Ordering::SeqCst);
        self.ready.store(false, Ordering::SeqCst);
        self.force_reconnect.store(true, Ordering::SeqCst);
        self.reconnect_wake.notify_one();
    }

    pub async fn topic_lock(&self, key: &SubKey) -> Arc<Mutex<()>> {
        self.topic_locks
            .lock()
            .await
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub fn check_ready(&self) -> Result<()> {
        if self.ready.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(Error::NotSent(
                "gateway not ready (plants reconnecting); request not sent".into(),
            ))
        }
    }
}

pub struct SessionHost<V: Venue + ?Sized + 'static = dyn Venue> {
    pub state: Arc<HostState<V>>,
    pump: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    credential_lock: std::sync::Mutex<Option<crate::SessionLock>>,
    pub(crate) runtime: Option<tokio::runtime::Handle>,
    pub(crate) attachments: std::sync::atomic::AtomicUsize,
    attachment_gate: std::sync::Mutex<()>,
    closing: AtomicBool,
    shutdown_done: AtomicBool,
    shutdown_gate: Mutex<()>,
}

impl<V: Venue + 'static> SessionHost<V> {
    /// Start local delivery paths. `connect` on an attachment ensures the login.
    pub fn start(venue: V, gates: ParentGates, timing: Timing, policy: HistoryPolicy) -> Arc<Self> {
        Self::with_handle(
            Some(VenueHandle::new(Arc::new(venue))),
            gates,
            timing,
            policy,
            false,
            DEFAULT_QUEUE_CAP,
        )
    }
}

impl<V: Venue + ?Sized + 'static> SessionHost<V> {
    /// Adopt a venue connected by the composition root (gateway startup).
    pub fn from_connected(
        venue: VenueHandle<V>,
        gates: ParentGates,
        timing: Timing,
        policy: HistoryPolicy,
    ) -> Arc<Self> {
        Self::with_handle(Some(venue), gates, timing, policy, true, DEFAULT_QUEUE_CAP)
    }

    /// Explicit capacity is useful for bounded-queue characterization tests.
    pub fn with_handle(
        venue: Option<VenueHandle<V>>,
        gates: ParentGates,
        timing: Timing,
        policy: HistoryPolicy,
        connected: bool,
        capacity: usize,
    ) -> Arc<Self> {
        let hub = Arc::new(FanoutHub::new(capacity));
        let (transport_epoch, _) = watch::channel(1);
        let state = Arc::new(HostState {
            gates: crate::gates::HostGates::new(gates),
            reconnect: Arc::new(ReconnectController::new(hub.clone())),
            hub,
            ready: AtomicBool::new(connected),
            force_reconnect: AtomicBool::new(false),
            recovering: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            interruption: watch::channel(0).0,
            sessions: std::sync::Mutex::new(HashMap::new()),
            reconnect_wake: tokio::sync::Notify::new(),
            session: venue,
            topic_locks: Mutex::new(HashMap::new()),
            recon_lock: Mutex::new(()),
            md_history_gate: Mutex::new(()),
            history_load_lock: Mutex::new(()),
            transport_generation: AtomicU64::new(1),
            transport_epoch,
            timing,
            history_policy: policy,
        });
        let host = Arc::new(Self {
            state,
            pump: std::sync::Mutex::new(None),
            credential_lock: std::sync::Mutex::new(None),
            runtime: tokio::runtime::Handle::try_current().ok(),
            attachments: std::sync::atomic::AtomicUsize::new(0),
            attachment_gate: std::sync::Mutex::new(()),
            closing: AtomicBool::new(false),
            shutdown_done: AtomicBool::new(false),
            shutdown_gate: Mutex::new(()),
        });
        if host.state.session.is_some() {
            let state = host.state.clone();
            *host.pump.lock().unwrap_or_else(|e| e.into_inner()) =
                Some(tokio::spawn(crate::pump::run_event_pump(state)));
        }
        host
    }

    pub fn attach(self: &Arc<Self>) -> LocalSession<V> {
        let _attach = self
            .attachment_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        LocalSession::attach(self.clone(), false)
    }
    /// Only the adapter composition root creates attachments that may open gates.
    pub fn attach_owned(self: &Arc<Self>) -> Result<LocalSession<V>> {
        let _attach = self
            .attachment_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if self.is_terminal() {
            return Err(Error::NotSent("host closed; start a new host".into()));
        }
        Ok(LocalSession::attach(self.clone(), true))
    }
    pub(crate) fn hold_lock(&self, lock: crate::SessionLock) {
        *self
            .credential_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(lock);
    }
    pub fn is_terminal(&self) -> bool {
        self.closing.load(Ordering::SeqCst) || self.state.failed.load(Ordering::SeqCst)
    }
    pub(crate) async fn detached(&self) {
        let last = {
            let _attach = self
                .attachment_gate
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let last = self.attachments.fetch_sub(1, Ordering::SeqCst) == 1
                && self
                    .credential_lock
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_some();
            if last {
                self.closing.store(true, Ordering::SeqCst);
            }
            last
        };
        if last {
            self.shutdown().await;
        }
    }
    pub fn generation(&self) -> u64 {
        self.state.transport_generation.load(Ordering::SeqCst)
    }
    pub fn subscribe_generation(&self) -> watch::Receiver<u64> {
        self.state.transport_epoch.subscribe()
    }
    /// Compatibility for pre-host gateway clients; new consumer sessions never reset plants.
    pub async fn reset_ticker_for_legacy_client(&self) -> Result<()> {
        crate::local::subscriptions::reset_ticker_plant(&self.state).await
    }
    pub async fn request_plants(&self, extra: PlantSet) -> Result<()> {
        self.state
            .session
            .as_ref()
            .ok_or_else(crate::local::no_venue)?
            .lock()
            .await
            .request_plants(extra)
            .await
    }
    pub async fn connect(&self) -> Result<()> {
        let venue = self
            .state
            .session
            .as_ref()
            .ok_or_else(crate::local::no_venue)?;
        loop {
            let mut generation = self.subscribe_generation();
            let guard = venue.lock_exclusive().await;
            if self.is_terminal() {
                return Err(Error::Session(
                    "host recovery failed; start a new host".into(),
                ));
            }
            if self.state.ready.load(Ordering::SeqCst) {
                return Ok(());
            }
            // The reconnect loop owns connect + restore. A concurrent consumer
            // connect waits for its completed generation rather than exposing a
            // half-restored venue or starting a second login.
            if self.state.recovering.load(Ordering::SeqCst) {
                drop(guard);
                generation
                    .changed()
                    .await
                    .map_err(|_| Error::Session("host closed during reconnect".into()))?;
                continue;
            }
            match guard.connect().await {
                Ok(()) | Err(Error::AlreadyConnected) => {}
                Err(e) => {
                    let _ = guard.disconnect().await;
                    return Err(e);
                }
            }
            self.state.ready.store(true, Ordering::SeqCst);
            self.state.reconnect_wake.notify_one();
            return Ok(());
        }
    }

    pub async fn shutdown(&self) {
        let _shutdown = self.shutdown_gate.lock().await;
        if self.shutdown_done.load(Ordering::SeqCst) {
            return;
        }
        self.closing.store(true, Ordering::SeqCst);
        let pump = self.pump.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(pump) = pump {
            pump.abort();
            let _ = pump.await;
        }
        self.state.ready.store(false, Ordering::SeqCst);
        let sessions: Vec<_> = self
            .state
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter_map(std::sync::Weak::upgrade)
            .collect();
        for inner in sessions {
            crate::local::LocalSession::from_inner(inner).close_streams();
        }
        if let Some(venue) = &self.state.session {
            let _ = venue.lock_exclusive().await.disconnect().await;
        }
        self.credential_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        self.shutdown_done.store(true, Ordering::SeqCst);
    }
}
impl<V: Venue + ?Sized + 'static> Drop for SessionHost<V> {
    fn drop(&mut self) {
        if let Some(pump) = self
            .pump
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            pump.abort();
        }
        if !self.shutdown_done.load(Ordering::SeqCst) {
            let state = self.state.clone();
            let lock = self
                .credential_lock
                .get_mut()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            if let Some(runtime) = &self.runtime {
                runtime.spawn(async move {
                    if let Some(venue) = &state.session {
                        let _ = venue.lock_exclusive().await.disconnect().await;
                    }
                    drop(lock);
                });
            }
        }
    }
}
