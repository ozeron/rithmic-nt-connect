//! Refcounted subscriptions and bounded per-client fan-out queues.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};

/// Default per-client outbound queue capacity.
pub const DEFAULT_QUEUE_CAP: usize = 1024;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// Opaque client id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(u64);

impl ClientId {
    pub fn new() -> Self {
        Self(NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

/// Subscription key for MD / history live feeds.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubKey {
    pub symbol: String,
    pub exchange: String,
}

/// Fan-out hub: one broadcast channel per sub key + per-client receivers.
pub struct FanoutHub {
    capacity: usize,
    subs: RwLock<HashMap<SubKey, SubState>>,
}

struct SubState {
    refcount: usize,
    tx: broadcast::Sender<bytes::Bytes>,
}

impl FanoutHub {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            subs: RwLock::new(HashMap::new()),
        }
    }

    /// Subscribe client interest; returns whether venue subscribe is needed (refcount 0→1).
    pub async fn add_interest(&self, key: SubKey) -> bool {
        let mut guard = self.subs.write().await;
        match guard.get_mut(&key) {
            Some(st) => {
                st.refcount += 1;
                false
            }
            None => {
                let (tx, _) = broadcast::channel(self.capacity);
                guard.insert(key, SubState { refcount: 1, tx });
                true
            }
        }
    }

    /// Drop client interest; returns whether venue unsubscribe is needed (refcount →0).
    pub async fn remove_interest(&self, key: &SubKey) -> bool {
        let mut guard = self.subs.write().await;
        let remove = if let Some(st) = guard.get_mut(key) {
            st.refcount = st.refcount.saturating_sub(1);
            st.refcount == 0
        } else {
            false
        };
        if remove {
            guard.remove(key);
        }
        remove
    }

    /// Subscribe a receiver to an existing key (after add_interest).
    pub async fn subscribe_receiver(&self, key: &SubKey) -> Option<broadcast::Receiver<bytes::Bytes>> {
        let guard = self.subs.read().await;
        guard.get(key).map(|st| st.tx.subscribe())
    }

    /// Publish an event to all receivers of `key`.
    pub async fn publish(&self, key: &SubKey, payload: bytes::Bytes) -> usize {
        let guard = self.subs.read().await;
        match guard.get(key) {
            Some(st) => st.tx.send(payload).unwrap_or(0),
            None => 0,
        }
    }
}

/// Per-client bounded queue. Lagging consumers get [`ClientQueueError::Overflow`].
pub struct ClientQueue {
    pub id: ClientId,
    rx: broadcast::Receiver<bytes::Bytes>,
}

impl ClientQueue {
    pub fn from_receiver(id: ClientId, rx: broadcast::Receiver<bytes::Bytes>) -> Self {
        Self { id, rx }
    }

    pub async fn recv(&mut self) -> Result<bytes::Bytes, ClientQueueError> {
        loop {
            match self.rx.recv().await {
                Ok(b) => return Ok(b),
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    return Err(ClientQueueError::Overflow);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(ClientQueueError::Closed);
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ClientQueueError {
    #[error("client queue overflow — disconnect slow client")]
    Overflow,
    #[error("client queue closed")]
    Closed,
}

/// Parent-side capability gates.
#[derive(Debug, Clone, Copy, Default)]
pub struct ParentGates {
    pub trading_enabled: bool,
    pub cancel_all_enabled: bool,
}

impl ParentGates {
    pub fn from_env() -> Self {
        Self {
            trading_enabled: env_truthy("RITHMIC_ENABLE_TRADING"),
            cancel_all_enabled: env_truthy("RITHMIC_GATEWAY_CANCEL_ALL"),
        }
    }

    pub fn allow_place(&self) -> bool {
        self.trading_enabled
    }

    pub fn allow_cancel_all(&self) -> bool {
        self.cancel_all_enabled
    }

    pub fn scopes(&self) -> Vec<String> {
        let mut s = vec![
            "md".into(),
            "history".into(),
            "pnl".into(),
        ];
        if self.trading_enabled {
            s.push("trade".into());
        }
        if self.cancel_all_enabled {
            s.push("cancel_all".into());
        }
        s
    }
}

fn env_truthy(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

/// Shared hub handle.
pub type SharedFanout = Arc<FanoutHub>;
