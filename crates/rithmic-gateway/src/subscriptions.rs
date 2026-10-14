//! Refcounted subscriptions and bounded per-client fan-out queues.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};

/// Default fan-out capacity: events a topic's broadcast ring, and a client's
/// outbound queue, hold. A receiver that falls further behind than this is told
/// of an exact gap; a client whose socket stops draining is disconnected by the
/// writer's timeout. Sized so a 200k events/s burst of ~100-byte frames costs a
/// few MB per topic, not so small that a brief stall drops events.
pub const DEFAULT_QUEUE_CAP: usize = 65_536;

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// Opaque client id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(u64);

impl ClientId {
    pub fn new() -> Self {
        Self(NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl Default for ClientId {
    fn default() -> Self {
        Self::new()
    }
}

/// Subscription key for MD / history live feeds.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubKey {
    pub symbol: String,
    pub exchange: String,
}

/// Fan-out hub: one broadcast channel per sub key + per-client receivers.
/// What a fanned-out frame is, so a client's forwarder can drop kinds that client
/// never asked for without writing them to its socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Delivered to every attached client (account/order events, gaps).
    Any,
    /// LastTrade / BBO.
    Ticker,
    /// Order-book summary.
    Book,
    /// Live time bars.
    Bars,
}

impl Kind {
    /// The bit a client's interest mask carries for this kind.
    pub fn bit(self) -> u8 {
        match self {
            Self::Any => 0,
            Self::Ticker => 1,
            Self::Book => 2,
            Self::Bars => 4,
        }
    }
}

/// One encoded frame on a topic. Cloning shares the bytes (a refcount bump).
#[derive(Debug, Clone)]
pub struct Fan {
    pub kind: Kind,
    pub bytes: bytes::Bytes,
}

/// Fan-out hub: one broadcast ring per topic; every attached client has its own
/// receiver. Topics are held as `symbol -> exchange -> state` so the event hot
/// path can look one up from borrowed `&str`s without allocating a key.
pub struct FanoutHub {
    capacity: usize,
    subs: RwLock<HashMap<String, HashMap<String, SubState>>>,
}

struct SubState {
    refcount: usize,
    tx: broadcast::Sender<Fan>,
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
        let by_exchange = guard.entry(key.symbol).or_default();
        match by_exchange.get_mut(&key.exchange) {
            Some(st) => {
                st.refcount += 1;
                false
            }
            None => {
                let (tx, _) = broadcast::channel(self.capacity);
                by_exchange.insert(key.exchange, SubState { refcount: 1, tx });
                true
            }
        }
    }

    /// Drop client interest; returns whether venue unsubscribe is needed (refcount →0).
    pub async fn remove_interest(&self, key: &SubKey) -> bool {
        let mut guard = self.subs.write().await;
        let Some(by_exchange) = guard.get_mut(&key.symbol) else {
            return false;
        };
        let remove = match by_exchange.get_mut(&key.exchange) {
            Some(st) => {
                st.refcount = st.refcount.saturating_sub(1);
                st.refcount == 0
            }
            None => false,
        };
        if remove {
            by_exchange.remove(&key.exchange);
            if by_exchange.is_empty() {
                guard.remove(&key.symbol);
            }
        }
        remove
    }

    /// Subscribe a receiver to an existing key (after add_interest).
    pub async fn subscribe_receiver(&self, key: &SubKey) -> Option<broadcast::Receiver<Fan>> {
        let guard = self.subs.read().await;
        guard
            .get(&key.symbol)?
            .get(&key.exchange)
            .map(|st| st.tx.subscribe())
    }

    /// The topic's sender, looked up from borrowed strings, when at least one
    /// client is attached to it. `None` means nobody would receive the event, so
    /// the caller can skip converting and encoding it altogether.
    pub async fn live_sender(
        &self,
        symbol: &str,
        exchange: &str,
    ) -> Option<broadcast::Sender<Fan>> {
        let guard = self.subs.read().await;
        let st = guard.get(symbol)?.get(exchange)?;
        (st.tx.receiver_count() > 0).then(|| st.tx.clone())
    }

    /// Publish to every symbol topic (not the internal `__*__` sentinels). Used
    /// for a gap on the ticker or history stream, which affects every symbol.
    pub async fn publish_symbols(&self, fan: Fan) -> usize {
        let guard = self.subs.read().await;
        guard
            .iter()
            .filter(|(symbol, _)| !symbol.starts_with("__"))
            .flat_map(|(_, by_exchange)| by_exchange.values())
            .map(|st| st.tx.send(fan.clone()).unwrap_or(0))
            .sum()
    }

    /// Publish an event to all receivers of `key`.
    pub async fn publish(&self, key: &SubKey, fan: Fan) -> usize {
        let guard = self.subs.read().await;
        match guard.get(&key.symbol).and_then(|m| m.get(&key.exchange)) {
            Some(st) => st.tx.send(fan).unwrap_or(0),
            None => 0,
        }
    }
}

/// Per-client bounded queue. A lagging consumer gets [`ClientQueueError::Overflow`]
/// carrying the exact number of skipped frames.
pub struct ClientQueue {
    pub id: ClientId,
    rx: broadcast::Receiver<Fan>,
}

impl ClientQueue {
    pub fn from_receiver(id: ClientId, rx: broadcast::Receiver<Fan>) -> Self {
        Self { id, rx }
    }

    pub async fn recv(&mut self) -> Result<Fan, ClientQueueError> {
        match self.rx.recv().await {
            Ok(f) => Ok(f),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                Err(ClientQueueError::Overflow { skipped })
            }
            Err(broadcast::error::RecvError::Closed) => Err(ClientQueueError::Closed),
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ClientQueueError {
    #[error("client queue overflow: {skipped} frames skipped")]
    Overflow { skipped: u64 },
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
        let mut s = vec!["md".into(), "history".into(), "pnl".into()];
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
