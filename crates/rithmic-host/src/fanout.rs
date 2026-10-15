//! Refcounted subscriptions and bounded per-client fan-out queues.

use crate::PlantEvent;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use tokio::sync::{broadcast, RwLock};

/// Default fan-out capacity: events a topic's broadcast ring, and a client's
/// outbound queue, hold. A receiver that falls further behind than this is told
/// of an exact gap; a client whose socket stops draining is disconnected by the
/// writer's timeout. Sized so a 200k events/s burst of ~100-byte frames costs a
/// few MB per topic, not so small that a brief stall drops events.
pub const DEFAULT_QUEUE_CAP: usize = 65_536;

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

/// Opaque client id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(u64);

impl SessionId {
    pub fn new() -> Self {
        Self(NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl Default for SessionId {
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
    /// Bracket updates on the order topic (only clients that subscribed to them).
    Brackets,
}

impl Kind {
    /// The bit a client's interest mask carries for this kind.
    pub fn bit(self) -> u8 {
        match self {
            Self::Any => 0,
            Self::Ticker => 1,
            Self::Book => 2,
            Self::Bars => 4,
            Self::Brackets => 8,
        }
    }
}

/// One shared event record. Keeping native and encoded payloads in alternative
/// variants preserves the compact queue slot used by remote-only hosts.
#[derive(Debug, Clone)]
pub struct Fan {
    payload: FanPayload,
}

#[derive(Debug, Clone)]
enum FanPayload {
    Flush(Arc<Flush>),
    Encoded {
        kind: Kind,
        bytes: bytes::Bytes,
    },
    Native {
        kind: Kind,
        record: Arc<EventRecord>,
    },
}

/// Rare recovery barrier. It keeps lifecycle signals behind already-published
/// records without changing event records or their shared encoding.
#[derive(Debug)]
struct Flush {
    session: SessionId,
    done: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

/// A venue record is shared across sessions. A transport may memoize its wire
/// representation; the host never encodes or interprets it.
#[derive(Debug)]
struct EventRecord {
    event: PlantEvent,
    encoded: OnceLock<bytes::Bytes>,
}

impl Fan {
    pub(crate) fn flush(session: SessionId, done: tokio::sync::oneshot::Sender<()>) -> Self {
        Self {
            payload: FanPayload::Flush(Arc::new(Flush {
                session,
                done: std::sync::Mutex::new(Some(done)),
            })),
        }
    }
    pub(crate) fn flush_for(&self, session: SessionId) -> Option<bool> {
        match &self.payload {
            FanPayload::Flush(flush) => Some(flush.session == session),
            _ => None,
        }
    }
    /// A transport acknowledges a recovery barrier after writing all preceding
    /// records. Barriers have no wire representation.
    pub fn acknowledge_flush(&self) -> bool {
        if let FanPayload::Flush(flush) = &self.payload {
            if let Some(done) = flush.done.lock().expect("flush barrier").take() {
                let _ = done.send(());
            }
            return true;
        }
        false
    }
    pub fn encoded(kind: Kind, bytes: bytes::Bytes) -> Self {
        Self {
            payload: FanPayload::Encoded { kind, bytes },
        }
    }

    pub fn native(kind: Kind, event: PlantEvent) -> Self {
        Self {
            payload: FanPayload::Native {
                kind,
                record: Arc::new(EventRecord {
                    event,
                    encoded: OnceLock::new(),
                }),
            },
        }
    }

    pub fn kind(&self) -> Kind {
        match &self.payload {
            FanPayload::Encoded { kind, .. } | FanPayload::Native { kind, .. } => *kind,
            FanPayload::Flush(_) => Kind::Any,
        }
    }

    pub fn event(&self) -> Option<&PlantEvent> {
        match &self.payload {
            FanPayload::Native { record, .. } => Some(&record.event),
            FanPayload::Encoded { .. } | FanPayload::Flush(_) => None,
        }
    }

    /// All remote consumers of a record share one encoding and allocation.
    pub fn encoded_with(&self, encode: impl FnOnce(&PlantEvent) -> bytes::Bytes) -> bytes::Bytes {
        match &self.payload {
            FanPayload::Native { record, .. } => {
                record.encoded.get_or_init(|| encode(&record.event)).clone()
            }
            FanPayload::Encoded { bytes, .. } => bytes.clone(),
            FanPayload::Flush(_) => bytes::Bytes::new(),
        }
    }
}

/// Fan-out hub: one broadcast ring per topic; every attached client has its own
/// receiver. Topics are held as `symbol -> exchange -> state` so the event hot
/// path can look one up from borrowed `&str`s without allocating a key.
/// An optional transport projection. It consumes a record only when no native
/// session can need it; mixed native/remote hosts retain the shared typed record.
pub type EventEncoder = fn(PlantEvent) -> bytes::Bytes;

pub struct FanoutHub {
    capacity: usize,
    native_sessions: AtomicUsize,
    encoder: OnceLock<EventEncoder>,
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
            native_sessions: AtomicUsize::new(0),
            encoder: OnceLock::new(),
            subs: RwLock::new(HashMap::new()),
        }
    }

    /// A host has one wire format. Installing a second encoder is refused.
    pub fn set_encoder(&self, encoder: EventEncoder) -> bool {
        self.encoder.set(encoder).is_ok()
    }
    pub(crate) fn native_attached(&self) {
        self.native_sessions.fetch_add(1, Ordering::SeqCst);
    }
    pub(crate) fn native_detached(&self) {
        self.native_sessions.fetch_sub(1, Ordering::SeqCst);
    }

    fn fan(&self, kind: Kind, event: PlantEvent) -> Fan {
        if self.native_sessions.load(Ordering::SeqCst) == 0 {
            if let Some(encode) = self.encoder.get() {
                return Fan::encoded(kind, encode(event));
            }
        }
        Fan::native(kind, event)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
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
    pub id: SessionId,
    rx: broadcast::Receiver<Fan>,
}

impl ClientQueue {
    pub fn from_receiver(id: SessionId, rx: broadcast::Receiver<Fan>) -> Self {
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

/// Shared hub handle.
pub type SharedFanout = Arc<FanoutHub>;

/// Sentinel routing keys for plant events that are not symbol/exchange scoped
/// (account PnL, order notifications). These share the [`SubKey`] refcount +
/// fan-out machinery used for market data so there is a single hub type.
pub fn pnl_key() -> SubKey {
    SubKey {
        symbol: "__pnl__".into(),
        exchange: String::new(),
    }
}

pub fn order_key() -> SubKey {
    SubKey {
        symbol: "__order__".into(),
        exchange: String::new(),
    }
}

/// Sentinel key for a gap on the ticker or history stream: those streams are
/// not scoped to one topic, so the pump publishes the gap to every symbol topic.
pub const GAP_BROADCAST: &str = "__gap__";

/// Routing key for a gap on `stream`.
pub fn gap_key(stream: crate::Stream) -> SubKey {
    use crate::Stream;
    match stream {
        Stream::Pnl => pnl_key(),
        Stream::Order => order_key(),
        Stream::Ticker | Stream::History => SubKey {
            symbol: GAP_BROADCAST.into(),
            exchange: String::new(),
        },
    }
}

/// Sentinel key for unclassified plant messages (template types with no
/// symbol/exchange to route on). Routed separately from order notifications
/// so order-update subscribers do not receive unrelated plant chatter.
pub fn other_key() -> SubKey {
    SubKey {
        symbol: "__other__".into(),
        exchange: String::new(),
    }
}

/// Where an event is delivered, described with borrowed strings so deciding it
/// allocates nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route<'a> {
    /// A symbol topic, and the kind of frame it carries.
    Topic {
        symbol: &'a str,
        exchange: &'a str,
        kind: Kind,
    },
    Pnl,
    Order,
    Other,
    /// A gap on `stream`.
    Gap(crate::Stream),
}

fn text(field: &Option<String>) -> &str {
    field.as_deref().unwrap_or("")
}

/// The route of `event`, without consuming or copying it.
pub fn route_of(event: &PlantEvent) -> Route<'_> {
    match event {
        PlantEvent::LastTrade(t) => Route::Topic {
            symbol: text(&t.symbol),
            exchange: text(&t.exchange),
            kind: Kind::Ticker,
        },
        PlantEvent::Bbo(b) => Route::Topic {
            symbol: text(&b.symbol),
            exchange: text(&b.exchange),
            kind: Kind::Ticker,
        },
        PlantEvent::OrderBook(o) => Route::Topic {
            symbol: text(&o.symbol),
            exchange: text(&o.exchange),
            kind: Kind::Book,
        },
        PlantEvent::TimeBar(b) => Route::Topic {
            symbol: text(&b.symbol),
            exchange: text(&b.exchange),
            kind: Kind::Bars,
        },
        PlantEvent::AccountPnl(_) | PlantEvent::InstrumentPnl(_) => Route::Pnl,
        // Bracket updates ride the order-plant stream, so they share its topic.
        PlantEvent::OrderNotification(_) | PlantEvent::BracketUpdate(_) => Route::Order,
        PlantEvent::Gap(g) => Route::Gap(g.stream),
        PlantEvent::Other { .. } => Route::Other,
    }
}

/// Route a venue event before allocating its shared delivery record.
pub async fn publish_plant_event(hub: &FanoutHub, event: PlantEvent) {
    match route_of(&event) {
        Route::Topic {
            symbol,
            exchange,
            kind,
        } => {
            let Some(tx) = hub.live_sender(symbol, exchange).await else {
                return;
            };
            let _ = tx.send(hub.fan(kind, event));
        }
        Route::Gap(crate::Stream::Ticker | crate::Stream::History) => {
            hub.publish_symbols(hub.fan(Kind::Any, event)).await;
        }
        route => {
            let key = match route {
                Route::Pnl => pnl_key(),
                Route::Order => order_key(),
                Route::Other => other_key(),
                Route::Gap(stream) => gap_key(stream),
                Route::Topic { .. } => unreachable!(),
            };
            let kind = if matches!(event, PlantEvent::BracketUpdate(_)) {
                Kind::Brackets
            } else {
                Kind::Any
            };
            hub.publish(&key, hub.fan(kind, event)).await;
        }
    }
}
