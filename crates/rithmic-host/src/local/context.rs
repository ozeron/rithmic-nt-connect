use super::*;
use crate::fanout::*;
use crate::intents::TimeBarIntent;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU8;
use subscriptions::TopicIntent;

/// Per-attachment topic delivery and typed venue intent bookkeeping.
pub(super) struct SessionCtx {
    pub(super) subscribed: HashSet<SubKey>,
    pub(super) ticker: HashSet<SubKey>,
    pub(super) book: HashSet<SubKey>,
    pub(super) time_bars: HashSet<TimeBarIntent>,
    pub(super) pnl: bool,
    pub(super) order: bool,
    pub(super) brackets: bool,
    /// Per topic: which frame kinds this client asked for (bit mask of
    /// [`Kind::bit`]). The publisher skips records of other kinds.
    pub(super) kinds: HashMap<SubKey, Arc<AtomicU8>>,
    pub(super) topics: HashMap<SubKey, Arc<DeliveryTopic>>,
    pub(super) delivery: Arc<delivery::Delivery>,
}

impl SessionCtx {
    pub(super) fn new(delivery: Arc<delivery::Delivery>) -> Self {
        Self {
            subscribed: HashSet::new(),
            ticker: HashSet::new(),
            book: HashSet::new(),
            time_bars: HashSet::new(),
            pnl: false,
            order: false,
            brackets: false,
            kinds: HashMap::new(),
            topics: HashMap::new(),
            delivery,
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
        if self.brackets && *key == crate::fanout::order_key() {
            mask |= Kind::Brackets.bit();
        }
        if mask == 0 && !self.subscribed.contains(key) {
            // No delivery and nothing wanted: keep no empty entry around.
            self.kinds.remove(key);
            return;
        }
        self.kinds
            .entry(key.clone())
            .or_insert_with(|| Arc::new(AtomicU8::new(0)))
            .store(mask, Ordering::SeqCst);
    }

    pub(super) fn wants_md(&self, key: &SubKey) -> bool {
        self.ticker.contains(key)
            || self.book.contains(key)
            || self
                .time_bars
                .iter()
                .any(|tb| tb.symbol == key.symbol && tb.exchange == key.exchange)
    }

    /// Attach this client to `key`'s fan-out topic (idempotent). The topic
    /// must already exist in the hub (caller subscribes hub interest first).
    pub(super) async fn ensure_delivery(&mut self, hub: &FanoutHub, key: &SubKey) {
        if self.subscribed.contains(key) {
            return;
        }
        let kinds = Arc::clone(
            self.kinds
                .entry(key.clone())
                .or_insert_with(|| Arc::new(AtomicU8::new(0))),
        );
        if let Some(topic) = hub
            .subscribe_delivery(key, self.delivery.clone(), kinds)
            .await
        {
            self.topics.insert(key.clone(), topic);
            self.subscribed.insert(key.clone());
        }
    }

    pub(super) async fn flush(&self) {
        if let Some(ack) = self.delivery.flush() {
            // A stalled writer cannot block recovery indefinitely.
            let _ = tokio::time::timeout(Duration::from_secs(2), ack).await;
        }
    }

    pub(super) fn drop_delivery(&mut self, key: &SubKey) {
        self.topics.remove(key);
        self.kinds.remove(key);
        self.subscribed.remove(key);
    }

    /// Forget one typed intent and leave the venue topic when this was the
    /// last peer. Shared by all MD teardown drains via [`TopicIntent`].
    pub(super) async fn teardown_topic<V: Venue + ?Sized + 'static>(
        state: &HostState<V>,
        intent: subscriptions::TopicIntent,
    ) {
        let key = intent.key();
        let lock = super::topic_lock(state, &key).await;
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
    pub(crate) async fn teardown<V: Venue + ?Sized + 'static>(&mut self, state: &HostState<V>) {
        self.topics.clear();

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

    pub(super) async fn teardown_pnl<V: Venue + ?Sized + 'static>(&mut self, state: &HostState<V>) {
        // The client is gone: nothing to roll back to, nobody to tell.
        if let Err((code, e)) = subscriptions::leave_pnl(state, self, false).await {
            eprintln!("rithmic-gateway: teardown {code}: {e}");
        }
    }

    pub(super) async fn teardown_order_plants<V: Venue + ?Sized + 'static>(
        &mut self,
        state: &HostState<V>,
    ) {
        if let Err((code, e)) = subscriptions::leave_order(state, self, false).await {
            eprintln!("rithmic-gateway: teardown {code}: {e}");
        }
    }
}
