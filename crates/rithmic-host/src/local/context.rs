use super::*;
use crate::fanout::*;
use crate::intents::TimeBarIntent;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU8;
use subscriptions::TopicIntent;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

/// Per-connection fan-out bookkeeping: which topics this client forwards,
/// typed venue joins this client asked for, and forwarder tasks.
pub(super) struct SessionCtx {
    id: SessionId,
    pub(super) subscribed: HashSet<SubKey>,
    pub(super) ticker: HashSet<SubKey>,
    pub(super) book: HashSet<SubKey>,
    pub(super) time_bars: HashSet<TimeBarIntent>,
    pub(super) pnl: bool,
    pub(super) order: bool,
    pub(super) brackets: bool,
    pub(super) forwarders: HashMap<SubKey, JoinHandle<()>>,
    /// Per forwarded topic: which frame kinds this client asked for (bit mask of
    /// [`Kind::bit`]). A forwarder drops frames of other kinds.
    pub(super) kinds: HashMap<SubKey, Arc<AtomicU8>>,
    pub(super) out_tx: mpsc::Sender<Fan>,
}

impl SessionCtx {
    pub(super) fn new(out_tx: mpsc::Sender<Fan>) -> Self {
        Self::with_id(out_tx, SessionId::new())
    }

    pub(super) fn with_id(out_tx: mpsc::Sender<Fan>, id: SessionId) -> Self {
        Self {
            id,
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
        if self.brackets && *key == crate::fanout::order_key() {
            mask |= Kind::Brackets.bit();
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
    pub(super) async fn ensure_forwarder(&mut self, hub: &FanoutHub, key: &SubKey) {
        if self.subscribed.contains(key) {
            return;
        }
        if let Some(rx) = hub.subscribe_receiver(key).await {
            let kinds = Arc::clone(
                self.kinds
                    .entry(key.clone())
                    .or_insert_with(|| Arc::new(AtomicU8::new(0))),
            );
            let handle = spawn_forwarder(
                self.id,
                rx,
                self.out_tx.clone(),
                stream_of_topic(key),
                kinds,
            );
            self.forwarders.insert(key.clone(), handle);
            self.subscribed.insert(key.clone());
        }
    }

    pub(super) fn drop_forwarder(&mut self, key: &SubKey) {
        if let Some(handle) = self.forwarders.remove(key) {
            handle.abort();
        }
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

/// Which plant stream a topic's events come from, for a gap frame.
fn stream_of_topic(key: &SubKey) -> crate::Stream {
    match key.symbol.as_str() {
        "__pnl__" => crate::Stream::Pnl,
        "__order__" => crate::Stream::Order,
        _ => crate::Stream::Ticker,
    }
}

/// The streams a forwarder overflow is reported on. A symbol topic carries
/// ticks, book and live bars, but bars are consumed from the history channel: a
/// client that asked for bars is told on that channel too, so its bar consumer
/// resyncs instead of continuing with a hole.
fn overflow_gap_streams(stream: crate::Stream, kinds: u8) -> Vec<crate::Stream> {
    let mut streams = vec![stream];
    if stream == crate::Stream::Ticker && kinds & Kind::Bars.bit() != 0 {
        streams.push(crate::Stream::History);
    }
    streams
}

/// Forward one topic to one client. A receiver that falls behind the topic's
/// ring is not disconnected: it is told exactly how many frames it missed (a
/// `Gap` in order with the rest) and keeps going. A client that stops reading
/// altogether is cut by the writer's timeout.
fn spawn_forwarder(
    id: SessionId,
    rx: broadcast::Receiver<Fan>,
    out_tx: mpsc::Sender<Fan>,
    stream: crate::Stream,
    kinds: Arc<AtomicU8>,
) -> JoinHandle<()> {
    let mut queue = crate::fanout::ClientQueue::from_receiver(id, rx);
    tokio::spawn(async move {
        loop {
            match queue.recv().await {
                Ok(fan) => {
                    // A frame of a kind this client never asked for (a book
                    // update for a ticker-only client) is dropped here, before
                    // it costs a socket write or a decode on the far side.
                    let wanted = fan.kind() == Kind::Any
                        || kinds.load(Ordering::Relaxed) & fan.kind().bit() != 0;
                    if wanted && out_tx.send(fan).await.is_err() {
                        return;
                    }
                }
                Err(crate::fanout::ClientQueueError::Overflow { skipped }) => {
                    for gap_stream in overflow_gap_streams(stream, kinds.load(Ordering::Relaxed)) {
                        let gap = Fan::native(
                            Kind::Any,
                            PlantEvent::Gap(crate::GapDto {
                                stream: gap_stream,
                                skipped,
                            }),
                        );
                        if out_tx.send(gap).await.is_err() {
                            return;
                        }
                    }
                }
                Err(crate::fanout::ClientQueueError::Closed) => return,
            }
        }
    })
}

#[cfg(test)]
mod overflow_gap_tests {
    use super::*;
    use crate::Stream;

    #[test]
    fn a_bar_subscriber_hears_about_the_overflow_on_its_bar_channel() {
        let kinds = Kind::Ticker.bit() | Kind::Bars.bit();
        assert_eq!(
            overflow_gap_streams(Stream::Ticker, kinds),
            vec![Stream::Ticker, Stream::History]
        );
    }

    #[test]
    fn a_ticker_only_subscriber_gets_one_gap() {
        assert_eq!(
            overflow_gap_streams(Stream::Ticker, Kind::Ticker.bit()),
            vec![Stream::Ticker]
        );
    }

    #[test]
    fn account_topics_report_on_their_own_stream() {
        assert_eq!(
            overflow_gap_streams(Stream::Order, 0xff),
            vec![Stream::Order]
        );
        assert_eq!(overflow_gap_streams(Stream::Pnl, 0xff), vec![Stream::Pnl]);
    }
}
