//! Stable, lock-free plant event streams.
//!
//! Each plant stream (ticker, history, PnL, order) has one bounded lane that
//! outlives the plant connections feeding it. A forwarder task per plant
//! connection moves events from the plant's broadcast receiver into the lane;
//! consumers wait on the lane with no session lock held and wake the moment an
//! event arrives, so delivery never depends on a poll interval.
//!
//! Conditions a consumer must act on travel in-band, in order:
//!
//! * a broadcast `Lagged(n)` becomes a gap (`PlantEvent::Gap`), never a log line;
//! * a venue connection error and a closed channel surface as errors once the
//!   queued events ahead of them are drained.
//!
//! A lane carries a generation. Re-attaching after a reconnect bumps it, so a
//! stale forwarder or stale queued items can never leak into the new connection.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::{mpsc, Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::dto::{GapDto, PlantEvent};
use crate::error::{Error, Result};

/// One of the four plant event streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stream {
    Ticker,
    History,
    Pnl,
    Order,
}

impl Stream {
    pub const ALL: [Stream; 4] = [Stream::Ticker, Stream::History, Stream::Pnl, Stream::Order];

    /// Plant name used in errors and on the wire.
    pub fn plant(self) -> &'static str {
        match self {
            Self::Ticker => "ticker",
            Self::History => "history",
            Self::Pnl => "pnl",
            Self::Order => "order",
        }
    }

    pub fn from_plant(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.plant() == name)
    }

    fn index(self) -> usize {
        match self {
            Self::Ticker => 0,
            Self::History => 1,
            Self::Pnl => 2,
            Self::Order => 3,
        }
    }

    /// What a poll reports while the plant is not attached: the PnL plant is
    /// optional, so "not attached" reads as "no events"; the others are errors.
    fn down_is_error(self) -> bool {
        !matches!(self, Self::Pnl)
    }
}

/// Queued items per lane. A full lane applies backpressure to the forwarder,
/// whose broadcast receiver then lags and reports a gap.
pub const STREAM_CAPACITY: usize = 65_536;

/// `PlantEvent` is already the large variant by design (see its docs); boxing it
/// would add an allocation per event on the hot path.
#[allow(clippy::large_enum_variant)]
enum Msg {
    Event(PlantEvent),
    Gap(u64),
}

/// A `PlantEvent::Gap` queued as an event is stored as a gap, so a scripted gap
/// and a forwarded `Lagged` read the same way to every consumer.
fn msg_of(event: PlantEvent) -> Msg {
    match event {
        PlantEvent::Gap(g) => Msg::Gap(g.skipped),
        other => Msg::Event(other),
    }
}

struct Tagged {
    generation: u64,
    msg: Msg,
}

/// Consumer-side state, guarded by one lock that only the lane's consumer takes.
struct Consumer {
    rx: mpsc::Receiver<Tagged>,
    /// Messages already taken from the channel that did not fit the last batch.
    carry: VecDeque<Msg>,
    /// Attachment generation the carried messages belong to.
    carry_gen: u64,
}

struct Lane {
    tx: mpsc::Sender<Tagged>,
    /// The single consumer's receiving end and its carry buffer.
    consumer: Mutex<Consumer>,
    generation: AtomicU64,
    up: AtomicBool,
    closed: AtomicBool,
    fail: StdMutex<Option<Error>>,
    /// Items discarded because they belonged to a replaced attachment. They are
    /// reported as a gap rather than vanishing.
    stale: AtomicU64,
    /// Wakes a waiting consumer when `up`, `closed` or `fail` change.
    changed: Notify,
}

impl Lane {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel(STREAM_CAPACITY);
        Self {
            tx,
            consumer: Mutex::new(Consumer {
                rx,
                carry: VecDeque::new(),
                carry_gen: 0,
            }),
            generation: AtomicU64::new(0),
            up: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            fail: StdMutex::new(None),
            stale: AtomicU64::new(0),
            changed: Notify::new(),
        }
    }
}

/// The four lanes. Cheap to share (`Arc`); consumers never touch the session.
pub struct StreamHub {
    lanes: [Lane; 4],
}

/// What one wait on a lane produced.
enum Taken {
    Msgs(Vec<Msg>),
    Timeout,
    Down,
    Closed,
    Fail(Error),
}

impl StreamHub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            lanes: [Lane::new(), Lane::new(), Lane::new(), Lane::new()],
        })
    }

    fn lane(&self, stream: Stream) -> &Lane {
        &self.lanes[stream.index()]
    }

    /// Mark a plant attached and return the sender its forwarder feeds. Items
    /// queued for an earlier attachment are discarded by the generation bump.
    pub fn attach(self: &Arc<Self>, stream: Stream) -> StreamTx {
        let lane = self.lane(stream);
        let generation = lane.generation.fetch_add(1, Ordering::SeqCst) + 1;
        lane.closed.store(false, Ordering::SeqCst);
        *lane.fail.lock().expect("stream fail") = None;
        lane.up.store(true, Ordering::SeqCst);
        lane.changed.notify_waiters();
        StreamTx {
            hub: Arc::clone(self),
            stream,
            generation,
        }
    }

    /// Mark the plant detached, unless a newer attachment already replaced it.
    pub fn detach(&self, stream: Stream, generation: u64) {
        let lane = self.lane(stream);
        if lane.generation.load(Ordering::SeqCst) != generation {
            return;
        }
        lane.up.store(false, Ordering::SeqCst);
        lane.closed.store(false, Ordering::SeqCst);
        // Stale items are skipped by the generation check on the next read.
        lane.generation.fetch_add(1, Ordering::SeqCst);
        lane.changed.notify_waiters();
    }

    /// Whether a plant is attached to the lane.
    pub fn is_up(&self, stream: Stream) -> bool {
        self.lane(stream).up.load(Ordering::SeqCst)
    }

    /// Simulate or record a closed channel for the current attachment.
    pub fn close(&self, stream: Stream) {
        let lane = self.lane(stream);
        lane.closed.store(true, Ordering::SeqCst);
        lane.changed.notify_waiters();
    }

    /// Wait up to `timeout` for the first item, then take up to `max - 1` more
    /// that are already queued. Holds no lock other than the lane's own
    /// consumer lock, which only the stream's single consumer takes.
    async fn take(&self, stream: Stream, max: usize, timeout: Duration) -> Taken {
        let lane = self.lane(stream);
        let limit = max.max(1);
        let deadline = Instant::now() + timeout;
        let mut cons = lane.consumer.lock().await;
        loop {
            let notified = lane.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let up = lane.up.load(Ordering::SeqCst);
            let current = lane.generation.load(Ordering::SeqCst);
            // While detached, the items still owed are those of the attachment
            // that just detached (a detach bumps the generation once); a later
            // attach bumps it again and turns them into an exact gap.
            let owed = if up { current } else { current.wrapping_sub(1) };
            // Carried messages from a replaced attachment were lost.
            if !cons.carry.is_empty() && cons.carry_gen != owed {
                lane.stale
                    .fetch_add(cons.carry.len() as u64, Ordering::SeqCst);
                cons.carry.clear();
            }
            // Queued events come first (carry, then the channel), then a recorded
            // failure, close or detach.
            let batch = drain(lane, &mut cons, limit, owed);
            if let Some(out) = finish(lane, &mut cons, batch, limit, owed) {
                return Taken::Msgs(out);
            }
            if !up {
                return Taken::Down;
            }
            if let Some(text) = lane.fail.lock().expect("stream fail").take() {
                // The forwarder queues an event before it records a failure, so
                // an event can land between the drain above and this read. It is
                // delivered first; the failure stays recorded for the next take.
                let late = drain(lane, &mut cons, limit, current);
                if let Some(out) = finish(lane, &mut cons, late, limit, current) {
                    *lane.fail.lock().expect("stream fail") = Some(text);
                    return Taken::Msgs(out);
                }
                return Taken::Fail(text);
            }
            if lane.closed.load(Ordering::SeqCst) {
                let late = drain(lane, &mut cons, limit, current);
                if let Some(out) = finish(lane, &mut cons, late, limit, current) {
                    return Taken::Msgs(out);
                }
                return Taken::Closed;
            }
            tokio::select! {
                biased;
                first = cons.rx.recv() => match first {
                    Some(t) if t.generation == lane.generation.load(Ordering::SeqCst) => {
                        let generation = t.generation;
                        let mut batch = vec![t.msg];
                        while batch.len() < limit {
                            match cons.rx.try_recv() {
                                Ok(t) if t.generation == generation => batch.push(t.msg),
                                Ok(_) => {
                                    lane.stale.fetch_add(1, Ordering::SeqCst);
                                }
                                Err(_) => break,
                            }
                        }
                        if let Some(out) = finish(lane, &mut cons, batch, limit, generation) {
                            return Taken::Msgs(out);
                        }
                    }
                    Some(_) => {
                        lane.stale.fetch_add(1, Ordering::SeqCst);
                    }
                    None => return Taken::Closed,
                },
                _ = &mut notified => {}
                _ = tokio::time::sleep_until(deadline) => return Taken::Timeout,
            }
        }
    }

    /// Wait until the lane is attached; `false` if the deadline passes first.
    async fn wait_up(&self, stream: Stream, deadline: Instant) -> bool {
        let lane = self.lane(stream);
        loop {
            let notified = lane.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if lane.up.load(Ordering::SeqCst) {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return lane.up.load(Ordering::SeqCst);
            }
        }
    }

    /// Compatibility poll: one event, or `None` after `timeout`. A gap reads as
    /// [`Error::ChannelLagged`] so existing resync paths keep working.
    pub async fn poll(&self, stream: Stream, timeout: Duration) -> Result<Option<PlantEvent>> {
        let plant = stream.plant();
        let deadline = Instant::now() + timeout;
        match self.take(stream, 1, timeout).await {
            Taken::Msgs(mut msgs) => match msgs.pop() {
                Some(Msg::Event(event)) => Ok(Some(event)),
                Some(Msg::Gap(skipped)) => Err(Error::ChannelLagged { plant, skipped }),
                None => Ok(None),
            },
            Taken::Timeout => Ok(None),
            Taken::Down if stream.down_is_error() => Err(Error::NotConnected { plant }),
            Taken::Down => {
                // An optional plant that is not attached: block for the rest of
                // the timeout (an attach wakes us), as a poll always has.
                if self.wait_up(stream, deadline).await {
                    return Box::pin(
                        self.poll(stream, deadline.saturating_duration_since(Instant::now())),
                    )
                    .await;
                }
                Ok(None)
            }
            Taken::Closed => Err(Error::ChannelClosed { plant }),
            Taken::Fail(error) => Err(error),
        }
    }

    /// Batch wait for the event-driven consumers (the gateway pump, FFI
    /// polling): waits for an attached plant, then up to `max` events at once.
    /// A gap is returned in order as `PlantEvent::Gap`.
    pub async fn next_events(
        &self,
        stream: Stream,
        max: usize,
        timeout: Duration,
    ) -> Result<Vec<PlantEvent>> {
        let plant = stream.plant();
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.take(stream, max, remaining).await {
                Taken::Msgs(msgs) => {
                    return Ok(msgs
                        .into_iter()
                        .map(|m| match m {
                            Msg::Event(e) => e,
                            Msg::Gap(skipped) => PlantEvent::Gap(GapDto { stream, skipped }),
                        })
                        .collect())
                }
                Taken::Timeout => return Ok(Vec::new()),
                Taken::Closed => return Err(Error::ChannelClosed { plant }),
                Taken::Fail(error) => return Err(error),
                Taken::Down => {
                    // Not attached yet: wait for an attach (or the deadline).
                    if !self.wait_up(stream, deadline).await {
                        return Ok(Vec::new());
                    }
                }
            }
        }
    }
}

/// Take up to `limit` queued messages of attachment `generation` (carry first,
/// then the channel); items of any other attachment are counted as stale.
fn drain(lane: &Lane, cons: &mut Consumer, limit: usize, generation: u64) -> Vec<Msg> {
    let mut batch = Vec::new();
    while batch.len() < limit {
        match cons.carry.pop_front() {
            Some(m) => batch.push(m),
            None => break,
        }
    }
    while batch.len() < limit {
        match cons.rx.try_recv() {
            Ok(t) if t.generation == generation => batch.push(t.msg),
            Ok(_) => {
                lane.stale.fetch_add(1, Ordering::SeqCst);
            }
            Err(_) => break,
        }
    }
    batch
}

/// Prefix a batch with a gap for any items lost to a replaced attachment, and
/// keep whatever does not fit `limit` for the next call so nothing is dropped.
fn finish(
    lane: &Lane,
    cons: &mut Consumer,
    mut batch: Vec<Msg>,
    limit: usize,
    generation: u64,
) -> Option<Vec<Msg>> {
    let stale = lane.stale.swap(0, Ordering::SeqCst);
    if stale > 0 {
        batch.insert(0, Msg::Gap(stale));
    }
    if batch.is_empty() {
        return None;
    }
    if batch.len() > limit {
        let rest = batch.split_off(limit);
        cons.carry_gen = generation;
        for m in rest.into_iter().rev() {
            cons.carry.push_front(m);
        }
    }
    Some(batch)
}

/// The feeding side of one lane attachment.
#[derive(Clone)]
pub struct StreamTx {
    hub: Arc<StreamHub>,
    stream: Stream,
    generation: u64,
}

impl StreamTx {
    fn lane(&self) -> &Lane {
        self.hub.lane(self.stream)
    }

    fn current(&self) -> bool {
        self.lane().generation.load(Ordering::SeqCst) == self.generation
    }

    /// Queue an event; waits when the lane is full (backpressure).
    pub async fn event(&self, event: PlantEvent) {
        let _ = self
            .lane()
            .tx
            .send(Tagged {
                generation: self.generation,
                msg: msg_of(event),
            })
            .await;
    }

    /// Queue a gap of `skipped` events, in order with the events around it.
    pub async fn gap(&self, skipped: u64) {
        let _ = self
            .lane()
            .tx
            .send(Tagged {
                generation: self.generation,
                msg: Msg::Gap(skipped),
            })
            .await;
    }

    /// Non-blocking queue of an event; `false` when the lane is full or the
    /// attachment was replaced. For synchronous producers (tests, fakes).
    pub fn try_event(&self, event: PlantEvent) -> bool {
        self.current()
            && self
                .lane()
                .tx
                .try_send(Tagged {
                    generation: self.generation,
                    msg: msg_of(event),
                })
                .is_ok()
    }

    /// Record a venue connection error; consumers see it after queued events.
    pub fn fail(&self, text: String) {
        if self.current() {
            *self.lane().fail.lock().expect("stream fail") = Some(Error::Rithmic(text));
            self.lane().changed.notify_waiters();
        }
    }

    /// A login-wide failure is typed at the venue boundary, rather than guessed
    /// from an error string by the host. It is ordered after queued records.
    pub fn fail_login(&self, text: String) {
        if self.current() {
            *self.lane().fail.lock().expect("stream fail") = Some(Error::Transport(text));
            self.lane().changed.notify_waiters();
        }
    }

    /// Record that the plant's channel closed.
    pub fn close(&self) {
        if self.current() {
            self.lane().closed.store(true, Ordering::SeqCst);
            self.lane().changed.notify_waiters();
        }
    }

    pub fn stream(&self) -> Stream {
        self.stream
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Owns a plant's forwarder task: dropping it stops the task and detaches the lane.
pub struct StreamGuard {
    hub: Arc<StreamHub>,
    stream: Stream,
    generation: u64,
    task: Option<JoinHandle<()>>,
}

impl StreamGuard {
    pub fn new(tx: &StreamTx, task: JoinHandle<()>) -> Self {
        Self {
            hub: Arc::clone(&tx.hub),
            stream: tx.stream,
            generation: tx.generation,
            task: Some(task),
        }
    }
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.hub.detach(self.stream, self.generation);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::LastTradeDto;

    fn tick(seq: i32) -> PlantEvent {
        PlantEvent::LastTrade(LastTradeDto {
            symbol: Some("NQ".into()),
            exchange: Some("CME".into()),
            trade_price: Some(1.0),
            trade_size: Some(seq),
            aggressor: None,
            ssboe: None,
            usecs: None,
            ts_event_ns: None,
            is_snapshot: None,
        })
    }

    fn seq(event: &PlantEvent) -> i32 {
        match event {
            PlantEvent::LastTrade(t) => t.trade_size.unwrap(),
            other => panic!("not a tick: {other:?}"),
        }
    }

    const SHORT: Duration = Duration::from_millis(50);

    #[tokio::test]
    async fn events_arrive_in_order_through_poll() {
        let hub = StreamHub::new();
        let tx = hub.attach(Stream::Ticker);
        for i in 0..5 {
            tx.event(tick(i)).await;
        }
        for i in 0..5 {
            let e = hub.poll(Stream::Ticker, SHORT).await.unwrap().unwrap();
            assert_eq!(seq(&e), i);
        }
        assert!(hub.poll(Stream::Ticker, SHORT).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_waiting_consumer_wakes_on_arrival_not_on_a_poll_interval() {
        let hub = StreamHub::new();
        let tx = hub.attach(Stream::Ticker);
        let waiter = {
            let hub = hub.clone();
            tokio::spawn(async move {
                let start = std::time::Instant::now();
                let e = hub.poll(Stream::Ticker, Duration::from_secs(5)).await;
                (e, start.elapsed())
            })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        let sent = std::time::Instant::now();
        tx.event(tick(7)).await;
        let (event, waited) = waiter.await.unwrap();
        assert_eq!(seq(&event.unwrap().unwrap()), 7);
        assert!(waited < Duration::from_millis(500), "waited {waited:?}");
        assert!(sent.elapsed() < Duration::from_millis(200));
    }

    #[tokio::test]
    async fn a_gap_is_in_order_and_reads_as_lagged_through_poll() {
        let hub = StreamHub::new();
        let tx = hub.attach(Stream::Ticker);
        tx.event(tick(1)).await;
        tx.gap(41).await;
        tx.event(tick(2)).await;
        assert_eq!(
            seq(&hub.poll(Stream::Ticker, SHORT).await.unwrap().unwrap()),
            1
        );
        match hub.poll(Stream::Ticker, SHORT).await {
            Err(Error::ChannelLagged { plant, skipped }) => {
                assert_eq!((plant, skipped), ("ticker", 41))
            }
            other => panic!("expected lag, got {other:?}"),
        }
        assert_eq!(
            seq(&hub.poll(Stream::Ticker, SHORT).await.unwrap().unwrap()),
            2
        );
    }

    #[tokio::test]
    async fn next_events_returns_a_batch_with_the_gap_in_order() {
        let hub = StreamHub::new();
        let tx = hub.attach(Stream::Ticker);
        tx.event(tick(1)).await;
        tx.gap(9).await;
        tx.event(tick(2)).await;
        let batch = hub.next_events(Stream::Ticker, 16, SHORT).await.unwrap();
        assert_eq!(batch.len(), 3);
        assert_eq!(seq(&batch[0]), 1);
        assert!(matches!(
            &batch[1],
            PlantEvent::Gap(GapDto {
                stream: Stream::Ticker,
                skipped: 9
            })
        ));
        assert_eq!(seq(&batch[2]), 2);
    }

    #[tokio::test]
    async fn next_events_honours_the_batch_limit() {
        let hub = StreamHub::new();
        let tx = hub.attach(Stream::Order);
        for i in 0..10 {
            tx.event(tick(i)).await;
        }
        assert_eq!(
            hub.next_events(Stream::Order, 4, SHORT)
                .await
                .unwrap()
                .len(),
            4
        );
        assert_eq!(
            hub.next_events(Stream::Order, 100, SHORT)
                .await
                .unwrap()
                .len(),
            6
        );
    }

    #[tokio::test]
    async fn close_is_reported_only_after_queued_events_drain() {
        let hub = StreamHub::new();
        let tx = hub.attach(Stream::History);
        tx.event(tick(1)).await;
        tx.close();
        assert_eq!(
            seq(&hub.poll(Stream::History, SHORT).await.unwrap().unwrap()),
            1
        );
        assert!(matches!(
            hub.poll(Stream::History, SHORT).await,
            Err(Error::ChannelClosed { plant: "history" })
        ));
    }

    #[tokio::test]
    async fn a_recorded_failure_surfaces_once_after_queued_events() {
        let hub = StreamHub::new();
        let tx = hub.attach(Stream::Ticker);
        tx.event(tick(1)).await;
        tx.fail("connection lost".into());
        assert_eq!(
            seq(&hub.poll(Stream::Ticker, SHORT).await.unwrap().unwrap()),
            1
        );
        assert!(matches!(
            hub.poll(Stream::Ticker, SHORT).await,
            Err(Error::Rithmic(m)) if m == "connection lost"
        ));
        assert!(hub.poll(Stream::Ticker, SHORT).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_detached_lane_reads_by_plant_semantics() {
        let hub = StreamHub::new();
        assert!(matches!(
            hub.poll(Stream::Ticker, SHORT).await,
            Err(Error::NotConnected { plant: "ticker" })
        ));
        assert!(matches!(
            hub.poll(Stream::Order, SHORT).await,
            Err(Error::NotConnected { plant: "order" })
        ));
        assert!(hub.poll(Stream::Pnl, SHORT).await.unwrap().is_none());
        // The batch API waits for an attach instead of erroring.
        let start = std::time::Instant::now();
        assert!(hub
            .next_events(Stream::Order, 8, SHORT)
            .await
            .unwrap()
            .is_empty());
        assert!(start.elapsed() >= Duration::from_millis(40));
    }

    #[tokio::test]
    async fn next_events_wakes_when_the_plant_attaches() {
        let hub = StreamHub::new();
        let waiter = {
            let hub = hub.clone();
            tokio::spawn(async move {
                hub.next_events(Stream::Order, 8, Duration::from_secs(5))
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(30)).await;
        let tx = hub.attach(Stream::Order);
        tx.event(tick(3)).await;
        let batch = waiter.await.unwrap().unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(seq(&batch[0]), 3);
    }

    #[tokio::test]
    async fn reattaching_discards_stale_items_and_stale_senders() {
        let hub = StreamHub::new();
        let old = hub.attach(Stream::Ticker);
        old.event(tick(1)).await;
        let fresh = hub.attach(Stream::Ticker);
        old.event(tick(2)).await; // a straggler from the old forwarder
        old.close(); // must not close the new attachment
        fresh.event(tick(3)).await;
        // The two stale items are reported as one exact gap, never silently, and
        // never delivered as if they belonged to the new connection.
        assert!(matches!(
            hub.poll(Stream::Ticker, SHORT).await,
            Err(Error::ChannelLagged { skipped: 2, .. })
        ));
        assert_eq!(
            seq(&hub.poll(Stream::Ticker, SHORT).await.unwrap().unwrap()),
            3
        );
        assert!(hub.poll(Stream::Ticker, SHORT).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn events_queued_when_a_plant_detaches_surface_as_an_exact_gap_on_reattach() {
        let hub = StreamHub::new();
        let old = hub.attach(Stream::Ticker);
        for i in 0..3 {
            old.event(tick(i)).await;
        }
        hub.detach(Stream::Ticker, old.generation());
        let fresh = hub.attach(Stream::Ticker);
        fresh.event(tick(10)).await;
        let batch = hub.next_events(Stream::Ticker, 16, SHORT).await.unwrap();
        assert!(matches!(
            &batch[0],
            PlantEvent::Gap(GapDto {
                stream: Stream::Ticker,
                skipped: 3
            })
        ));
        assert_eq!(seq(&batch[1]), 10);
    }

    #[tokio::test]
    async fn events_queued_before_a_detach_are_delivered_before_down() {
        let hub = StreamHub::new();
        let tx = hub.attach(Stream::Order);
        tx.event(tick(1)).await;
        tx.event(tick(2)).await;
        hub.detach(Stream::Order, tx.generation());
        // The fills that arrived before the plant went away are not lost...
        let batch = hub.next_events(Stream::Order, 8, SHORT).await.unwrap();
        assert_eq!(batch.iter().map(seq).collect::<Vec<_>>(), vec![1, 2]);
        // ...and only then does the lane read as down.
        assert!(matches!(
            hub.poll(Stream::Order, SHORT).await,
            Err(Error::NotConnected { plant: "order" })
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failure_never_overtakes_an_event_queued_before_it() {
        // The forwarder queues an event and then records a failure; a consumer
        // racing it must see the event first, every time.
        for _ in 0..2_000 {
            let hub = StreamHub::new();
            let tx = hub.attach(Stream::Order);
            let consumer = {
                let hub = hub.clone();
                tokio::spawn(async move {
                    let mut seen = Vec::new();
                    loop {
                        match hub.next_events(Stream::Order, 8, SHORT).await {
                            Ok(batch) => seen.extend(batch.iter().map(seq)),
                            Err(_) => return seen,
                        }
                    }
                })
            };
            tokio::task::yield_now().await;
            tx.event(tick(7)).await;
            tx.fail("connection lost".into());
            assert_eq!(consumer.await.unwrap(), vec![7], "event before failure");
        }
    }

    #[tokio::test]
    async fn detach_is_ignored_for_a_replaced_attachment() {
        let hub = StreamHub::new();
        let old = hub.attach(Stream::Ticker);
        let _fresh = hub.attach(Stream::Ticker);
        hub.detach(Stream::Ticker, old.generation());
        assert!(hub.is_up(Stream::Ticker));
    }

    #[tokio::test]
    async fn a_full_lane_applies_backpressure_instead_of_dropping() {
        let hub = StreamHub::new();
        let tx = hub.attach(Stream::Ticker);
        let producer = tokio::spawn(async move {
            for i in 0..(STREAM_CAPACITY as i32 + 100) {
                tx.event(tick(i)).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !producer.is_finished(),
            "producer should be blocked on a full lane"
        );
        let mut got = 0usize;
        while got < STREAM_CAPACITY + 100 {
            got += hub
                .next_events(Stream::Ticker, 1024, Duration::from_secs(2))
                .await
                .unwrap()
                .len();
        }
        producer.await.unwrap();
    }
}
