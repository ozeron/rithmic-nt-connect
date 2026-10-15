//! Attachment queues fed synchronously by the host publisher, without tasks.
//!
//! Each session/stream has its own bound and exact gap accounting. Native reads
//! move owned records; multicast records stay shared until that boundary. The
//! gateway drains the same queues as shared/encoded records. Sequence numbers
//! merge streams for its writer; per-stream source order is preserved across
//! topics (the old independent topic forwarders only guaranteed topic FIFO).
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc::error::TryRecvError, oneshot, Notify};
use tokio::time::Instant;

use crate::fanout::{Fan, Kind};
use crate::{Error, GapDto, PlantEvent, Result, Stream};

pub(crate) fn stream_of(event: &PlantEvent) -> Stream {
    match event {
        PlantEvent::TimeBar(_) => Stream::History,
        PlantEvent::AccountPnl(_) | PlantEvent::InstrumentPnl(_) => Stream::Pnl,
        PlantEvent::OrderNotification(_) | PlantEvent::BracketUpdate(_) => Stream::Order,
        PlantEvent::Gap(gap) => gap.stream,
        _ => Stream::Ticker,
    }
}

// Keep queued slots compact even when a gateway writer stalls: the large
// PlantEvent variant lives in an owned batch or shared record, never each slot.
enum Record {
    Batch(VecDeque<PlantEvent>),
    Shared(Fan),
}
impl Record {
    fn len(&self) -> usize {
        match self {
            Self::Batch(events) => events.len(),
            Self::Shared(_) => 1,
        }
    }
}
#[allow(clippy::large_enum_variant)]
enum Taken {
    Owned(PlantEvent),
    Shared(Fan),
}
impl Taken {
    fn event(&self) -> Option<&PlantEvent> {
        match self {
            Self::Owned(event) => Some(event),
            Self::Shared(fan) => fan.event(),
        }
    }
    fn into_event(self) -> PlantEvent {
        match self {
            Self::Owned(event) => event,
            Self::Shared(fan) => fan.into_event().expect("native record"),
        }
    }
    fn into_fan(self) -> Fan {
        match self {
            Self::Owned(event) => Fan::native(Kind::Any, event),
            Self::Shared(fan) => fan,
        }
    }
}

#[derive(Default)]
struct Queue {
    records: VecDeque<(u64, Record)>,
    len: usize,
    skipped: u64,
    gap_sequence: u64,
}
impl Queue {
    fn head(&self) -> Option<u64> {
        if self.skipped > 0 {
            Some(self.gap_sequence)
        } else {
            self.records.front().map(|r| r.0)
        }
    }
    fn pop(&mut self, stream: Stream) -> Option<Taken> {
        if self.skipped > 0 {
            return Some(Taken::Owned(PlantEvent::Gap(GapDto {
                stream,
                skipped: std::mem::take(&mut self.skipped),
            })));
        }
        self.pop_record().map(|r| r.1)
    }
    fn pop_record(&mut self) -> Option<(u64, Taken)> {
        let (sequence, record) = self.records.front_mut()?;
        self.len -= 1;
        let first = *sequence;
        *sequence += 1;
        match record {
            Record::Batch(events) => {
                let event = events.pop_front().expect("batch record");
                if events.is_empty() {
                    self.records.pop_front();
                }
                Some((first, Taken::Owned(event)))
            }
            Record::Shared(_) => {
                let (_, Record::Shared(fan)) = self.records.pop_front().expect("queued record")
                else {
                    unreachable!()
                };
                Some((first, Taken::Shared(fan)))
            }
        }
    }
}
#[derive(Default)]
struct State {
    queues: [Queue; 4],
    barriers: VecDeque<(u64, Fan)>,
    sequence: u64,
    native_started: bool,
    closed: bool,
}

pub(crate) struct Delivery {
    capacity: usize,
    state: Mutex<State>,
    changed: [Notify; 4],
    any: Notify,
    transport: AtomicBool,
}
impl Delivery {
    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity: capacity.max(1),
            state: Mutex::new(State::default()),
            changed: std::array::from_fn(|_| Notify::new()),
            any: Notify::new(),
            transport: AtomicBool::new(false),
        })
    }
    pub(crate) fn is_transport(&self) -> bool {
        self.transport.load(Ordering::Relaxed)
    }
    pub(crate) fn event(&self, event: PlantEvent) {
        self.batch(vec![event]);
    }
    pub(crate) fn batch(&self, events: Vec<PlantEvent>) {
        let stream = stream_of(&events[0]);
        self.push(stream, Record::Batch(events.into()));
    }
    pub(crate) fn shared(&self, stream: Stream, fan: Fan) {
        self.push(stream, Record::Shared(fan));
    }
    fn push(&self, stream: Stream, record: Record) {
        let mut state = self.state.lock().expect("attachment queue");
        if state.closed {
            return;
        }
        let sequence = state.sequence + 1;
        state.sequence += record.len() as u64;
        let queue = &mut state.queues[stream as usize];
        queue.len += record.len();
        queue.records.push_back((sequence, record));
        while queue.len > self.capacity {
            let (sequence, lost) = queue.pop_record().expect("full queue");
            if queue.skipped == 0 {
                queue.gap_sequence = sequence;
            }
            queue.skipped += match lost.event() {
                Some(PlantEvent::Gap(gap)) => gap.skipped,
                _ => 1,
            };
        }
        drop(state);
        if self.is_transport() {
            self.any.notify_one();
        } else {
            self.changed[stream as usize].notify_one();
        }
    }
    pub(crate) fn close(&self) {
        self.state.lock().expect("attachment queue").closed = true;
        for changed in &self.changed {
            changed.notify_waiters();
        }
        self.any.notify_waiters();
    }
    /// Called only after the pump has quiesced all publishers. Native records
    /// are already queued. A writer must finish preceding records before it
    /// acknowledges this barrier and the host broadcasts the lifecycle signal.
    pub(crate) fn flush(&self) -> Option<oneshot::Receiver<()>> {
        let mut state = self.state.lock().expect("attachment queue");
        if !self.is_transport() {
            // Lifecycle delivery has committed this attachment to native
            // consumption, just as the old native flush forwarder did.
            state.native_started = true;
            return None;
        }
        if state.closed {
            return None;
        }
        let (done, ack) = oneshot::channel();
        state.sequence += 1;
        let sequence = state.sequence;
        state.barriers.push_back((sequence, Fan::flush(done)));
        drop(state);
        self.any.notify_one();
        Some(ack)
    }
    pub(crate) fn take_events(self: &Arc<Self>) -> Option<EventReceiver> {
        let state = self.state.lock().expect("attachment queue");
        if state.native_started || self.transport.swap(true, Ordering::SeqCst) {
            return None;
        }
        Some(EventReceiver(self.clone()))
    }
    pub(crate) async fn next_events(
        &self,
        stream: Stream,
        max: usize,
        timeout: Duration,
    ) -> Result<Vec<PlantEvent>> {
        let deadline = Instant::now() + timeout;
        let max = max.max(1);
        loop {
            // Register before checking, including concurrent cloned readers.
            // Cancellation never removes a queued record.
            let changed = self.changed[stream as usize].notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.state.lock().expect("attachment queue");
                state.native_started = true;
                let closed = state.closed;
                let queue = &mut state.queues[stream as usize];
                // Move a complete venue batch without reallocating or copying
                // its records. Partial takes still obey max and retain FIFO.
                if queue.skipped == 0
                    && matches!(queue.records.front(), Some((_, Record::Batch(events))) if events.len() <= max)
                {
                    let (_, Record::Batch(events)) = queue.records.pop_front().expect("batch")
                    else {
                        unreachable!()
                    };
                    queue.len -= events.len();
                    if queue.len > 0 {
                        self.changed[stream as usize].notify_one();
                    }
                    return Ok(events.into());
                }
                let count = max.min(queue.len + usize::from(queue.skipped > 0));
                if count > 0 {
                    let mut batch = Vec::with_capacity(count);
                    while batch.len() < count {
                        batch.push(queue.pop(stream).expect("queued record").into_event());
                    }
                    if queue.len > 0 {
                        self.changed[stream as usize].notify_one();
                    }
                    return Ok(batch);
                }
                if closed {
                    return Err(Error::ChannelClosed {
                        plant: stream.plant(),
                    });
                }
            }
            if Instant::now() >= deadline
                || tokio::time::timeout_at(deadline, changed).await.is_err()
            {
                return Ok(Vec::new());
            }
        }
    }
    pub(crate) async fn poll(
        &self,
        stream: Stream,
        timeout: Duration,
    ) -> Result<Option<PlantEvent>> {
        match self.next_events(stream, 1, timeout).await?.pop() {
            Some(PlantEvent::Gap(gap)) => Err(Error::ChannelLagged {
                plant: stream.plant(),
                skipped: gap.skipped,
            }),
            event => Ok(event),
        }
    }
}

/// The gateway's exclusive consumer of an attachment's shared event queues.
/// Like an mpsc receiver, `recv` is cancellation safe and drains before closing.
pub struct EventReceiver(Arc<Delivery>);
impl EventReceiver {
    pub fn try_recv(&self) -> std::result::Result<Fan, TryRecvError> {
        let mut state = self.0.state.lock().expect("attachment queue");
        let head = Stream::ALL
            .into_iter()
            .filter_map(|stream| {
                state.queues[stream as usize]
                    .head()
                    .map(|seq| (seq, stream))
            })
            .min_by_key(|r| r.0);
        if let Some((sequence, _)) = state.barriers.front() {
            if head.is_none_or(|head| *sequence < head.0) {
                return Ok(state.barriers.pop_front().expect("barrier").1);
            }
        }
        if let Some((_, stream)) = head {
            return Ok(state.queues[stream as usize]
                .pop(stream)
                .expect("queued record")
                .into_fan());
        }
        Err(if state.closed {
            TryRecvError::Disconnected
        } else {
            TryRecvError::Empty
        })
    }
    pub async fn recv(&mut self) -> Option<Fan> {
        loop {
            let changed = self.0.any.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.try_recv() {
                Ok(fan) => return Some(fan),
                Err(TryRecvError::Disconnected) => return None,
                Err(TryRecvError::Empty) => changed.await,
            }
        }
    }
}
