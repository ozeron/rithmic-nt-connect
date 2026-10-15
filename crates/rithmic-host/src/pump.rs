//! Host-owned plant recovery and intent restore, shared by both attachments.
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::fanout::{publish_plant_event, FanoutHub};
use crate::host::{HostState, VenueHandle};
use crate::intents::{MdIntent, ReconnectController, RestorePlan};
use crate::local::subscriptions::TopicIntent;
use crate::{Error, SessionEvent, Stream, Venue};

const BATCH: usize = 1024;
const IDLE_WAIT: Duration = Duration::from_secs(1);
const RECOVERY_ATTEMPTS: u32 = 3;
const RESTORE_ATTEMPTS: u32 = 3;

/// Production pauses; tests can use paused tokio time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    pub error_backoff: Duration,
    pub connect_retry: Duration,
    pub restore_backoff: Duration,
}
impl Default for Timing {
    fn default() -> Self {
        Self {
            error_backoff: Duration::from_millis(250),
            connect_retry: Duration::from_secs(5),
            restore_backoff: Duration::from_secs(2),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Recovery {
    Plant(Stream),
    Login,
}
impl Recovery {
    fn carries(self, stream: Stream) -> bool {
        matches!(self, Self::Login) || matches!(self, Self::Plant(s) if s == stream)
    }
    fn plan(self, mut plan: RestorePlan) -> RestorePlan {
        if !self.carries(Stream::Ticker) {
            plan.ticker.clear();
            plan.book.clear();
        }
        if !self.carries(Stream::History) {
            plan.time_bars.clear();
        }
        plan.pnl &= self.carries(Stream::Pnl);
        plan.order &= self.carries(Stream::Order);
        plan.brackets &= self.carries(Stream::Order);
        plan
    }
    async fn rebuild<V: Venue + ?Sized>(self, venue: &V) -> crate::Result<()> {
        match self {
            Self::Plant(Stream::Ticker) => venue.reset_ticker().await,
            Self::Plant(Stream::History) => venue.reset_history().await,
            // PnL reconnects its subscription on the existing handle. Account
            // restore failure escalates to a login rebuild below.
            Self::Plant(Stream::Pnl) => Ok(()),
            Self::Plant(Stream::Order) => {
                venue.disconnect_order_plant().await?;
                venue.ensure_order_plant().await
            }
            Self::Login => {
                let _ = venue.disconnect().await;
                venue.connect().await
            }
        }
    }
}

struct Issue {
    recovery: Recovery,
    cause: String,
}

async fn serve_stream<V: Venue + ?Sized + 'static>(
    venue: Arc<V>,
    hub: Arc<FanoutHub>,
    stream: Stream,
    timing: Timing,
    mut stop: watch::Receiver<bool>,
) -> Option<Issue> {
    loop {
        let batch = tokio::select! {
            biased;
            _ = stop.changed() => return None,
            batch = venue.next_events(stream, BATCH, IDLE_WAIT) => batch,
        };
        match batch {
            Ok(events) => {
                // Finish a taken batch even when another plant fails: aborting
                // here would silently lose events already drained from the venue.
                for event in events {
                    publish_plant_event(&hub, event).await;
                }
            }
            Err(e) => {
                let recovery = match &e {
                    Error::Transport(_) => Recovery::Login,
                    Error::Rithmic(_) | Error::ChannelClosed { .. } => Recovery::Plant(stream),
                    _ => {
                        tokio::select! {
                            _ = stop.changed() => return None,
                            _ = tokio::time::sleep(timing.error_backoff) => {}
                        }
                        continue;
                    }
                };
                return Some(Issue {
                    recovery,
                    cause: e.to_string(),
                });
            }
        }
    }
}

fn interrupt<V: Venue + ?Sized>(state: &HostState<V>) {
    state.recovering.store(true, Ordering::SeqCst);
    state.ready.store(false, Ordering::SeqCst);
    state.interruption.send_modify(|n| *n += 1);
}

async fn serve_until_issue<V: Venue + ?Sized + 'static>(
    venue: &VenueHandle<V>,
    state: &HostState<V>,
) -> Issue {
    if state.force_reconnect.swap(false, Ordering::SeqCst) {
        interrupt(state);
        return Issue {
            recovery: Recovery::Login,
            cause: "host requested login recovery".into(),
        };
    }
    let plants = venue.checkout().await;
    let (stop, stopped) = watch::channel(false);
    let mut streams = JoinSet::new();
    for stream in Stream::ALL {
        streams.spawn(serve_stream(
            plants.clone(),
            state.hub.clone(),
            stream,
            state.timing,
            stopped.clone(),
        ));
    }
    let issue = loop {
        tokio::select! {
            result = streams.join_next() => match result {
                Some(Ok(Some(issue))) => break issue,
                Some(Err(_)) | None => break Issue { recovery: Recovery::Login, cause: "plant pump stopped unexpectedly".into() },
                Some(Ok(None)) => {}
            },
            _ = state.reconnect_wake.notified() => {
                if state.force_reconnect.swap(false, Ordering::SeqCst) {
                    break Issue { recovery: Recovery::Login, cause: "host requested login recovery".into() };
                }
            }
        }
    };
    interrupt(state);
    let _ = stop.send(true);
    // Quiesce, rather than aborting publishers mid-batch. A second concurrent
    // plant failure widens this recovery to the login, never loses its signal.
    let mut issue = issue;
    while let Some(result) = streams.join_next().await {
        if matches!(result, Ok(Some(_)) | Err(_)) {
            issue.recovery = Recovery::Login;
        }
    }
    issue
}

pub async fn run_event_pump<V: Venue + ?Sized + 'static>(state: Arc<HostState<V>>) {
    let venue = state.session.as_ref().expect("pump requires a venue");
    while !state.ready.load(Ordering::SeqCst) && !state.force_reconnect.load(Ordering::SeqCst) {
        let wake = state.reconnect_wake.notified();
        if state.ready.load(Ordering::SeqCst) || state.force_reconnect.load(Ordering::SeqCst) {
            break;
        }
        wake.await;
    }
    loop {
        let issue = serve_until_issue(venue, &state).await;
        state
            .notify_sessions(SessionEvent::Interrupted {
                generation: state.transport_generation.load(Ordering::SeqCst),
                cause: Some(issue.cause.clone()),
            })
            .await;
        if !recover(venue, &state, issue).await {
            return;
        }
    }
}

async fn recover<V: Venue + ?Sized + 'static>(
    venue: &VenueHandle<V>,
    state: &HostState<V>,
    issue: Issue,
) -> bool {
    let mut scope = issue.recovery;
    let mut cause = issue.cause;
    for attempt in 1..=RECOVERY_ATTEMPTS {
        let result = {
            let guard = venue.lock_exclusive().await;
            scope.rebuild(&*guard).await
        };
        match result {
            Err(e) => cause = e.to_string(),
            Ok(()) => {
                let mut pending = scope.plan(state.reconnect.restore_plan().await);
                for pass in 1..=RESTORE_ATTEMPTS {
                    let guard = venue.lock_exclusive().await;
                    let (_, failed) = restore_plan_pass(&*guard, &state.reconnect, pending).await;
                    drop(guard);
                    pending = failed;
                    if !has_account(&pending) && md_intents(&pending).is_empty() {
                        break;
                    }
                    if pass < RESTORE_ATTEMPTS {
                        tokio::time::sleep(state.timing.restore_backoff).await;
                    }
                }
                if !has_account(&pending) {
                    for intent in md_intents(&pending) {
                        state.reconnect.mark_degraded(intent).await;
                    }
                    // A failed re-seat requested during this recovery must not
                    // be silently discarded by a plant-only restore.
                    if state.force_reconnect.swap(false, Ordering::SeqCst) {
                        scope = Recovery::Login;
                        cause = "host requested login recovery during restore".into();
                    } else {
                        state.recovering.store(false, Ordering::SeqCst);
                        state.ready.store(true, Ordering::SeqCst);
                        let generation =
                            state.transport_generation.fetch_add(1, Ordering::SeqCst) + 1;
                        state.transport_epoch.send_replace(generation);
                        state
                            .notify_sessions(SessionEvent::Resumed { generation })
                            .await;
                        return true;
                    }
                } else {
                    scope = Recovery::Login;
                    cause = "account intent restore failed; escalating to full reconnect".into();
                }
            }
        }
        if attempt < RECOVERY_ATTEMPTS {
            tokio::time::sleep(state.timing.connect_retry).await;
        }
    }
    state.failed.store(true, Ordering::SeqCst);
    state.recovering.store(false, Ordering::SeqCst);
    state
        .notify_sessions(SessionEvent::RecoveryFailed {
            generation: state.transport_generation.load(Ordering::SeqCst),
            attempts: RECOVERY_ATTEMPTS,
            cause: Some(cause),
        })
        .await;
    // Also wake connect waiters; the generation did not advance.
    state.transport_epoch.send_modify(|_| {});
    false
}

fn has_account(plan: &RestorePlan) -> bool {
    plan.pnl || plan.order || plan.brackets
}
fn md_intents(plan: &RestorePlan) -> Vec<MdIntent> {
    plan.ticker
        .iter()
        .cloned()
        .map(MdIntent::Ticker)
        .chain(plan.book.iter().cloned().map(MdIntent::Book))
        .chain(plan.time_bars.iter().cloned().map(MdIntent::TimeBars))
        .collect()
}

#[derive(Debug, Default)]
pub struct RestorePass {
    pub restored: usize,
    pub attempted: usize,
    pub failed_md: Vec<MdIntent>,
    pub account_failed: bool,
}

/// A retry plan contains only failed joins, including individual account joins.
async fn restore_plan_pass<V: Venue + ?Sized>(
    venue: &V,
    reconnect: &ReconnectController,
    mut plan: RestorePlan,
) -> (RestorePass, RestorePlan) {
    // Detach remains possible during recovery. Retry only failed joins that
    // still have holders; teardown must not be undone by a saved retry plan.
    let held = reconnect.restore_plan().await;
    plan.ticker.retain(|key| held.ticker.contains(key));
    plan.book.retain(|key| held.book.contains(key));
    plan.time_bars.retain(|key| held.time_bars.contains(key));
    plan.pnl &= held.pnl;
    plan.order &= held.order;
    plan.brackets &= held.brackets;
    let mut pass = RestorePass::default();
    let mut failed = RestorePlan::default();
    for intent in md_intents(&plan) {
        pass.attempted += 1;
        let topic = TopicIntent::from_md(&intent);
        match topic.venue_join(venue).await {
            Ok(()) => {
                pass.restored += 1;
                reconnect.clear_degraded(&intent).await;
            }
            Err(e) => {
                eprintln!("{}", topic.resubscribe_fail_log(e));
                match &intent {
                    MdIntent::Ticker(key) => failed.ticker.push(key.clone()),
                    MdIntent::Book(key) => failed.book.push(key.clone()),
                    MdIntent::TimeBars(key) => failed.time_bars.push(key.clone()),
                }
                pass.failed_md.push(intent);
            }
        }
    }
    if plan.pnl {
        pass.attempted += 1;
        let joined = match venue.ensure_pnl_plant().await {
            Ok(()) => venue.subscribe_pnl().await,
            Err(e) => Err(e),
        };
        if joined.is_ok() {
            pass.restored += 1;
        } else {
            failed.pnl = true;
        }
    }
    if plan.order {
        pass.attempted += 1;
        if venue.subscribe_order_updates().await.is_ok() {
            pass.restored += 1;
        } else {
            failed.order = true;
        }
    }
    if plan.brackets {
        pass.attempted += 1;
        if !failed.order && venue.subscribe_bracket_updates().await.is_ok() {
            pass.restored += 1;
        } else {
            failed.brackets = true;
        }
    }
    pass.account_failed = has_account(&failed);
    (pass, failed)
}

/// Public compatibility entry point for restore coverage and dispatch tests.
pub async fn restore_pass<V: Venue + ?Sized>(
    guard: &V,
    reconnect: &ReconnectController,
    only_md: Option<&[MdIntent]>,
    account: bool,
) -> RestorePass {
    let mut plan = reconnect.restore_plan().await;
    if let Some(only) = only_md {
        plan.ticker
            .retain(|k| only.contains(&MdIntent::Ticker(k.clone())));
        plan.book
            .retain(|k| only.contains(&MdIntent::Book(k.clone())));
        plan.time_bars
            .retain(|k| only.contains(&MdIntent::TimeBars(k.clone())));
    }
    if !account {
        plan.pnl = false;
        plan.order = false;
        plan.brackets = false;
    }
    restore_plan_pass(guard, reconnect, plan).await.0
}

pub async fn restore_intents<V: Venue + ?Sized>(
    guard: &V,
    reconnect: &ReconnectController,
) -> (usize, usize) {
    let pass = restore_pass(guard, reconnect, None, true).await;
    (pass.restored, pass.attempted)
}
