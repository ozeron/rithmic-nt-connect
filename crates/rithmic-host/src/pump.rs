//! Parent-side event pump and reconnect: poll every connected plant, fan out
//! events, and drive parent-owned reconnect + refcounted intent restore
//! (F6 / KTD11) on a connection issue. Lives in the library so the reconnect
//! path stays unit-testable; the binary keeps only CLI orchestration.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::{Stream, Venue};

use tokio::task::JoinSet;

use crate::fanout::FanoutHub;
use crate::intents::{MdIntent, ReconnectController};

use crate::fanout::publish_plant_event;
use crate::host::{HostState, VenueHandle};
use crate::local::subscriptions::TopicIntent;

/// Events taken from a stream per wake. A busy ticker is drained in batches
/// without a round trip per event; an idle one costs nothing.
const BATCH: usize = 1024;
/// Upper bound on one wait. It only bounds how soon a stream task notices that
/// it should stop; events and plant attaches wake it immediately.
const IDLE_WAIT: Duration = Duration::from_secs(1);
/// Pause after a non-connection error so a persistent fault cannot spin.
const ERROR_BACKOFF: Duration = Duration::from_millis(250);

/// The pump's pauses. Production uses [`Timing::default`]; tests pass short
/// ones (or run under paused tokio time) instead of sleeping for real.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    /// After a non-connection stream error.
    pub error_backoff: Duration,
    /// Before retrying a failed plant connect.
    pub connect_retry: Duration,
    /// Between restore passes.
    pub restore_backoff: Duration,
}

impl Default for Timing {
    /// Production defaults; composition roots may supply overrides.
    fn default() -> Self {
        Self {
            error_backoff: ERROR_BACKOFF,
            connect_retry: CONNECT_RETRY,
            restore_backoff: RESTORE_BACKOFF,
        }
    }
}

/// True for the subset of `crate::Error` that the session
/// constructor maps `is_connection_issue()` errors onto (see
/// `rithmic_plants::session::forward` / the stream lanes).
///
/// A gap is a slow consumer of a live plant channel, not a transport failure:
/// it is published to clients and must not tear plants down.
fn is_connection_issue(e: &crate::Error) -> bool {
    matches!(
        e,
        crate::Error::Rithmic(_) | crate::Error::ChannelClosed { .. }
    )
}

/// Serve one plant stream until it reports a connection issue. Waits on the
/// stream (no session lock held, no polling) and publishes every event, and
/// every gap, as it arrives.
async fn serve_stream<V: Venue + ?Sized + 'static>(
    session: Arc<V>,
    hub: Arc<FanoutHub>,
    stream: Stream,
    error_backoff: Duration,
) {
    loop {
        match session.next_events(stream, BATCH, IDLE_WAIT).await {
            Ok(events) => {
                for event in events {
                    publish_plant_event(&hub, event).await;
                }
            }
            Err(e) => {
                eprintln!("rithmic-gateway: {} stream error: {e}", stream.plant());
                if is_connection_issue(&e) {
                    return;
                }
                tokio::time::sleep(error_backoff).await;
            }
        }
    }
}

/// Run until a plant connection issue (or a forced reconnect) is seen.
async fn serve_until_issue<V: Venue + ?Sized + 'static>(
    session: &VenueHandle<V>,
    hub: &Arc<FanoutHub>,
    state: &HostState<V>,
) {
    if state.force_reconnect.swap(false, Ordering::SeqCst) {
        return;
    }
    // Wait for the gate once (a reconnect holds it) and then work on the plain
    // session: waiting for events must never hold it.
    let plants: Arc<V> = session.checkout().await;
    let mut streams = JoinSet::new();
    for stream in Stream::ALL {
        streams.spawn(serve_stream(
            Arc::clone(&plants),
            Arc::clone(hub),
            stream,
            state.timing.error_backoff,
        ));
    }
    loop {
        tokio::select! {
            // One stream task ending means that plant reported a connection issue.
            _ = streams.join_next() => break,
            _ = state.reconnect_wake.notified() => {
                if state.force_reconnect.swap(false, Ordering::SeqCst) {
                    break;
                }
            }
        }
    }
    streams.abort_all();
    while streams.join_next().await.is_some() {}
}

/// Serve every connected plant stream, fan events out, and drive parent-owned
/// reconnect + refcounted intent restore (F6 / KTD11) on a connection issue.
/// Clients never see this — they keep their broker connection and simply
/// stop receiving events until plants are back.
pub async fn run_event_pump<V: Venue + ?Sized + 'static>(state: Arc<HostState<V>>) {
    let session = state.session.as_ref().expect("pump requires a venue");
    while !state.ready.load(Ordering::SeqCst) {
        let wake = state.reconnect_wake.notified();
        if state.ready.load(Ordering::SeqCst) {
            break;
        }
        wake.await;
    }
    loop {
        serve_until_issue(session, &state.hub, &state).await;
        reconnect_loop(session, &state.reconnect, &state).await;
    }
}

/// Pause before retrying a failed plant connect.
const CONNECT_RETRY: Duration = Duration::from_secs(5);
/// Restore passes before a market-data join that keeps failing is left degraded.
const RESTORE_ATTEMPTS: u32 = 3;
/// Pause between restore passes (override with `RITHMIC_GATEWAY_RESTORE_BACKOFF_MS`,
/// which the integration tests use to keep the bounded give-up quick).
const RESTORE_BACKOFF: Duration = Duration::from_secs(2);

/// Rebuild the plants and restore every client's subscriptions, then tell the
/// attached clients (the epoch bump). A market-data join that cannot be restored
/// after a few passes (an expired contract, say) is left *degraded* instead of
/// holding every client out: the gateway becomes ready, the join stays noted for
/// the clients that hold it, and their next subscribe retries it. An account-level
/// restore (PnL, orders) that keeps failing still keeps the gateway not ready,
/// because trading on a half-restored order plant is the unsafe choice.
async fn reconnect_loop<V: Venue + ?Sized + 'static>(
    session: &VenueHandle<V>,
    reconnect: &ReconnectController,
    state: &HostState<V>,
) {
    eprintln!("rithmic-gateway: plant connection issue detected; reconnecting...");
    state.recovering.store(true, Ordering::SeqCst);
    state.ready.store(false, Ordering::SeqCst);
    loop {
        {
            let guard = session.lock_exclusive().await;
            let _ = guard.disconnect().await;
            if let Err(e) = guard.connect().await {
                drop(guard);
                eprintln!(
                    "rithmic-gateway: reconnect failed: {e}; retrying in {:.1}s",
                    state.timing.connect_retry.as_secs_f64()
                );
                tokio::time::sleep(state.timing.connect_retry).await;
                continue;
            }
        }
        // Connect succeeded: restore without tearing TLS down. Later passes retry
        // only what failed, so a join that already succeeded is not issued twice.
        let mut pending: Option<Vec<MdIntent>> = None; // None = every market-data join
        let mut account = true;
        let mut failed_md: Vec<MdIntent> = Vec::new();
        for attempt in 1..=RESTORE_ATTEMPTS {
            let guard = session.lock_exclusive().await;
            let pass = restore_pass(&*guard, reconnect, pending.as_deref(), account).await;
            drop(guard);
            eprintln!(
                "rithmic-gateway: restore attempt {attempt}: {}/{} subscriptions",
                pass.restored, pass.attempted
            );
            account = pass.account_failed;
            failed_md = pass.failed_md;
            pending = Some(failed_md.clone());
            if !account && failed_md.is_empty() {
                break;
            }
            if attempt < RESTORE_ATTEMPTS {
                tokio::time::sleep(state.timing.restore_backoff).await;
            }
        }
        if account {
            eprintln!(
                "rithmic-gateway: account restore still incomplete after {RESTORE_ATTEMPTS} \
                 attempts; full reconnect in {:.1}s",
                state.timing.connect_retry.as_secs_f64()
            );
            tokio::time::sleep(state.timing.connect_retry).await;
            continue;
        }
        for intent in failed_md {
            eprintln!(
                "rithmic-gateway: could not restore {intent:?}; left degraded until a client \
                 subscribes again"
            );
            reconnect.mark_degraded(intent).await;
        }
        // A rebuild was requested while this one ran (a failed reseat, say): this
        // one just rebuilt everything, so it must not trigger a second.
        state.force_reconnect.store(false, Ordering::SeqCst);
        // Ready first: a client that resynchronizes on the epoch bump must not meet
        // `not_ready`.
        state.recovering.store(false, Ordering::SeqCst);
        state.ready.store(true, Ordering::SeqCst);
        let next = state.transport_generation.fetch_add(1, Ordering::SeqCst) + 1;
        state.transport_epoch.send_replace(next);
        return;
    }
}

/// What one restore pass achieved.
#[derive(Debug, Default)]
pub struct RestorePass {
    pub restored: usize,
    pub attempted: usize,
    /// Market-data joins that failed and should be retried (or left degraded).
    pub failed_md: Vec<MdIntent>,
    /// An account-level join (PnL, order updates, brackets) failed.
    pub account_failed: bool,
}

/// One restore pass over the remembered intents. `only_md` limits the
/// market-data joins to those listed (a retry pass); `account` says whether to
/// restore the account-level joins (PnL, orders, brackets) in this pass.
pub async fn restore_pass<V: Venue + ?Sized>(
    guard: &V,
    reconnect: &ReconnectController,
    only_md: Option<&[MdIntent]>,
    account: bool,
) -> RestorePass {
    let plan = reconnect.restore_plan().await;
    let mut pass = RestorePass::default();

    // MD intents (ticker / book / time bars, in that order) share the
    // dispatch-side venue-join mapping via TopicIntent. The failure policy
    // here is deliberately log-and-continue — dispatch rolls back instead.
    let mut md: Vec<MdIntent> = Vec::new();
    md.extend(plan.ticker.iter().cloned().map(MdIntent::Ticker));
    md.extend(plan.book.iter().cloned().map(MdIntent::Book));
    md.extend(plan.time_bars.iter().cloned().map(MdIntent::TimeBars));
    for intent in md {
        if only_md.is_some_and(|only| !only.contains(&intent)) {
            continue;
        }
        let topic = TopicIntent::from_md(&intent);
        pass.attempted += 1;
        match topic.venue_join(guard).await {
            Ok(()) => {
                pass.restored += 1;
                // Restored by a later rebuild: no longer degraded.
                reconnect.clear_degraded(&intent).await;
            }
            Err(e) => {
                eprintln!("{}", topic.resubscribe_fail_log(e));
                pass.failed_md.push(intent);
            }
        }
    }
    if !account {
        return pass;
    }
    let mut account_ok = true;
    if plan.pnl {
        pass.attempted += 1;
        let pnl_ok = match guard.ensure_pnl_plant().await {
            Ok(()) => match guard.subscribe_pnl().await {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("rithmic-gateway: resubscribe pnl failed: {e}");
                    false
                }
            },
            Err(e) => {
                eprintln!("rithmic-gateway: ensure pnl on reconnect failed: {e}");
                false
            }
        };
        if pnl_ok {
            pass.restored += 1;
        } else {
            account_ok = false;
        }
    }
    let mut order_ok = !plan.order;
    if plan.order {
        pass.attempted += 1;
        match guard.subscribe_order_updates().await {
            Ok(()) => {
                pass.restored += 1;
                order_ok = true;
            }
            Err(e) => {
                eprintln!("rithmic-gateway: resubscribe order updates failed: {e}");
                account_ok = false;
            }
        }
    }
    if plan.brackets {
        pass.attempted += 1;
        // Order updates failure disconnects the plant — do not ensure+subscribe
        // brackets alone (would look like a partial restore success).
        if plan.order && !order_ok {
            eprintln!("rithmic-gateway: skip bracket restore; order updates not restored");
            account_ok = false;
        } else {
            match guard.subscribe_bracket_updates().await {
                Ok(()) => pass.restored += 1,
                Err(e) => {
                    eprintln!("rithmic-gateway: resubscribe bracket updates failed: {e}");
                    account_ok = false;
                }
            }
        }
    }
    pass.account_failed = !account_ok;
    pass
}

/// Re-issue every remembered intent on a freshly reconnected session; returns
/// `(restored, attempted)`. Public so the RPC-table tests can prove each
/// subscription intent is re-issued.
pub async fn restore_intents<V: Venue + ?Sized>(
    guard: &V,
    reconnect: &ReconnectController,
) -> (usize, usize) {
    let pass = restore_pass(guard, reconnect, None, true).await;
    (pass.restored, pass.attempted)
}
