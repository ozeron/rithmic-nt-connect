//! Parent-side event pump and reconnect: poll every connected plant, fan out
//! events, and drive parent-owned reconnect + refcounted intent restore
//! (F6 / KTD11) on a connection issue. Lives in the library so the reconnect
//! path stays unit-testable; the binary keeps only CLI orchestration.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rithmic_plants::{Session, Stream};
use tokio::task::JoinSet;

use crate::reconnect::ReconnectController;
use crate::subscriptions::FanoutHub;

use super::dispatch::TopicIntent;
use super::{publish_plant_event, GatewayState, SessionHandle};

/// Events taken from a stream per wake. A busy ticker is drained in batches
/// without a round trip per event; an idle one costs nothing.
const BATCH: usize = 1024;
/// Upper bound on one wait. It only bounds how soon a stream task notices that
/// it should stop; events and plant attaches wake it immediately.
const IDLE_WAIT: Duration = Duration::from_secs(1);
/// Pause after a non-connection error so a persistent fault cannot spin.
const ERROR_BACKOFF: Duration = Duration::from_millis(250);

/// True for the subset of `rithmic_plants::Error` that the session
/// constructor maps `is_connection_issue()` errors onto (see
/// `rithmic_plants::session::forward` / the stream lanes).
///
/// A gap is a slow consumer of a live plant channel, not a transport failure:
/// it is published to clients and must not tear plants down.
fn is_connection_issue(e: &rithmic_plants::Error) -> bool {
    matches!(
        e,
        rithmic_plants::Error::Rithmic(_) | rithmic_plants::Error::ChannelClosed { .. }
    )
}

/// Serve one plant stream until it reports a connection issue. Waits on the
/// stream (no session lock held, no polling) and publishes every event, and
/// every gap, as it arrives.
async fn serve_stream(session: Arc<dyn Session>, hub: Arc<FanoutHub>, stream: Stream) {
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
                tokio::time::sleep(ERROR_BACKOFF).await;
            }
        }
    }
}

/// Run until a plant connection issue (or a forced reconnect) is seen.
async fn serve_until_issue(session: &SessionHandle, hub: &Arc<FanoutHub>, state: &GatewayState) {
    if state.force_reconnect.swap(false, Ordering::SeqCst) {
        return;
    }
    // Wait for the gate once (a reconnect holds it) and then work on the plain
    // session: waiting for events must never hold it.
    let plants: Arc<dyn Session> = session.checkout().await;
    let mut streams = JoinSet::new();
    for stream in Stream::ALL {
        streams.spawn(serve_stream(Arc::clone(&plants), Arc::clone(hub), stream));
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
pub async fn run_event_pump(
    session: SessionHandle,
    hub: Arc<FanoutHub>,
    reconnect: Arc<ReconnectController>,
    state: Arc<GatewayState>,
) {
    loop {
        serve_until_issue(&session, &hub, &state).await;
        reconnect_loop(&session, &reconnect, &state).await;
    }
}

async fn reconnect_loop(
    session: &SessionHandle,
    reconnect: &ReconnectController,
    state: &GatewayState,
) {
    eprintln!("rithmic-gateway: plant connection issue detected; reconnecting...");
    state.ready.store(false, Ordering::SeqCst);
    loop {
        {
            let guard = session.lock_exclusive().await;
            let _ = guard.disconnect().await;
            if let Err(e) = guard.connect().await {
                drop(guard);
                eprintln!("rithmic-gateway: reconnect failed: {e}; retrying in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        }
        // Connect succeeded: retry restore a bounded number of times without
        // tearing TLS down. If still incomplete, full-reconnect (outer loop)
        // while staying Ready=false — never claim Ready with half-restored intent.
        for attempt in 1..=6u32 {
            let guard = session.lock_exclusive().await;
            let (restored, attempted) = restore_intents(&*guard, reconnect).await;
            drop(guard);
            eprintln!(
                "rithmic-gateway: restore attempt {attempt}: {restored}/{attempted} subscriptions"
            );
            if restored == attempted {
                let next = state.transport_generation.fetch_add(1, Ordering::SeqCst) + 1;
                let _ = state.transport_epoch.send(next);
                state.ready.store(true, Ordering::SeqCst);
                return;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        eprintln!("rithmic-gateway: restore still incomplete after 6 attempts; full reconnect");
    }
}

/// Re-issue every remembered intent on a freshly reconnected session; returns
/// `(restored, attempted)`. Public so the RPC-table tests can prove each
/// subscription intent is re-issued.
pub async fn restore_intents(
    guard: &dyn Session,
    reconnect: &ReconnectController,
) -> (usize, usize) {
    let plan = reconnect.restore_plan().await;
    let mut restored = 0usize;
    let mut attempted = 0usize;

    // MD intents (ticker / book / time bars, in that order) share the
    // dispatch-side venue-join mapping via TopicIntent. The failure policy
    // here is deliberately log-and-continue — dispatch rolls back instead.
    let mut md_intents: Vec<TopicIntent> = Vec::new();
    md_intents.extend(plan.ticker.iter().cloned().map(TopicIntent::Ticker));
    md_intents.extend(plan.book.iter().cloned().map(TopicIntent::Book));
    md_intents.extend(plan.time_bars.iter().cloned().map(TopicIntent::TimeBars));
    for intent in &md_intents {
        attempted += 1;
        match intent.venue_join(guard).await {
            Ok(()) => restored += 1,
            Err(e) => eprintln!("{}", intent.resubscribe_fail_log(e)),
        }
    }
    if plan.pnl {
        attempted += 1;
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
            restored += 1;
        }
    }
    let mut order_ok = !plan.order;
    if plan.order {
        attempted += 1;
        match guard.subscribe_order_updates().await {
            Ok(()) => {
                restored += 1;
                order_ok = true;
            }
            Err(e) => eprintln!("rithmic-gateway: resubscribe order updates failed: {e}"),
        }
    }
    if plan.brackets {
        attempted += 1;
        // Order updates failure disconnects the plant — do not ensure+subscribe
        // brackets alone (would look like a partial restore success).
        if plan.order && !order_ok {
            eprintln!("rithmic-gateway: skip bracket restore; order updates not restored");
        } else {
            match guard.subscribe_bracket_updates().await {
                Ok(()) => restored += 1,
                Err(e) => eprintln!("rithmic-gateway: resubscribe bracket updates failed: {e}"),
            }
        }
    }
    (restored, attempted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconnect::TimeBarIntent;
    use crate::subscriptions::{FanoutHub, SharedFanout, SubKey};
    use rithmic_plants::{DirectSession, PlantSet};
    use std::str::FromStr;

    /// An unconnected session: every venue call fails fast, so restore
    /// tallies are exercised without any network. The pump's log-and-
    /// continue policy means every attempt is counted and nothing restored.
    fn offline_session() -> DirectSession {
        let cfg = rithmic_plants::SessionConfig::builder()
            .user("tally-test")
            .password("pw")
            .system_name("LucidTrading")
            .url("wss://rprotocol.rithmic.com:443")
            .env(rithmic_rs::RithmicEnv::from_str("live").expect("env"))
            .build()
            .expect("config");
        DirectSession::new(cfg, PlantSet::MARKET_DATA)
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    }

    fn sub(symbol: &str) -> SubKey {
        SubKey {
            symbol: symbol.into(),
            exchange: "CME".into(),
        }
    }

    #[test]
    fn empty_plan_restores_nothing() {
        let rt = runtime();
        rt.block_on(async {
            let hub: SharedFanout = Arc::new(FanoutHub::new(8));
            let reconnect = ReconnectController::new(hub);
            let session = offline_session();
            let (restored, attempted) = restore_intents(&session, &reconnect).await;
            assert_eq!((restored, attempted), (0, 0));
        });
    }

    #[test]
    fn md_intents_counted_per_kind() {
        let rt = runtime();
        rt.block_on(async {
            let hub: SharedFanout = Arc::new(FanoutHub::new(8));
            let reconnect = ReconnectController::new(hub);
            reconnect.note_ticker(sub("NQ")).await;
            reconnect.note_book(sub("ES")).await;
            reconnect
                .note_time_bar(TimeBarIntent {
                    symbol: "NQ".into(),
                    exchange: "CME".into(),
                    bar_type: 1,
                    period: 60,
                })
                .await;
            let session = offline_session();
            let (restored, attempted) = restore_intents(&session, &reconnect).await;
            // All three MD kinds attempted in plan order; all fail offline.
            assert_eq!((restored, attempted), (0, 3));
        });
    }

    #[test]
    fn brackets_counted_but_skipped_when_order_updates_fail() {
        let rt = runtime();
        rt.block_on(async {
            let hub: SharedFanout = Arc::new(FanoutHub::new(8));
            let reconnect = ReconnectController::new(hub);
            reconnect.note_order().await;
            reconnect.note_brackets().await;
            let session = offline_session();
            let (restored, attempted) = restore_intents(&session, &reconnect).await;
            // Order updates counted + failed; brackets still counted even
            // though their venue call is skipped to avoid a partial restore.
            assert_eq!(attempted, 2);
            assert_eq!(restored, 0);
        });
    }
}
