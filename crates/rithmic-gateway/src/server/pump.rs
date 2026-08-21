//! Parent-side event pump and reconnect: poll every connected plant, fan out
//! events, and drive parent-owned reconnect + refcounted intent restore
//! (F6 / KTD11) on a connection issue. Lives in the library so the reconnect
//! path stays unit-testable; the binary keeps only CLI orchestration.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rithmic_plants::RithmicSession;
use tokio::sync::Mutex as TokioMutex;

use crate::reconnect::ReconnectController;
use crate::subscriptions::FanoutHub;

use super::dispatch::TopicIntent;
use super::{publish_plant_event, GatewayState};

/// Signature shared by all four plant poll methods.
type PlantPollFn =
    fn(&mut RithmicSession) -> rithmic_plants::Result<Option<rithmic_plants::dto::PlantEvent>>;

/// True for the subset of `rithmic_plants::Error` that the session
/// constructor maps `is_connection_issue()` errors onto (see
/// `rithmic_plants::session::check_response_ref` / broadcast poll helpers).
///
/// `ChannelLagged` is a slow consumer of a live plant channel — not a
/// transport failure — and must not tear plants down.
fn is_connection_issue(e: &rithmic_plants::Error) -> bool {
    matches!(
        e,
        rithmic_plants::Error::Rithmic(_) | rithmic_plants::Error::ChannelClosed { .. }
    )
}

fn is_not_connected(e: &rithmic_plants::Error) -> bool {
    matches!(e, rithmic_plants::Error::NotConnected { .. })
}

/// One plant poll + fan-out; returns whether the error is a connection issue.
/// The four poll methods share one signature, so a plain fn pointer suffices.
async fn poll_and_publish(
    guard: &mut RithmicSession,
    hub: &FanoutHub,
    label: &str,
    poll_fn: PlantPollFn,
) -> bool {
    match poll_fn(guard) {
        Ok(Some(event)) => publish_plant_event(hub, event).await,
        Ok(None) => {}
        Err(e) => {
            if !is_not_connected(&e) {
                eprintln!("rithmic-gateway: {label} poll error: {e}");
                return is_connection_issue(&e);
            }
        }
    }
    false
}

/// Poll every connected plant, fan out events, and drive parent-owned
/// reconnect + refcounted intent restore (F6 / KTD11) on a connection issue.
/// Clients never see this — they keep their broker connection and simply
/// stop receiving events until plants are back.
pub async fn run_event_pump(
    session: Arc<TokioMutex<RithmicSession>>,
    hub: Arc<FanoutHub>,
    reconnect: Arc<ReconnectController>,
    state: Arc<GatewayState>,
) {
    loop {
        let mut connection_issue = state.force_reconnect.swap(false, Ordering::SeqCst);
        {
            let mut guard = session.lock().await;
            // MD / history / pnl / order plants, polled in that fixed order.
            let polls: [(&str, PlantPollFn); 4] = [
                ("ticker", RithmicSession::poll_event),
                ("history", RithmicSession::poll_history_event),
                ("pnl", RithmicSession::poll_pnl_event),
                ("order", RithmicSession::poll_order_event),
            ];
            for (label, poll_fn) in polls {
                if poll_and_publish(&mut guard, &hub, label, poll_fn).await {
                    connection_issue = true;
                }
            }
        }

        if connection_issue {
            reconnect_loop(&session, &reconnect, &state).await;
        } else {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn reconnect_loop(
    session: &Arc<TokioMutex<RithmicSession>>,
    reconnect: &ReconnectController,
    state: &GatewayState,
) {
    eprintln!("rithmic-gateway: plant connection issue detected; reconnecting...");
    state.ready.store(false, Ordering::SeqCst);
    loop {
        {
            let mut guard = session.lock().await;
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
            let mut guard = session.lock().await;
            let (restored, attempted) = restore_intents(&mut guard, reconnect).await;
            drop(guard);
            eprintln!(
                "rithmic-gateway: restore attempt {attempt}: {restored}/{attempted} subscriptions"
            );
            if restored == attempted {
                state.ready.store(true, Ordering::SeqCst);
                return;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        eprintln!("rithmic-gateway: restore still incomplete after 6 attempts; full reconnect");
    }
}

async fn restore_intents(
    guard: &mut RithmicSession,
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
    use rithmic_plants::PlantSet;
    use std::str::FromStr;

    /// An unconnected session: every venue call fails fast, so restore
    /// tallies are exercised without any network. The pump's log-and-
    /// continue policy means every attempt is counted and nothing restored.
    fn offline_session() -> RithmicSession {
        let cfg = rithmic_plants::SessionConfig::builder()
            .user("tally-test")
            .password("pw")
            .system_name("LucidTrading")
            .url("wss://rprotocol.rithmic.com:443")
            .env(rithmic_rs::RithmicEnv::from_str("live").expect("env"))
            .build()
            .expect("config");
        RithmicSession::with_plants(cfg, PlantSet::MARKET_DATA)
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
            let mut session = offline_session();
            let (restored, attempted) = restore_intents(&mut session, &reconnect).await;
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
            let mut session = offline_session();
            let (restored, attempted) = restore_intents(&mut session, &reconnect).await;
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
            let mut session = offline_session();
            let (restored, attempted) = restore_intents(&mut session, &reconnect).await;
            // Order updates counted + failed; brackets still counted even
            // though their venue call is skipped to avoid a partial restore.
            assert_eq!(attempted, 2);
            assert_eq!(restored, 0);
        });
    }
}
