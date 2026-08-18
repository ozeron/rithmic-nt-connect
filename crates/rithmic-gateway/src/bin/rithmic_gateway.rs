//! `rithmic-gateway` — own one Rithmic login; serve plant-semantic IPC on a
//! unix socket. See `docs/references/ops-runbook.md`.

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rithmic_gateway::idle_exit::{parse_idle_exit_sec, IdleExit};
use rithmic_gateway::listen::{bind_unix, default_unix_path, ListenEndpoint};
use rithmic_gateway::reconnect::ReconnectController;
use rithmic_gateway::server::{publish_plant_event, serve, Fingerprint, GatewayState};
use rithmic_gateway::singleton::SessionLock;
use rithmic_gateway::subscriptions::{FanoutHub, ParentGates, DEFAULT_QUEUE_CAP};
use rithmic_plants::{PlantSet, RithmicSession, SessionConfig};
use rithmic_rs::RithmicEnv;
use tokio::sync::Mutex as TokioMutex;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    if let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            print_help();
            return;
        }
        eprintln!("unknown argument: {arg}");
        print_help();
        std::process::exit(2);
    }

    if let Err(e) = run().await {
        eprintln!("rithmic-gateway error: {e}");
        std::process::exit(1);
    }
}

fn print_help() {
    eprintln!(
        "rithmic-gateway — own one Rithmic login; serve plant-semantic IPC on a unix socket.\n\n\
         Required env: RITHMIC_USER, RITHMIC_PASSWORD\n\
         Optional: RITHMIC_SYSTEM_NAME, RITHMIC_URL, RITHMIC_ENV,\n\
                   RITHMIC_GATEWAY_LISTEN (unix://…),\n\
                   RITHMIC_ENABLE_TRADING, RITHMIC_GATEWAY_CANCEL_ALL,\n\
                   RITHMIC_GATEWAY_AUTH_TOKEN,\n\
                   RITHMIC_GATEWAY_IDLE_EXIT_SEC (unset/-1=never; 0=immediate; N=grace),\n\
                   RITHMIC_ACCOUNT_ID, RITHMIC_FCM_ID, RITHMIC_IB_ID\n\
         See docs/references/ops-runbook.md and docs/references/gateway-remote.md."
    );
}

async fn run() -> Result<(), String> {
    let user = std::env::var("RITHMIC_USER").map_err(|_| "RITHMIC_USER required")?;
    let password = std::env::var("RITHMIC_PASSWORD").map_err(|_| "RITHMIC_PASSWORD required")?;
    let system_name =
        std::env::var("RITHMIC_SYSTEM_NAME").unwrap_or_else(|_| "LucidTrading".into());
    let url =
        std::env::var("RITHMIC_URL").unwrap_or_else(|_| "wss://rprotocol.rithmic.com:443".into());
    let env_name = std::env::var("RITHMIC_ENV").unwrap_or_else(|_| "Live".into());
    // rithmic-rs only parses lowercase env names ("live"/"demo"/"test").
    let env = RithmicEnv::from_str(&env_name.to_lowercase())
        .map_err(|e| format!("RITHMIC_ENV {env_name:?}: {e}"))?;

    let idle_policy = parse_idle_exit_sec(
        std::env::var("RITHMIC_GATEWAY_IDLE_EXIT_SEC")
            .ok()
            .as_deref(),
    )?;

    // Flock first (R10 / AGENTS.md single-login): refuse before ever
    // touching Rithmic if a direct or parent process already holds it.
    let _lock = SessionLock::try_acquire(&user, &system_name, &url, &env_name)
        .map_err(|e| format!("credential flock: {e}"))?;

    let listen_raw = std::env::var("RITHMIC_GATEWAY_LISTEN").unwrap_or_else(|_| {
        format!(
            "unix://{}",
            default_unix_path(&user, &system_name, &url, &env_name).display()
        )
    });
    let endpoint = ListenEndpoint::parse(&listen_raw).map_err(|e| e.to_string())?;
    let ListenEndpoint::Unix(path) = endpoint;

    let account_id = std::env::var("RITHMIC_ACCOUNT_ID").unwrap_or_default();
    let fcm_id = std::env::var("RITHMIC_FCM_ID").unwrap_or_default();
    let ib_id = std::env::var("RITHMIC_IB_ID").unwrap_or_default();
    let expected_auth_token = std::env::var("RITHMIC_GATEWAY_AUTH_TOKEN").unwrap_or_default();

    let mut cfg_builder = SessionConfig::builder()
        .user(user.clone())
        .password(password)
        .system_name(system_name.clone())
        .url(url.clone())
        .env(env);
    if !account_id.is_empty() {
        cfg_builder = cfg_builder.account_id(account_id.clone());
    }
    if !fcm_id.is_empty() {
        cfg_builder = cfg_builder.fcm_id(fcm_id.clone());
    }
    if !ib_id.is_empty() {
        cfg_builder = cfg_builder.ib_id(ib_id.clone());
    }
    let cfg = cfg_builder.build().map_err(|e| e.to_string())?;

    let plants = if cfg.account().is_some() {
        PlantSet::EXECUTION
    } else {
        PlantSet::MARKET_DATA
    };

    // Claim the listen path before plants.connect (KTD5 Ready still means plants
    // up — Handshakes get not_ready until ready=true). Early bind stops impostors
    // from squatting the sock during the Rithmic connect window.
    let listener = bind_unix(&path)
        .await
        .map_err(|e| format!("bind {}: {e}", path.display()))?;
    eprintln!(
        "rithmic-gateway listening on unix://{} (plants connecting…)",
        path.display()
    );

    let mut session = RithmicSession::with_plants(cfg, plants);
    if let Err(e) = session.connect().await {
        let _ = session.disconnect().await;
        drop(listener);
        let _ = std::fs::remove_file(&path);
        return Err(format!("rithmic connect: {e}"));
    }
    eprintln!("rithmic-gateway: plants connected ({plants:?})");

    let gates = ParentGates::from_env();
    let hub = Arc::new(FanoutHub::new(DEFAULT_QUEUE_CAP));
    let reconnect = Arc::new(ReconnectController::new(hub.clone()));
    let session = Arc::new(TokioMutex::new(session));
    // Ready only after plants are up (KTD5).
    let ready = AtomicBool::new(true);

    let state = Arc::new(GatewayState {
        gates,
        hub: hub.clone(),
        fingerprint: Fingerprint {
            user,
            system_name,
            url,
            env: env_name,
            account_id,
            fcm_id,
            ib_id,
        },
        ready,
        force_reconnect: AtomicBool::new(false),
        expected_auth_token,
        session: Some(session.clone()),
        reconnect: reconnect.clone(),
        topic_locks: TokioMutex::new(std::collections::HashMap::new()),
        recon_lock: Arc::new(TokioMutex::new(())),
        idle: IdleExit::new(idle_policy),
    });

    let pump = tokio::spawn(run_event_pump(
        session.clone(),
        hub,
        reconnect,
        Arc::clone(&state),
    ));

    let serve_result = serve(listener, state).await.map_err(|e| e.to_string());
    pump.abort();
    let _ = pump.await;
    {
        let mut guard = session.lock().await;
        let _ = guard.disconnect().await;
    }
    let _ = std::fs::remove_file(&path);
    serve_result
}

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

/// Poll every connected plant, fan out events, and drive parent-owned
/// reconnect + refcounted intent restore (F6 / KTD11) on a connection issue.
/// Clients never see this — they keep their broker connection and simply
/// stop receiving events until plants are back.
async fn run_event_pump(
    session: Arc<TokioMutex<RithmicSession>>,
    hub: Arc<FanoutHub>,
    reconnect: Arc<ReconnectController>,
    state: Arc<GatewayState>,
) {
    loop {
        let mut connection_issue = state.force_reconnect.swap(false, Ordering::SeqCst);
        {
            let mut guard = session.lock().await;
            match guard.poll_event() {
                Ok(Some(event)) => publish_plant_event(&hub, event).await,
                Ok(None) => {}
                Err(e) => {
                    if !is_not_connected(&e) {
                        eprintln!("rithmic-gateway: ticker poll error: {e}");
                        connection_issue |= is_connection_issue(&e);
                    }
                }
            }
            match guard.poll_history_event() {
                Ok(Some(event)) => publish_plant_event(&hub, event).await,
                Ok(None) => {}
                Err(e) => {
                    if !is_not_connected(&e) {
                        eprintln!("rithmic-gateway: history poll error: {e}");
                        connection_issue |= is_connection_issue(&e);
                    }
                }
            }
            match guard.poll_pnl_event() {
                Ok(Some(event)) => publish_plant_event(&hub, event).await,
                Ok(None) => {}
                Err(e) => {
                    if !is_not_connected(&e) {
                        eprintln!("rithmic-gateway: pnl poll error: {e}");
                        connection_issue |= is_connection_issue(&e);
                    }
                }
            }
            match guard.poll_order_event() {
                Ok(Some(event)) => publish_plant_event(&hub, event).await,
                Ok(None) => {}
                Err(e) => {
                    if !is_not_connected(&e) {
                        eprintln!("rithmic-gateway: order poll error: {e}");
                        connection_issue |= is_connection_issue(&e);
                    }
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
    reconnect: &Arc<ReconnectController>,
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
    for key in &plan.ticker {
        attempted += 1;
        match guard.subscribe(&key.symbol, &key.exchange).await {
            Ok(()) => restored += 1,
            Err(e) => eprintln!(
                "rithmic-gateway: resubscribe ticker {}/{} failed: {e}",
                key.symbol, key.exchange
            ),
        }
    }
    for key in &plan.book {
        attempted += 1;
        match guard
            .subscribe_order_book_summary(&key.symbol, &key.exchange)
            .await
        {
            Ok(()) => restored += 1,
            Err(e) => eprintln!(
                "rithmic-gateway: resubscribe book {}/{} failed: {e}",
                key.symbol, key.exchange
            ),
        }
    }
    for tb in &plan.time_bars {
        attempted += 1;
        match guard
            .subscribe_time_bars(&tb.symbol, &tb.exchange, tb.bar_type, tb.period)
            .await
        {
            Ok(()) => restored += 1,
            Err(e) => eprintln!(
                "rithmic-gateway: resubscribe time_bars {}/{} type={} period={} failed: {e}",
                tb.symbol, tb.exchange, tb.bar_type, tb.period
            ),
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
