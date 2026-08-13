//! `rithmic-gateway` — own one Rithmic login; serve plant-semantic IPC on a
//! unix socket. See `docs/references/ops-runbook.md`.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

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
                   RITHMIC_ACCOUNT_ID, RITHMIC_FCM_ID, RITHMIC_IB_ID\n\
         See docs/references/ops-runbook.md and docs/references/gateway-remote.md."
    );
}

async fn run() -> Result<(), String> {
    let user = std::env::var("RITHMIC_USER").map_err(|_| "RITHMIC_USER required")?;
    let password = std::env::var("RITHMIC_PASSWORD").map_err(|_| "RITHMIC_PASSWORD required")?;
    let system_name =
        std::env::var("RITHMIC_SYSTEM_NAME").unwrap_or_else(|_| "LucidTrading".into());
    let url = std::env::var("RITHMIC_URL")
        .unwrap_or_else(|_| "wss://rprotocol.rithmic.com:443".into());
    let env_name = std::env::var("RITHMIC_ENV").unwrap_or_else(|_| "Live".into());
    // rithmic-rs only parses lowercase env names ("live"/"demo"/"test").
    let env = RithmicEnv::from_str(&env_name.to_lowercase())
        .map_err(|e| format!("RITHMIC_ENV {env_name:?}: {e}"))?;

    // Flock first (R10 / AGENTS.md single-login): refuse before ever
    // touching Rithmic if a direct or parent process already holds it.
    let _lock = SessionLock::try_acquire(&user, &system_name, &url)
        .map_err(|e| format!("credential flock: {e}"))?;

    let listen_raw = std::env::var("RITHMIC_GATEWAY_LISTEN").unwrap_or_else(|_| {
        format!(
            "unix://{}",
            default_unix_path(&user, &system_name, &url).display()
        )
    });
    let endpoint = ListenEndpoint::parse(&listen_raw).map_err(|e| e.to_string())?;
    let ListenEndpoint::Unix(path) = endpoint;

    let account_id = std::env::var("RITHMIC_ACCOUNT_ID").unwrap_or_default();
    let fcm_id = std::env::var("RITHMIC_FCM_ID").unwrap_or_default();
    let ib_id = std::env::var("RITHMIC_IB_ID").unwrap_or_default();

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

    // Plants connect before the listener binds (KTD5): Ready only after
    // plants are up, so a client never sees a socket for a dead parent.
    let plants = if cfg.account().is_some() {
        PlantSet::EXECUTION
    } else {
        PlantSet::MARKET_DATA
    };
    let mut session = RithmicSession::with_plants(cfg, plants);
    session
        .connect()
        .await
        .map_err(|e| format!("rithmic connect: {e}"))?;
    eprintln!("rithmic-gateway: plants connected ({plants:?})");

    let listener = bind_unix(&path)
        .await
        .map_err(|e| format!("bind {}: {e}", path.display()))?;
    eprintln!("rithmic-gateway listening on unix://{}", path.display());

    let gates = ParentGates::from_env();
    let hub = Arc::new(FanoutHub::new(DEFAULT_QUEUE_CAP));
    let reconnect = Arc::new(ReconnectController::new(hub.clone()));
    let session = Arc::new(TokioMutex::new(session));

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
        ready: true,
        session: Some(session.clone()),
        reconnect: reconnect.clone(),
    });

    tokio::spawn(run_event_pump(session, hub, reconnect));

    serve(listener, state).await.map_err(|e| e.to_string())
}

/// True for the subset of `rithmic_plants::Error` that the session
/// constructor maps `is_connection_issue()` errors onto (see
/// `rithmic_plants::session::check_response_ref` / broadcast poll helpers).
fn is_connection_issue(e: &rithmic_plants::Error) -> bool {
    matches!(
        e,
        rithmic_plants::Error::Rithmic(_)
            | rithmic_plants::Error::ChannelClosed { .. }
            | rithmic_plants::Error::ChannelLagged { .. }
    )
}

/// Poll every connected plant, fan out events, and drive parent-owned
/// reconnect + refcounted intent restore (F6 / KTD11) on a connection issue.
/// Clients never see this — they keep their broker connection and simply
/// stop receiving events until plants are back.
async fn run_event_pump(
    session: Arc<TokioMutex<RithmicSession>>,
    hub: Arc<FanoutHub>,
    reconnect: Arc<ReconnectController>,
) {
    loop {
        let mut connection_issue = false;
        {
            let mut guard = session.lock().await;
            match guard.poll_event() {
                Ok(Some(event)) => publish_plant_event(&hub, event).await,
                Ok(None) => {}
                Err(e) => {
                    eprintln!("rithmic-gateway: ticker poll error: {e}");
                    connection_issue |= is_connection_issue(&e);
                }
            }
            match guard.poll_history_event() {
                Ok(Some(event)) => publish_plant_event(&hub, event).await,
                Ok(None) => {}
                Err(e) => {
                    eprintln!("rithmic-gateway: history poll error: {e}");
                    connection_issue |= is_connection_issue(&e);
                }
            }
            match guard.poll_pnl_event() {
                Ok(Some(event)) => publish_plant_event(&hub, event).await,
                Ok(None) => {}
                Err(e) => {
                    eprintln!("rithmic-gateway: pnl poll error: {e}");
                    connection_issue |= is_connection_issue(&e);
                }
            }
            match guard.poll_order_event() {
                Ok(Some(event)) => publish_plant_event(&hub, event).await,
                Ok(None) => {}
                Err(e) => {
                    eprintln!("rithmic-gateway: order poll error: {e}");
                    connection_issue |= is_connection_issue(&e);
                }
            }
        }

        if connection_issue {
            reconnect_loop(&session, &reconnect).await;
        } else {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn reconnect_loop(session: &Arc<TokioMutex<RithmicSession>>, reconnect: &Arc<ReconnectController>) {
    eprintln!("rithmic-gateway: plant connection issue detected; reconnecting...");
    loop {
        let mut guard = session.lock().await;
        let _ = guard.disconnect().await;
        match guard.connect().await {
            Ok(()) => {
                let keys = reconnect.restore_after_reconnect().await;
                let mut restored = 0usize;
                for key in &keys {
                    match guard.subscribe(&key.symbol, &key.exchange).await {
                        Ok(()) => restored += 1,
                        Err(e) => eprintln!(
                            "rithmic-gateway: resubscribe {}/{} failed: {e}",
                            key.symbol, key.exchange
                        ),
                    }
                }
                eprintln!(
                    "rithmic-gateway: reconnected plants; restored {restored}/{} subscriptions",
                    keys.len()
                );
                return;
            }
            Err(e) => {
                drop(guard);
                eprintln!("rithmic-gateway: reconnect failed: {e}; retrying in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}
