//! `rithmic-gateway` — own one Rithmic login; serve plant-semantic IPC on a
//! unix socket. See `docs/references/ops-runbook.md`.

use std::str::FromStr;
use std::sync::Arc;

use rithmic_gateway::env_keys::{self, process_env_first};
use rithmic_gateway::idle_exit::parse_idle_exit_sec;
use rithmic_gateway::listen::{bind_unix, default_unix_path, ListenEndpoint};
use rithmic_gateway::server::{run_gateway, Fingerprint, GatewayParams, SessionHandle};
use rithmic_gateway::singleton::{SessionLock, STARTUP_LOCK_GRACE};
use rithmic_gateway::subscriptions::ParentGates;
use rithmic_plants::{DirectSession, PlantSet, Session, SessionConfig};
use rithmic_rs::RithmicEnv;

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
         Required env: RITHMIC_USER (or RITHMIC_USERNAME), RITHMIC_PASSWORD\n\
         Optional: RITHMIC_SYSTEM (or RITHMIC_SYSTEM_NAME), RITHMIC_GATEWAY (or RITHMIC_URL),\n\
                   RITHMIC_ENV, RITHMIC_HISTORY_READY_PROBE (0 = skip the history Ready probe),\n\
                   RITHMIC_GATEWAY_LISTEN (unix://…),\n\
                   RITHMIC_ENABLE_TRADING, RITHMIC_GATEWAY_CANCEL_ALL,\n\
                   RITHMIC_GATEWAY_AUTH_TOKEN,\n\
                   RITHMIC_GATEWAY_IDLE_EXIT_SEC (unset/-1=never; 0=immediate; N=grace),\n\
                   RITHMIC_ACCOUNT_ID, RITHMIC_FCM_ID, RITHMIC_IB_ID\n\
         See docs/references/ops-runbook.md and docs/references/gateway-remote.md."
    );
}

async fn run() -> Result<(), String> {
    // Same keys and precedence as the Python clients (`env_keys`): one env file,
    // one login — never a silent fallback to the production URL.
    let user = process_env_first(env_keys::USER_KEYS).ok_or("RITHMIC_USER required")?;
    let password = std::env::var("RITHMIC_PASSWORD").map_err(|_| "RITHMIC_PASSWORD required")?;
    let endpoint = env_keys::resolve_endpoint(|key| std::env::var(key).ok())?;
    let env_keys::Endpoint {
        system_name,
        url,
        env_name,
    } = endpoint;
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
    let _lock =
        SessionLock::try_acquire_patient(&user, &system_name, &url, &env_name, STARTUP_LOCK_GRACE)
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

    let session = Arc::new(DirectSession::new(cfg, plants));
    if let Err(e) = session.connect().await {
        let _ = session.disconnect().await;
        drop(listener);
        let _ = std::fs::remove_file(&path);
        return Err(format!("rithmic connect: {e}"));
    }
    eprintln!("rithmic-gateway: plants connected ({plants:?}; history ready)");

    run_gateway(GatewayParams {
        listener,
        path,
        fingerprint: Fingerprint {
            user,
            system_name,
            url,
            env: env_name,
            account_id,
            fcm_id,
            ib_id,
        },
        expected_auth_token,
        idle_policy,
        gates: ParentGates::from_env(),
        session: SessionHandle::new(session),
    })
    .await
}
