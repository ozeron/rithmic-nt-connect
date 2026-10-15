//! `rithmic-gateway` — own one Rithmic login; serve plant-semantic IPC on a
//! unix socket. See `docs/references/ops-runbook.md`.

mod root_config;

use std::str::FromStr;
use std::sync::Arc;

use rithmic_gateway::env_keys::{self, process_env_first};
use rithmic_gateway::idle_exit::parse_idle_exit_sec;
use rithmic_gateway::listen::{bind_unix, default_unix_path, ListenEndpoint};
use rithmic_gateway::server::{run_gateway, Fingerprint, GatewayParams};
use rithmic_gateway::singleton::SessionLock;
use rithmic_host::Env;
use rithmic_host::{
    HistoryPolicy, HostFingerprint, HostOptions, HostRegistry, PlantSet, Session, Venue,
};
use rithmic_plants::{RithmicVenue, SessionConfig};

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    if let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            print_help();
            return;
        }
        if arg == "--version" || arg == "-V" {
            println!("rithmic-gateway {}", env!("CARGO_PKG_VERSION"));
            return;
        }
        eprintln!("unknown argument (configuration uses environment variables; see --help)");
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
    // Preserve the lowercase venue environment parser ("live"/"demo"/"test").
    let env = Env::from_str(&env_name.to_lowercase())
        .map_err(|e| format!("RITHMIC_ENV {env_name:?}: {e}"))?;

    let idle_policy = parse_idle_exit_sec(
        std::env::var("RITHMIC_GATEWAY_IDLE_EXIT_SEC")
            .ok()
            .as_deref(),
    )?;

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

    // The parent's own gates, enforced in the session as well as at dispatch, so
    // no caller of this session can bypass them.
    let gates = root_config::gates();
    let mut key = HostFingerprint::new(&user, &system_name, &url, &env_name);
    key.account_id = Some(account_id.clone()).filter(|s| !s.is_empty());
    key.fcm_id = Some(fcm_id.clone()).filter(|s| !s.is_empty());
    key.ib_id = Some(ib_id.clone()).filter(|s| !s.is_empty());
    key.app_name = cfg.app_name().into();
    key.app_version = cfg.app_version().into();
    let registry = HostRegistry::<dyn Venue>::default();
    let lock_path = SessionLock::lock_path(&user, &system_name, &url, &env_name);
    let lock_dir = lock_path.parent().expect("lock directory");
    let host = registry
        .get_or_start(
            key,
            || {
                Arc::new(RithmicVenue::new(cfg, PlantSet::MARKET_DATA).with_gates(true, true))
                    as Arc<dyn Venue>
            },
            lock_dir,
            HostOptions {
                gates,
                timing: root_config::timing(),
                history_policy: HistoryPolicy::RefuseUnderLiveMd,
                lock_filename: lock_path.file_name().map(ToOwned::to_owned),
            },
        )
        .await
        .map_err(|e| format!("credential flock: {e}"))?;
    let session = host.attach();
    session
        .request_plants(plants)
        .await
        .map_err(|e| e.to_string())?;
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

    if let Err(e) = session.connect().await {
        let _ = session.disconnect().await;
        drop(listener);
        let _ = std::fs::remove_file(&path);
        return Err(format!("rithmic connect: {e}"));
    }
    eprintln!("rithmic-gateway: plants connected ({plants:?}; history ready)");

    let result = run_gateway(GatewayParams {
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
        host: host.clone(),
    })
    .await;
    session.disconnect().await.map_err(|e| e.to_string())?;
    result
}
