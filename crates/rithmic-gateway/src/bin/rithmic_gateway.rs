//! `rithmic-gateway` — own one Rithmic login and serve plant-semantic IPC.

use std::str::FromStr;
use std::sync::Arc;

use rithmic_gateway::listen::{bind_unix, default_unix_path, ListenEndpoint};
use rithmic_gateway::server::{Fingerprint, GatewayState};
use rithmic_gateway::singleton::SessionLock;
use rithmic_gateway::subscriptions::{FanoutHub, ParentGates};
use rithmic_plants::{PlantSet, RithmicEnv, RithmicSession, SessionConfig};

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
    let env_name = std::env::var("RITHMIC_ENV").unwrap_or_else(|_| "live".into());
    let env = RithmicEnv::from_str(&env_name.to_ascii_lowercase())
        .map_err(|e| format!("RITHMIC_ENV: {e}"))?;

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

    let cfg = SessionConfig::builder()
        .user(user.clone())
        .password(password)
        .system_name(system_name.clone())
        .url(url.clone())
        .env(env)
        .build()
        .map_err(|e| e.to_string())?;

    let mut session = RithmicSession::with_plants(cfg, PlantSet::MARKET_DATA);
    session
        .connect()
        .await
        .map_err(|e| format!("rithmic connect: {e}"))?;

    let listener = bind_unix(&path)
        .await
        .map_err(|e| format!("bind {}: {e}", path.display()))?;
    eprintln!("rithmic-gateway listening on unix://{}", path.display());

    let gates = ParentGates::from_env();
    let state = Arc::new(GatewayState {
        gates,
        hub: Arc::new(FanoutHub::new(1024)),
        fingerprint: Fingerprint {
            user,
            system_name,
            url,
            env: env_name,
            account_id: std::env::var("RITHMIC_ACCOUNT_ID").unwrap_or_default(),
            fcm_id: std::env::var("RITHMIC_FCM_ID").unwrap_or_default(),
            ib_id: std::env::var("RITHMIC_IB_ID").unwrap_or_default(),
        },
        ready: true,
    });

    let _session = session;
    rithmic_gateway::server::serve(listener, state)
        .await
        .map_err(|e| e.to_string())
}
