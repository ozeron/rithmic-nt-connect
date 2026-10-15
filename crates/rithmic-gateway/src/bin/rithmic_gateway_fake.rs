//! `rithmic-gateway-fake` — the real gateway server over a scripted fake plant.
//!
//! **Test-only.** Built only with `--features testing` (`required-features`),
//! never bundled into the wheel, and the shipped `rithmic-gateway` binary has no
//! switch that selects a fake plant. It runs the exact production server path
//! (`run_gateway`: handshake, gates, fan-out, event pump, reconnect + intent
//! restore, credential flock) so the same scenarios can drive the Python and
//! Rust clients without Rithmic credentials.
//!
//! Env: `RITHMIC_USER`, `RITHMIC_GATEWAY_LISTEN` (unix://…) and
//! `RITHMIC_FAKE_CONTROL` (unix socket path for the control protocol, see
//! `rithmic_gateway::testing`); plus the usual parent env (`RITHMIC_SYSTEM_NAME`,
//! `RITHMIC_URL`, `RITHMIC_ENV`, `RITHMIC_ENABLE_TRADING`,
//! `RITHMIC_GATEWAY_CANCEL_ALL`, `RITHMIC_GATEWAY_AUTH_TOKEN`,
//! `RITHMIC_GATEWAY_IDLE_EXIT_SEC`). No password is read.

use std::sync::Arc;

use rithmic_gateway::env_keys::{self, process_env_first};
use rithmic_gateway::idle_exit::parse_idle_exit_sec;
use rithmic_gateway::listen::{bind_unix, ListenEndpoint};
use rithmic_gateway::server::{run_gateway, Fingerprint, GatewayParams, SessionHandle};
use rithmic_gateway::singleton::{SessionLock, STARTUP_LOCK_GRACE};
use rithmic_gateway::subscriptions::ParentGates;
use rithmic_gateway::testing::serve_control;
use rithmic_plants::fake::FakeSession;
use rithmic_plants::Session;
use tokio::net::UnixListener;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("rithmic-gateway-fake error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let user = process_env_first(env_keys::USER_KEYS).ok_or("RITHMIC_USER required")?;
    let endpoint = env_keys::resolve_endpoint(|key| std::env::var(key).ok())?;
    let env_keys::Endpoint {
        system_name,
        url,
        env_name,
    } = endpoint;
    let idle_policy = parse_idle_exit_sec(
        std::env::var("RITHMIC_GATEWAY_IDLE_EXIT_SEC")
            .ok()
            .as_deref(),
    )?;

    // Same single-login discipline as production: hold the credential flock.
    let _lock =
        SessionLock::acquire_patient(&user, &system_name, &url, &env_name, STARTUP_LOCK_GRACE)
            .await
            .map_err(|e| format!("credential flock: {e}"))?;

    let listen_raw =
        std::env::var("RITHMIC_GATEWAY_LISTEN").map_err(|_| "RITHMIC_GATEWAY_LISTEN required")?;
    let ListenEndpoint::Unix(path) =
        ListenEndpoint::parse(&listen_raw).map_err(|e| e.to_string())?;
    let control_path =
        std::env::var("RITHMIC_FAKE_CONTROL").map_err(|_| "RITHMIC_FAKE_CONTROL required")?;

    let listener = bind_unix(&path)
        .await
        .map_err(|e| format!("bind {}: {e}", path.display()))?;
    let _ = std::fs::remove_file(&control_path);
    let control = UnixListener::bind(&control_path)
        .map_err(|e| format!("bind control {control_path}: {e}"))?;

    let fake = Arc::new(FakeSession::new());
    fake.connect().await.map_err(|e| e.to_string())?;
    tokio::spawn(serve_control(control, Arc::clone(&fake)));
    eprintln!(
        "rithmic-gateway-fake listening on unix://{} (control {control_path})",
        path.display()
    );

    let result = run_gateway(GatewayParams {
        listener,
        path,
        fingerprint: Fingerprint {
            user,
            system_name,
            url,
            env: env_name,
            account_id: std::env::var("RITHMIC_ACCOUNT_ID").unwrap_or_default(),
            fcm_id: std::env::var("RITHMIC_FCM_ID").unwrap_or_default(),
            ib_id: std::env::var("RITHMIC_IB_ID").unwrap_or_default(),
        },
        expected_auth_token: std::env::var("RITHMIC_GATEWAY_AUTH_TOKEN").unwrap_or_default(),
        idle_policy,
        gates: ParentGates::from_env(),
        session: SessionHandle::new(fake),
        timing: rithmic_gateway::server::pump::PumpTiming::default(),
    })
    .await;
    let _ = std::fs::remove_file(&control_path);
    result
}
