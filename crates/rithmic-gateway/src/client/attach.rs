//! Attach-only dialing: connect to an explicitly started parent or fail.
//!
//! The client never starts a parent. A held credential flock means someone
//! else's parent is coming up, so the client waits (bounded) for its socket;
//! otherwise it reports `parent_not_running` with the listen path and the
//! launcher command.

use std::path::Path;
use std::time::Duration;

use rithmic_plants::{Error, Result};
use tokio::net::UnixStream;
use tokio::time::Instant;

use super::GatewayConfig;
use crate::singleton::flock_held;

const POLL_INTERVAL: Duration = Duration::from_millis(50);

fn gateway_err(code: &str, message: impl Into<String>) -> Error {
    Error::Gateway {
        code: code.into(),
        message: message.into(),
    }
}

pub(super) fn parent_flock_held(cfg: &GatewayConfig) -> bool {
    flock_held(&cfg.user, &cfg.system_name, &cfg.url, &cfg.env)
}

async fn socket_accepts(path: &Path) -> bool {
    UnixStream::connect(path).await.is_ok()
}

/// Wait for a flock-held parent's socket to accept.
///
/// `parent_flock_held_no_socket` if the flock is released first;
/// `listen_path_mismatch` if the socket never accepts within the timeout.
async fn wait_for_parent_socket(cfg: &GatewayConfig, path: &Path) -> Result<()> {
    let deadline = Instant::now() + cfg.attach_timeout;
    while Instant::now() < deadline {
        if !parent_flock_held(cfg) {
            return Err(gateway_err(
                "parent_flock_held_no_socket",
                "credential flock not held while waiting for existing parent — no gateway is starting",
            ));
        }
        if socket_accepts(path).await {
            return Ok(());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    Err(gateway_err(
        "listen_path_mismatch",
        format!(
            "timed out waiting for existing gateway parent at {} (flock held — check \
             RITHMIC_GATEWAY_LISTEN matches the running parent)",
            path.display()
        ),
    ))
}

/// Dial the parent socket, never starting a process.
pub(super) async fn attach(cfg: &GatewayConfig) -> Result<UnixStream> {
    let path = cfg.socket_path();
    if let Ok(stream) = UnixStream::connect(&path).await {
        return Ok(stream);
    }
    let outcome = if parent_flock_held(cfg) {
        wait_for_parent_socket(cfg, path).await
    } else {
        Err(gateway_err(
            "parent_not_running",
            format!(
                "no gateway parent is listening at {} and none holds the credential flock; \
                 start it with `python -m rithmic_gateway` (the parent needs RITHMIC_PASSWORD, \
                 clients do not)",
                path.display()
            ),
        ))
    };
    if let Err(err) = outcome {
        // A parent may have bound its socket between the dial and the flock check.
        return match UnixStream::connect(&path).await {
            Ok(stream) => Ok(stream),
            Err(_) => Err(err),
        };
    }
    UnixStream::connect(&path).await.map_err(|e| {
        gateway_err(
            "listen_path_mismatch",
            format!(
                "credential flock held but dial failed after wait ({e}) — check \
                 RITHMIC_GATEWAY_LISTEN matches the running parent"
            ),
        )
    })
}
