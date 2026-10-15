//! Unix accept and kernel peer identity; framing and serving are stream-independent.

use super::{serve_connection_with_peer, GatewayState};
use crate::idle_exit::IdleExitPolicy;
use std::sync::Arc;
use tokio::net::{UnixListener, UnixStream};

/// Accept connections until idle-exit fires (or forever when policy is Never).
pub async fn listen_unix(listener: UnixListener, state: Arc<GatewayState>) -> std::io::Result<()> {
    loop {
        tokio::select! {
            biased;
            _ = state.idle.wait_until_should_exit() => {
                match state.idle.policy() {
                    IdleExitPolicy::After(grace) => {
                        eprintln!(
                            "rithmic-gateway: idle-exit after {}s with 0 peers",
                            grace.as_secs()
                        );
                    }
                    IdleExitPolicy::Never => {
                        eprintln!("rithmic-gateway: idle-exit");
                    }
                }
                return Ok(());
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let same_uid = same_uid_peer(&stream);
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(e) = serve_connection_with_peer(stream, state, same_uid).await {
                        eprintln!("gateway client error: {e}");
                    }
                });
            }
        }
    }
}

/// Same-UID check for the v1 local-unix empty-token trust policy (R15 /
/// KTD16): unix `SO_PEERCRED` is a kernel fact, not client-supplied data.
fn same_uid_peer(stream: &UnixStream) -> bool {
    match stream.peer_cred() {
        Ok(cred) => {
            // SAFETY: getuid() has no preconditions and never fails.
            let our_uid = unsafe { libc::getuid() };
            cred.uid() == our_uid
        }
        Err(_) => false,
    }
}
