//! Transport injection must retain authentication and outcome semantics.
mod common;

use common::*;
use rithmic_gateway::client::{GatewayConfig, RemoteSession};
use rithmic_host::{Error, Session};

#[tokio::test]
async fn an_owned_stream_round_trips_without_a_socket_or_flock() {
    let parent = FakeParent::start(Opts::default()).await;
    // Default flock attestation stays on: the supplied transport bypasses Unix
    // attachment, while its fingerprint and auth token still go on the wire.
    let cfg = GatewayConfig::new(&parent.user, SYSTEM, URL, "Live").with_auth_token(AUTH_TOKEN);
    let session = RemoteSession::over(cfg, parent.stream());
    session.connect().await.unwrap();
    session.subscribe(NQ, CME).await.unwrap();
    parent.push_last_trades(NQ, 1).await;
    assert!(session
        .poll_event(std::time::Duration::from_secs(2))
        .await
        .unwrap()
        .is_some());
    session.disconnect().await.unwrap();
    // No implicit Unix fallback or second attempt to use a consumed stream.
    let err = session.connect().await.unwrap_err();
    assert!(
        matches!(err, Error::Transport(ref msg) if msg.contains("supplied stream consumed")),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_generic_stream_cannot_claim_same_uid_empty_token_trust() {
    let parent = FakeParent::start(Opts::default()).await;
    for (token, code) in [("", "auth_required"), ("wrong-token", "auth_failed")] {
        let cfg = GatewayConfig::new(&parent.user, SYSTEM, URL, "Live").with_auth_token(token);
        let session = RemoteSession::over(cfg, parent.stream());
        let err = session.connect().await.unwrap_err();
        assert!(
            matches!(err, Error::Gateway { code: ref actual, .. } if actual == code),
            "{err:?}"
        );
    }
    assert_eq!(parent.calls().await, ["connect"]);
}
