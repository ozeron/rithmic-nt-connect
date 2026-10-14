//! Unit tests for the session facade (no live network).

use rithmic_plants::{DirectSession, Error, PlantSet, Session, SessionConfig};

#[test]
fn config_rejects_incomplete_lucid_settings() {
    let err = SessionConfig::builder().user("alice").build().unwrap_err();
    assert!(err.to_string().contains("password"));

    let err = SessionConfig::builder().password("pw").build().unwrap_err();
    assert!(err.to_string().contains("user"));
}

#[tokio::test]
async fn disconnected_order_poll_errors_without_network() {
    // Only the non-I/O paths are unit-testable offline: place / load go through
    // `ensure_order_plant`, which would open a real connection.
    let cfg = SessionConfig::builder()
        .user("alice")
        .password("pw")
        .account_id("acct")
        .fcm_id("fcm")
        .ib_id("ib")
        .build()
        .unwrap();
    let session = DirectSession::new(cfg, PlantSet::EXECUTION);

    let err = session
        .poll_order_event(std::time::Duration::ZERO)
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::NotConnected { plant: "order" }),
        "unexpected poll_order_event error: {err}"
    );
}
