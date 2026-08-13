//! Unit tests for the Phase 2 session facade (no live network).

use rithmic_nt_connect::{Error, RithmicSession, SessionConfig};

#[test]
fn config_rejects_incomplete_lucid_settings() {
    let err = SessionConfig::builder().user("alice").build().unwrap_err();
    assert!(err.to_string().contains("password"));

    let err = SessionConfig::builder().password("pw").build().unwrap_err();
    assert!(err.to_string().contains("user"));
}

#[test]
fn disconnected_session_methods_error_without_network() {
    let cfg = SessionConfig::builder()
        .user("alice")
        .password("pw")
        .build()
        .unwrap();
    let mut session = RithmicSession::new(cfg);
    let err = session.poll_event().unwrap_err();
    assert!(matches!(err, Error::NotConnected { plant: "ticker" }));
}

#[tokio::test]
async fn disconnected_order_methods_error_without_network() {
    let cfg = SessionConfig::builder()
        .user("alice")
        .password("pw")
        .account_id("acct")
        .fcm_id("fcm")
        .ib_id("ib")
        .build()
        .unwrap();
    let mut session = RithmicSession::new(cfg);

    let err = session
        .place_order(
            "ESM6",
            "CME",
            "BUY",
            "MARKET",
            1,
            "tag-1",
            None,
            None,
            "DAY",
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::NotConnected { plant: "ticker" | "order" }),
        "unexpected place_order error: {err}"
    );

    let err = session.poll_order_event().unwrap_err();
    assert!(
        matches!(err, Error::NotConnected { plant: "order" }),
        "unexpected poll_order_event error: {err}"
    );
}

#[test]
fn debug_redacts_password() {
    let cfg = SessionConfig::builder()
        .user("alice")
        .password("super-secret")
        .build()
        .unwrap();
    let session = RithmicSession::new(cfg);
    let text = format!("{session:?}");
    assert!(text.contains("***"));
    assert!(!text.contains("super-secret"));
}
