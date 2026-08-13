//! Unit tests for the Phase 2 session facade (no live network).

use rithmic_connect::{RithmicSession, SessionConfig};

#[test]
fn config_rejects_incomplete_lucid_settings() {
    let err = SessionConfig::builder().user("alice").build().unwrap_err();
    assert!(err.to_string().contains("password"));

    let err = SessionConfig::builder().password("pw").build().unwrap_err();
    assert!(err.to_string().contains("user"));
}

#[test]
fn session_api_has_public_order_methods() {
    let session_src = include_str!("../src/session.rs");
    for required in [
        "place_order",
        "cancel_order",
        "modify_order",
        "cancel_all_orders",
        "subscribe_order_updates",
    ] {
        assert!(
            session_src.contains(&format!("pub async fn {required}")),
            "session.rs must expose pub async fn {required}"
        );
    }
    assert!(
        session_src.contains("pub fn poll_order_event"),
        "session.rs must expose pub fn poll_order_event"
    );
    assert!(
        session_src.contains("RithmicOrderPlant"),
        "session.rs must reference RithmicOrderPlant"
    );
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
    assert!(err.to_string().contains("not connected"));
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
        )
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("not connected") || msg.contains("order plant"),
        "unexpected place_order error: {msg}"
    );

    let err = session.poll_order_event().unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("not connected") || msg.contains("order plant"),
        "unexpected poll_order_event error: {msg}"
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
