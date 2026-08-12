//! Unit tests for the Phase 1 session facade (no live network).

use rithmic_connect::{RithmicSession, SessionConfig};

#[test]
fn config_rejects_incomplete_lucid_settings() {
    let err = SessionConfig::builder().user("alice").build().unwrap_err();
    assert!(err.to_string().contains("password"));

    let err = SessionConfig::builder().password("pw").build().unwrap_err();
    assert!(err.to_string().contains("user"));
}

#[test]
fn session_api_has_no_public_order_methods() {
    let session_src = include_str!("../src/session.rs");
    let lib_src = include_str!("../src/lib.rs");
    for forbidden in [
        "place_order",
        "cancel_order",
        "modify_order",
        "submit_order",
        "new_order",
    ] {
        assert!(
            !session_src.contains(&format!("pub async fn {forbidden}")),
            "session.rs must not expose pub async fn {forbidden}"
        );
        assert!(
            !session_src.contains(&format!("pub fn {forbidden}")),
            "session.rs must not expose pub fn {forbidden}"
        );
    }
    assert!(
        !session_src.contains("RithmicOrderPlant"),
        "session.rs must not reference RithmicOrderPlant"
    );
    assert!(
        !lib_src.contains("RithmicOrderPlant"),
        "lib.rs must not re-export RithmicOrderPlant"
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
