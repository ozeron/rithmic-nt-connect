//! Unit tests for the Phase 2 session facade (no live network).

use rithmic_plants::{Error, PlantSet, RithmicSession, SessionConfig};

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
            "ESM6", "CME", "BUY", "MARKET", 1, "tag-1", None, None, "DAY", None, None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::NotConnected {
                plant: "ticker" | "order"
            }
        ),
        "unexpected place_order error: {err}"
    );

    let err = session
        .place_bracket_order(
            "ESM6",
            "CME",
            "BUY",
            "MARKET",
            1,
            "bracket-1",
            None,
            None,
            "DAY",
            Some(40),
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::NotConnected {
                plant: "ticker" | "order"
            }
        ),
        "unexpected place_bracket_order error: {err}"
    );

    let err = session.poll_order_event().unwrap_err();
    assert!(
        matches!(err, Error::NotConnected { plant: "order" }),
        "unexpected poll_order_event error: {err}"
    );

    // load_orders is a bounded drain of the order plant (no longer the
    // unconditional ReconciliationUnavailable stub), so a disconnected
    // session must fail on the missing ticker/order plant, not on recon.
    let err = session.load_orders(0, 0).await.unwrap_err();
    assert!(
        matches!(
            err,
            Error::NotConnected {
                plant: "ticker" | "order"
            }
        ),
        "unexpected load_orders error: {err}"
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

#[test]
fn default_session_is_market_data_plants() {
    let cfg = SessionConfig::builder()
        .user("alice")
        .password("pw")
        .build()
        .unwrap();
    let session = RithmicSession::new(cfg);
    assert_eq!(session.plants(), PlantSet::MARKET_DATA);
}

#[test]
fn execution_plants_request_pnl() {
    let cfg = SessionConfig::builder()
        .user("alice")
        .password("pw")
        .build()
        .unwrap();
    let session = RithmicSession::with_plants(cfg, PlantSet::EXECUTION);
    assert!(session.plants().pnl);
}
