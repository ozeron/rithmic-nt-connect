//! Integration tests: parent trading / cancel_all gates.

use rithmic_gateway::pb::frame::Body;
use rithmic_gateway::pb::{CancelAllOrdersRequest, PlaceOrderRequest};
use rithmic_gateway::server::gate_rpc_for_test;
use rithmic_gateway::subscriptions::ParentGates;

#[test]
fn place_denied_when_trading_disabled() {
    let gates = ParentGates {
        trading_enabled: false,
        cancel_all_enabled: false,
    };
    let body = gate_rpc_for_test(
        &gates,
        Body::PlaceOrder(PlaceOrderRequest {
            symbol: "NQ".into(),
            exchange: "CME".into(),
            side: "BUY".into(),
            price_type: "MARKET".into(),
            quantity: 1,
            ..Default::default()
        }),
    );
    match body {
        Body::Error(e) => assert_eq!(e.code, "trading_disabled"),
        other => panic!("expected Error, got {other:?}"),
    }
}

#[test]
fn cancel_all_denied_by_default() {
    let gates = ParentGates::default();
    let body = gate_rpc_for_test(&gates, Body::CancelAllOrders(CancelAllOrdersRequest {}));
    match body {
        Body::Error(e) => assert_eq!(e.code, "cancel_all_denied"),
        other => panic!("expected Error, got {other:?}"),
    }
}

#[test]
fn place_ack_when_trading_enabled_no_session_is_gate_only() {
    // session: None → Ack means the trading gate allowed the RPC, not that an
    // order reached the venue. Venue place / venue_unknown need a live session.
    let gates = ParentGates {
        trading_enabled: true,
        cancel_all_enabled: false,
    };
    let body = gate_rpc_for_test(
        &gates,
        Body::PlaceOrder(PlaceOrderRequest {
            symbol: "NQ".into(),
            exchange: "CME".into(),
            side: "BUY".into(),
            price_type: "MARKET".into(),
            quantity: 1,
            ..Default::default()
        }),
    );
    assert!(
        matches!(body, Body::Ack(_)),
        "gate-allowed place with no session must Ack (no-op), got {body:?}"
    );
}

#[test]
fn cancel_all_ack_when_enabled_no_session() {
    let gates = ParentGates {
        trading_enabled: false,
        cancel_all_enabled: true,
    };
    let body = gate_rpc_for_test(&gates, Body::CancelAllOrders(CancelAllOrdersRequest {}));
    assert!(matches!(body, Body::Ack(_)));
}

