//! Parent trading / cancel_all gates + no-session honesty.

use rithmic_gateway::pb::frame::Body;
use rithmic_gateway::pb::{
    CancelAllOrdersRequest, LoadOrdersRequest, PlaceBracketOrderRequest, PlaceOrderRequest,
    SubscribeBracketUpdatesRequest, SubscribeOrderUpdatesRequest,
};
use rithmic_gateway::server::{gate_rpc_for_test, rpc_sequence_with_gates};
use rithmic_gateway::subscriptions::ParentGates;

fn trading_on() -> ParentGates {
    ParentGates {
        trading_enabled: true,
        cancel_all_enabled: false,
    }
}

fn assert_error_code(body: Body, code: &str) {
    match body {
        Body::Error(e) => assert_eq!(e.code, code),
        other => panic!("expected Error({code}), got {other:?}"),
    }
}

#[test]
fn place_denied_when_trading_disabled() {
    assert_error_code(
        gate_rpc_for_test(
            &ParentGates {
                trading_enabled: false,
                cancel_all_enabled: false,
            },
            Body::PlaceOrder(PlaceOrderRequest {
                symbol: "NQ".into(),
                exchange: "CME".into(),
                side: "BUY".into(),
                price_type: "MARKET".into(),
                quantity: 1,
                ..Default::default()
            }),
        ),
        "trading_disabled",
    );
}

#[test]
fn place_bracket_denied_when_trading_disabled() {
    assert_error_code(
        gate_rpc_for_test(
            &ParentGates {
                trading_enabled: false,
                cancel_all_enabled: false,
            },
            Body::PlaceBracketOrder(PlaceBracketOrderRequest {
                symbol: "NQ".into(),
                exchange: "CME".into(),
                side: "BUY".into(),
                price_type: "MARKET".into(),
                quantity: 1,
                localid: "t1".into(),
                stop_ticks: Some(40),
                ..Default::default()
            }),
        ),
        "trading_disabled",
    );
}

#[test]
fn cancel_all_denied_by_default() {
    assert_error_code(
        gate_rpc_for_test(&ParentGates::default(), Body::CancelAllOrders(CancelAllOrdersRequest {})),
        "cancel_all_denied",
    );
}

#[test]
fn place_errors_when_no_session() {
    assert_error_code(
        gate_rpc_for_test(
            &trading_on(),
            Body::PlaceOrder(PlaceOrderRequest {
                symbol: "NQ".into(),
                exchange: "CME".into(),
                side: "BUY".into(),
                price_type: "MARKET".into(),
                quantity: 1,
                ..Default::default()
            }),
        ),
        "no_session",
    );
}

#[test]
fn place_bracket_errors_when_no_session() {
    assert_error_code(
        gate_rpc_for_test(
            &trading_on(),
            Body::PlaceBracketOrder(PlaceBracketOrderRequest {
                symbol: "NQ".into(),
                exchange: "CME".into(),
                side: "BUY".into(),
                price_type: "MARKET".into(),
                quantity: 1,
                localid: "t1".into(),
                stop_ticks: Some(40),
                ..Default::default()
            }),
        ),
        "no_session",
    );
}

#[test]
fn cancel_all_errors_when_enabled_but_no_session() {
    assert_error_code(
        gate_rpc_for_test(
            &ParentGates {
                trading_enabled: false,
                cancel_all_enabled: true,
            },
            Body::CancelAllOrders(CancelAllOrdersRequest {}),
        ),
        "no_session",
    );
}

#[test]
fn subscribe_bracket_denied_when_trading_disabled() {
    assert_error_code(
        gate_rpc_for_test(
            &ParentGates {
                trading_enabled: false,
                cancel_all_enabled: false,
            },
            Body::SubscribeBracketUpdates(SubscribeBracketUpdatesRequest {}),
        ),
        "trading_disabled",
    );
}

#[test]
fn load_orders_denied_when_trading_disabled() {
    assert_error_code(
        gate_rpc_for_test(
            &ParentGates {
                trading_enabled: false,
                cancel_all_enabled: false,
            },
            Body::LoadOrders(LoadOrdersRequest {
                start_time_sec: 0,
                end_time_sec: 1,
            }),
        ),
        "trading_disabled",
    );
}

#[test]
fn load_orders_errors_when_no_session() {
    let (bodies, plan) = rpc_sequence_with_gates(
        trading_on(),
        vec![Body::LoadOrders(LoadOrdersRequest {
            start_time_sec: 0,
            end_time_sec: 1,
        })],
    );
    assert_error_code(bodies.into_iter().next().expect("body"), "no_session");
    assert!(!plan.order, "failed load_orders must not leave order intent");
}

#[test]
fn subscribe_order_errors_when_no_session() {
    let (bodies, plan) = rpc_sequence_with_gates(
        trading_on(),
        vec![Body::SubscribeOrderUpdates(SubscribeOrderUpdatesRequest {})],
    );
    assert_error_code(bodies.into_iter().next().expect("body"), "no_session");
    assert!(!plan.order && !plan.brackets, "failed subscribe must not leave intent");
}

#[test]
fn subscribe_bracket_errors_when_no_session() {
    let (bodies, plan) = rpc_sequence_with_gates(
        trading_on(),
        vec![Body::SubscribeBracketUpdates(SubscribeBracketUpdatesRequest {})],
    );
    assert_error_code(bodies.into_iter().next().expect("body"), "no_session");
    assert!(
        !plan.order && !plan.brackets,
        "failed bracket subscribe must not leave intent, got order={} brackets={}",
        plan.order,
        plan.brackets
    );
}
