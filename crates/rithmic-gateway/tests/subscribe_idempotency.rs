//! Subscribe idempotency + typed restore plan from RPC sequence.

use rithmic_gateway::pb::frame::Body;
use rithmic_gateway::pb::{
    SubscribeBookRequest, SubscribeRequest, SubscribeTimeBarsRequest, UnsubscribeBookRequest,
    UnsubscribeTimeBarsRequest,
};
use rithmic_gateway::server::rpc_sequence_for_test;

#[test]
fn book_then_subscribe_keeps_ticker_intent() {
    let (bodies, plan) = rpc_sequence_for_test(vec![
        Body::SubscribeBook(SubscribeBookRequest {
            symbol: "NQ".into(),
            exchange: "CME".into(),
        }),
        Body::Subscribe(SubscribeRequest {
            symbol: "NQ".into(),
            exchange: "CME".into(),
        }),
        Body::Subscribe(SubscribeRequest {
            symbol: "NQ".into(),
            exchange: "CME".into(),
        }),
    ]);
    assert!(bodies.iter().all(|b| matches!(b, Body::Ack(_))));
    assert_eq!(plan.ticker.len(), 1);
    assert_eq!(plan.book.len(), 1);
    assert_eq!(plan.ticker[0].symbol, "NQ");
}

#[test]
fn unsubscribe_book_keeps_ticker_intent() {
    let (bodies, plan) = rpc_sequence_for_test(vec![
        Body::Subscribe(SubscribeRequest {
            symbol: "NQ".into(),
            exchange: "CME".into(),
        }),
        Body::SubscribeBook(SubscribeBookRequest {
            symbol: "NQ".into(),
            exchange: "CME".into(),
        }),
        Body::UnsubscribeBook(UnsubscribeBookRequest {
            symbol: "NQ".into(),
            exchange: "CME".into(),
        }),
    ]);
    assert!(bodies.iter().all(|b| matches!(b, Body::Ack(_))));
    assert_eq!(plan.ticker.len(), 1);
    assert!(plan.book.is_empty());
}

#[test]
fn unsubscribe_time_bars_keeps_ticker_intent() {
    let (bodies, plan) = rpc_sequence_for_test(vec![
        Body::Subscribe(SubscribeRequest {
            symbol: "NQ".into(),
            exchange: "CME".into(),
        }),
        Body::SubscribeTimeBars(SubscribeTimeBarsRequest {
            symbol: "NQ".into(),
            exchange: "CME".into(),
            bar_type: 2,
            period: 1,
        }),
        Body::UnsubscribeTimeBars(UnsubscribeTimeBarsRequest {
            symbol: "NQ".into(),
            exchange: "CME".into(),
            bar_type: 2,
            period: 1,
        }),
    ]);
    assert!(bodies.iter().all(|b| matches!(b, Body::Ack(_))));
    assert_eq!(plan.ticker.len(), 1);
    assert!(plan.time_bars.is_empty());
}
