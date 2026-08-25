//! RC2.3: history Load* refused while live ticker intents are active.
//!
//! Admission shares ``md_history_gate`` with MD subscribe (note + venue join)
//! so a concurrent subscribe cannot race between the refuse check and
//! session-lock acquisition.

use rithmic_gateway::pb::frame::Body;
use rithmic_gateway::pb::{LoadTicksRequest, LoadTimeBarsRequest, ProbeTimeBarsRequest};
use rithmic_gateway::server::history_rpc_with_live_ticker_intent_for_test;

fn assert_history_denied(body: Body) {
    match body {
        Body::Error(e) => {
            assert_eq!(e.code, "history_denied_live_md");
            assert!(
                e.message.contains("live XOR") || e.message.contains("refused"),
                "message should explain live XOR: {}",
                e.message
            );
        }
        other => panic!("expected history_denied_live_md Error, got {other:?}"),
    }
}

#[test]
fn load_ticks_denied_when_ticker_intent_live() {
    assert_history_denied(history_rpc_with_live_ticker_intent_for_test(
        Body::LoadTicks(LoadTicksRequest {
            symbol: "ZN".into(),
            exchange: "CBOT".into(),
            start_time_sec: 1,
            end_time_sec: 2,
        }),
    ));
}

#[test]
fn load_time_bars_denied_when_ticker_intent_live() {
    assert_history_denied(history_rpc_with_live_ticker_intent_for_test(
        Body::LoadTimeBars(LoadTimeBarsRequest {
            symbol: "ZN".into(),
            exchange: "CBOT".into(),
            bar_type: 2,
            period: 1,
            start_time_sec: 1,
            end_time_sec: 2,
        }),
    ));
}

#[test]
fn probe_time_bars_denied_when_ticker_intent_live() {
    assert_history_denied(history_rpc_with_live_ticker_intent_for_test(
        Body::ProbeTimeBars(ProbeTimeBarsRequest {
            symbol: "ZN".into(),
            exchange: "CBOT".into(),
            bar_type: 2,
            period: 1,
            start_time_sec: 1,
            end_time_sec: 2,
        }),
    ));
}
