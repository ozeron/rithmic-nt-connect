//! The RPC table is the single classification of every gateway request and
//! `Session` trait method. These tests fail when the proto, the trait, the
//! restore plan, or the gates drift from it.

use std::collections::BTreeSet;
use std::sync::Arc;

use rithmic_gateway::pb::frame::Body;
use rithmic_gateway::reconnect::{ReconnectController, TimeBarIntent};
use rithmic_gateway::rpc_table::{by_body, Gate, RPC_TABLE};
use rithmic_gateway::server::{gate_rpc_for_test, pump::restore_intents};
use rithmic_gateway::subscriptions::{FanoutHub, ParentGates, SubKey};
use rithmic_host::fake::FakeVenue;

const PROTO: &str = include_str!("../../../proto/rithmic_gateway/v1/session.proto");

/// Request fields of the `Frame` oneof: field numbers 2..=49 except `ready` (3).
/// Responses start at 50, pushed events at 90.
fn proto_request_fields() -> BTreeSet<String> {
    let frame = PROTO
        .split("message Frame {")
        .nth(1)
        .expect("Frame message");
    let body = frame.split("oneof body {").nth(1).expect("oneof body");
    let body = body.split('}').next().expect("end of oneof");
    let mut out = BTreeSet::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("//") {
            continue;
        }
        // `TypeName field_name = N;`
        let mut parts = line.trim_end_matches(';').split_whitespace();
        let (Some(_ty), Some(name), Some("="), Some(num)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        let num: u32 = num.parse().expect("field number");
        if (2..50).contains(&num) && name != "ready" {
            out.insert(name.to_string());
        }
    }
    out
}

#[test]
fn every_proto_request_has_a_table_row() {
    let in_table: BTreeSet<String> = RPC_TABLE
        .iter()
        .filter_map(|r| r.body.map(str::to_string))
        .collect();
    let in_proto = proto_request_fields();
    assert_eq!(
        in_proto.difference(&in_table).collect::<Vec<_>>(),
        Vec::<&String>::new(),
        "session.proto request variants missing from RPC_TABLE"
    );
    assert_eq!(
        in_table.difference(&in_proto).collect::<Vec<_>>(),
        Vec::<&String>::new(),
        "RPC_TABLE bodies that are not session.proto request variants"
    );
}

#[test]
fn table_keys_are_unique() {
    let mut bodies = BTreeSet::new();
    let mut methods = BTreeSet::new();
    for row in RPC_TABLE {
        if let Some(b) = row.body {
            assert!(bodies.insert(b), "duplicate body row: {b}");
        }
        if let Some(m) = row.method {
            assert!(methods.insert(m), "duplicate method row: {m}");
        }
    }
}

/// Note the intent a table row restores, by `RestorePlan` field. An unknown
/// field fails the test so a new intent cannot ship without a note helper.
async fn note_intent(ctl: &ReconnectController, plan_field: &str) {
    let key = SubKey {
        symbol: "NQH6".into(),
        exchange: "CME".into(),
    };
    match plan_field {
        "ticker" => {
            ctl.note_ticker(key).await;
        }
        "book" => {
            ctl.note_book(key).await;
        }
        "time_bars" => {
            ctl.note_time_bar(TimeBarIntent {
                symbol: "NQH6".into(),
                exchange: "CME".into(),
                bar_type: 2,
                period: 1,
            })
            .await;
        }
        "pnl" => {
            ctl.note_pnl().await;
        }
        "order" => {
            ctl.note_order().await;
        }
        "brackets" => {
            ctl.note_brackets().await;
        }
        other => panic!("no note helper for RestorePlan field {other:?}"),
    }
}

#[tokio::test]
async fn every_subscription_intent_is_reissued_by_restore_intents() {
    for row in RPC_TABLE.iter().filter(|r| r.restore.is_some()) {
        let restore = row.restore.expect("restore");
        let ctl = ReconnectController::new(Arc::new(FanoutHub::new(8)));
        note_intent(&ctl, restore.plan_field).await;

        let fake = FakeVenue::new();
        let (restored, attempted) = restore_intents(&fake, &ctl).await;

        assert_eq!(
            (restored, attempted),
            (1, 1),
            "{:?}: restore must attempt and succeed",
            row.body
        );
        assert!(
            fake.calls().iter().any(|c| c.starts_with(restore.call)),
            "{:?}: restore_intents never issued `{}`; calls: {:?}",
            row.body,
            restore.call,
            fake.calls()
        );
    }
}

#[tokio::test]
async fn one_shot_commands_are_never_restored() {
    // Nothing a one-shot does notes an intent, so a plan with only one-shot
    // activity restores nothing.
    let ctl = ReconnectController::new(Arc::new(FanoutHub::new(8)));
    let fake = FakeVenue::new();
    let (restored, attempted) = restore_intents(&fake, &ctl).await;
    assert_eq!((restored, attempted), (0, 0));
    assert!(fake.calls().is_empty());
}

fn code_of(body: Body) -> String {
    match body {
        Body::Error(e) => e.code,
        _ => String::new(),
    }
}

fn gates(trading: bool, cancel_all: bool) -> ParentGates {
    ParentGates {
        trading_enabled: trading,
        cancel_all_enabled: cancel_all,
    }
}

#[test]
fn trading_gated_rows_are_denied_with_trading_off_even_if_cancel_all_is_on() {
    let rows: Vec<_> = RPC_TABLE
        .iter()
        .filter(|r| r.gate == Gate::Trading)
        .collect();
    assert!(!rows.is_empty());
    for row in rows {
        let body = (row.make_body.expect("body"))();
        let code = code_of(gate_rpc_for_test(&gates(false, true), body));
        assert_eq!(code, "trading_disabled", "{:?}", row.body);
    }
}

#[test]
fn trading_on_never_reports_a_trading_denial() {
    for row in RPC_TABLE.iter().filter(|r| r.gate == Gate::Trading) {
        let body = (row.make_body.expect("body"))();
        let code = code_of(gate_rpc_for_test(&gates(true, false), body));
        assert_ne!(code, "trading_disabled", "{:?}", row.body);
    }
}

#[test]
fn cancel_all_has_its_own_gate_independent_of_trading() {
    let row = RPC_TABLE
        .iter()
        .find(|r| r.gate == Gate::CancelAll)
        .expect("cancel-all row");
    assert_eq!(
        RPC_TABLE
            .iter()
            .filter(|r| r.gate == Gate::CancelAll)
            .count(),
        1
    );
    let make = || (row.make_body.expect("body"))();
    // Flag off: denied whether or not trading is on.
    for trading in [false, true] {
        let code = code_of(gate_rpc_for_test(&gates(trading, false), make()));
        assert_eq!(code, "cancel_all_denied", "trading={trading}");
    }
    // Flag on: permitted whether or not trading is on (no session here, so the
    // call reaches the no-session answer, never a gate denial).
    for trading in [false, true] {
        let code = code_of(gate_rpc_for_test(&gates(trading, true), make()));
        assert_ne!(code, "cancel_all_denied", "trading={trading}");
        assert_ne!(code, "trading_disabled", "trading={trading}");
    }
}

#[test]
fn ungated_rows_are_never_denied_by_a_gate() {
    for row in RPC_TABLE.iter().filter(|r| r.gate == Gate::None) {
        let Some(make) = row.make_body else { continue };
        let code = code_of(gate_rpc_for_test(&gates(false, false), make()));
        assert_ne!(code, "trading_disabled", "{:?}", row.body);
        assert_ne!(code, "cancel_all_denied", "{:?}", row.body);
    }
}

#[test]
fn every_row_with_a_body_dispatches_to_a_matching_proto_field() {
    // `make_body` and `body` must describe the same request, or the gate tests
    // above would be exercising the wrong RPC.
    for row in RPC_TABLE {
        let (Some(body), Some(make)) = (row.body, row.make_body) else {
            continue;
        };
        let debug = format!("{:?}", make());
        let camel: String = body
            .split('_')
            .map(|w| {
                let mut c = w.chars();
                c.next()
                    .map(|f| f.to_uppercase().chain(c).collect::<String>())
                    .unwrap_or_default()
            })
            .collect();
        assert!(
            debug.starts_with(&camel),
            "row {body}: make_body built {debug}"
        );
        assert!(by_body(body).is_some());
    }
}
