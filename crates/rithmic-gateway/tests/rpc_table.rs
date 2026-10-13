//! The RPC table is the single classification of every gateway request and
//! `Session` trait method. These tests fail when the proto, the trait, the
//! restore plan, or the gates drift from it.

use std::collections::BTreeSet;
use std::sync::Arc;

use rithmic_gateway::pb::frame::Body;
use rithmic_gateway::reconnect::{ReconnectController, TimeBarIntent};
use rithmic_gateway::rpc_table::{by_body, Gate, Kind, RPC_TABLE};
use rithmic_gateway::server::{gate_rpc_for_test, pump::restore_intents};
use rithmic_gateway::subscriptions::{FanoutHub, ParentGates, SubKey};
use rithmic_plants::fake::FakeSession;

const PROTO: &str = include_str!("../../../proto/rithmic_gateway/v1/session.proto");
const BACKEND: &str = include_str!("../../rithmic-plants/src/backend.rs");
const RECONNECT: &str = include_str!("../src/reconnect.rs");

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

/// Method names declared in `pub trait Session` (not `HistoryReplay`).
fn trait_methods() -> BTreeSet<String> {
    let session = BACKEND
        .split("pub trait Session")
        .nth(1)
        .expect("Session trait")
        .split("pub trait HistoryReplay")
        .next()
        .expect("end of Session");
    session
        .lines()
        .filter_map(|l| l.trim().strip_prefix("async fn "))
        .map(|rest| rest.split('(').next().expect("name").to_string())
        .collect()
}

fn restore_plan_fields() -> BTreeSet<String> {
    let plan = RECONNECT
        .split("pub struct RestorePlan {")
        .nth(1)
        .expect("RestorePlan")
        .split('}')
        .next()
        .expect("end of struct");
    plan.lines()
        .filter_map(|l| l.trim().strip_prefix("pub "))
        .map(|rest| rest.split(':').next().expect("field").to_string())
        .collect()
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
fn every_session_trait_method_has_a_table_row() {
    let in_table: BTreeSet<String> = RPC_TABLE
        .iter()
        .filter_map(|r| r.method.map(str::to_string))
        .collect();
    let in_trait = trait_methods();
    assert!(!in_trait.is_empty(), "failed to parse the Session trait");
    assert_eq!(
        in_trait.difference(&in_table).collect::<Vec<_>>(),
        Vec::<&String>::new(),
        "Session methods missing from RPC_TABLE"
    );
    assert_eq!(
        in_table.difference(&in_trait).collect::<Vec<_>>(),
        Vec::<&String>::new(),
        "RPC_TABLE methods that are not on the Session trait"
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

#[test]
fn only_subscription_intents_carry_a_restore_mapping() {
    for row in RPC_TABLE {
        assert_eq!(
            row.kind == Kind::SubscriptionIntent,
            row.restore.is_some(),
            "{:?}: restore mapping must exist exactly for subscription intents",
            row.body.or(row.method)
        );
    }
}

#[test]
fn intent_rows_cover_exactly_the_restore_plan_fields() {
    let mapped: BTreeSet<String> = RPC_TABLE
        .iter()
        .filter_map(|r| r.restore.map(|x| x.plan_field.to_string()))
        .collect();
    let plan = restore_plan_fields();
    assert!(!plan.is_empty(), "failed to parse RestorePlan");
    assert_eq!(
        plan, mapped,
        "every subscription intent needs a RestorePlan field and vice versa"
    );
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

        let fake = FakeSession::new();
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
    let fake = FakeSession::new();
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
