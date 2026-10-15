//! Contract and restore metadata coverage without protobuf.
use rithmic_host::rpc_table::{Kind, RPC_TABLE};
use std::collections::BTreeSet;
const BACKEND: &str = include_str!("../src/backend.rs");
const RECONNECT: &str = include_str!("../src/intents.rs");
fn trait_methods() -> BTreeSet<String> {
    let session = BACKEND
        .split("pub trait Session")
        .nth(1)
        .expect("Session trait");
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
fn every_session_trait_method_has_a_table_row() {
    let in_table: BTreeSet<String> = RPC_TABLE.iter().map(|r| r.method.to_string()).collect();
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
fn only_subscription_intents_carry_a_restore_mapping() {
    for row in RPC_TABLE {
        assert_eq!(
            row.kind == Kind::SubscriptionIntent,
            row.restore.is_some(),
            "{:?}: restore mapping must exist exactly for subscription intents",
            row.method
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
#[test]
fn table_method_keys_are_unique() {
    let mut methods = BTreeSet::new();
    for row in RPC_TABLE {
        assert!(
            methods.insert(row.method),
            "duplicate method: {}",
            row.method
        );
    }
}

use rithmic_host::fake::FakeVenue;
use rithmic_host::fanout::{FanoutHub, SubKey};
use rithmic_host::intents::{ReconnectController, TimeBarIntent};
use rithmic_host::pump::restore_intents;
use std::sync::Arc;
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
            row.method
        );
        assert!(
            fake.calls().iter().any(|c| c.starts_with(restore.call)),
            "{:?}: restore_intents never issued `{}`; calls: {:?}",
            row.method,
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
