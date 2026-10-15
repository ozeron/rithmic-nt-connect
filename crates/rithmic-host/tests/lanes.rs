//! Lane policy belongs to the host; socket ordering is tested by gateway.
use rithmic_host::rpc_table::{Lane, RPC_TABLE};
#[test]
fn slow_reads_and_ordered_intents_keep_their_lanes() {
    let slow: Vec<_> = RPC_TABLE
        .iter()
        .filter(|r| r.lane == Lane::Slow)
        .map(|r| r.method)
        .collect();
    assert_eq!(
        slow,
        [
            // Reads that wait on the plant session.
            "get_front_month",
            "get_reference_data",
            "resolved_account",
            "load_ticks",
            "load_time_bars",
            "probe_time_bars",
            "load_orders",
            "load_product_rms_info",
            "load_account_rms_info",
            "load_fill_history",
            "load_order_history_dates",
            "load_order_history",
        ]
    );
    // Subscription intents mutate the connection's context, so they must stay on
    // the ordered command lane.
    for row in RPC_TABLE {
        if matches!(
            row.kind,
            rithmic_host::rpc_table::Kind::SubscriptionIntent
                | rithmic_host::rpc_table::Kind::IntentRelease
        ) {
            assert_eq!(row.lane, Lane::Command, "{:?}", row.method);
        }
    }
}
