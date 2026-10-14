//! pb → DTO is the exact inverse of DTO → pb.
//!
//! Each message is populated in every field with a non-default value, so a field
//! dropped in either direction breaks equality. Combined with the fake-plant
//! end-to-end test (`client_scenarios::events_arrive_equal_to_what_the_plant_emitted`)
//! this pins that a scripted event stream yields the same records through
//! `DirectSession`'s plant source and through `GatewaySession`.

use rithmic_gateway::client::convert::{self as from, Channel};
use rithmic_gateway::convert as to;
use rithmic_gateway::pb;
use rithmic_plants::dto::PlantEvent;

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

fn last_trade() -> pb::LastTrade {
    pb::LastTrade {
        symbol: s("NQH6"),
        exchange: s("CME"),
        trade_price: Some(18_250.25),
        trade_size: Some(3),
        aggressor: Some(2),
        ssboe: Some(1_700_000_001),
        usecs: Some(123_456),
        ts_event_ns: Some(1_700_000_001_123_456_000),
        is_snapshot: Some(true),
    }
}

fn bbo() -> pb::Bbo {
    pb::Bbo {
        symbol: s("NQH6"),
        exchange: s("CME"),
        bid_price: Some(18_250.0),
        bid_size: Some(5),
        ask_price: Some(18_250.25),
        ask_size: Some(7),
        ssboe: Some(1_700_000_002),
        usecs: Some(2),
        ts_event_ns: Some(1_700_000_002_000_002_000),
        is_snapshot: Some(true),
    }
}

fn order_book() -> pb::OrderBook {
    pb::OrderBook {
        symbol: s("NQH6"),
        exchange: s("CME"),
        update_type: Some(3),
        bid_price: vec![18_250.0, 18_249.75],
        bid_size: vec![5, 9],
        ask_price: vec![18_250.25, 18_250.5],
        ask_size: vec![7, 11],
        ssboe: Some(1_700_000_003),
        usecs: Some(3),
        ts_event_ns: Some(1_700_000_003_000_003_000),
    }
}

fn account_pnl() -> pb::AccountPnl {
    pb::AccountPnl {
        account_id: s("ACCT"),
        fcm_id: s("FCM"),
        ib_id: s("IB"),
        account_balance: s("50000.00"),
        cash_on_hand: s("49000.00"),
        margin_balance: s("1000.00"),
        day_pnl: s("-12.50"),
        open_position_pnl: s("4.00"),
        closed_position_pnl: s("-16.50"),
        available_buying_power: s("40000.00"),
        used_buying_power: s("10000.00"),
        is_snapshot: Some(true),
        ssboe: Some(1_700_000_004),
        usecs: Some(4),
    }
}

fn instrument_pnl() -> pb::InstrumentPnl {
    pb::InstrumentPnl {
        account_id: s("ACCT"),
        symbol: s("NQH6"),
        exchange: s("CME"),
        product_code: s("NQ"),
        instrument_type: s("Future"),
        open_position_pnl: s("5.00"),
        closed_position_pnl: s("6.00"),
        mtm_security: s("7.00"),
        open_position_quantity: Some(2),
        closed_position_quantity: Some(3),
        net_quantity: Some(-1),
        avg_open_fill_price: Some(18_240.5),
        is_snapshot: Some(true),
        ssboe: Some(1_700_000_005),
        usecs: Some(5),
    }
}

fn order_notification() -> pb::OrderNotification {
    pb::OrderNotification {
        source: "rithmic".into(),
        kind: s("filled"),
        notify_type: Some(5),
        notify_type_name: s("FILL"),
        status: s("complete"),
        basket_id: s("basket-1"),
        exchange_order_id: s("EX-1"),
        user_tag: s("tag-1"),
        account_id: s("ACCT"),
        symbol: s("NQH6"),
        exchange: s("CME"),
        quantity: Some(4),
        total_fill_size: Some(4),
        total_unfilled_size: Some(0),
        fill_size: Some(2),
        price: Some(18_250.0),
        trigger_price: Some(18_240.0),
        avg_fill_price: Some(18_249.5),
        fill_price: Some(18_250.25),
        transaction_type: Some(1),
        price_type: Some(2),
        duration: Some(1),
        fill_id: s("FILL-1"),
        text: s("filled"),
        report_text: s("report"),
        completion_reason: s("F"),
        ssboe: Some(1_700_000_006),
        usecs: Some(6),
        ts_event_ns: Some(1_700_000_006_000_006_000),
        is_snapshot: Some(true),
    }
}

fn history_bar() -> pb::HistoryBar {
    pb::HistoryBar {
        symbol: s("NQH6"),
        exchange: s("CME"),
        bar_type: Some(2),
        period: s("1"),
        marker: Some(1_700_000_060),
        open_price: Some(1.0),
        high_price: Some(2.0),
        low_price: Some(0.5),
        close_price: Some(1.5),
        volume: Some(100),
        num_trades: Some(10),
        bid_volume: Some(40),
        ask_volume: Some(60),
        ts_event_ns: Some(1_700_000_060_000_000_000),
    }
}

fn history_tick() -> pb::HistoryTick {
    pb::HistoryTick {
        symbol: s("NQH6"),
        exchange: s("CME"),
        open_price: Some(1.0),
        high_price: Some(2.0),
        low_price: Some(0.5),
        close_price: Some(1.5),
        volume: Some(9),
        num_trades: Some(3),
        ssboe: Some(1_700_000_007),
        usecs: Some(7),
        ts_event_ns: Some(1_700_000_007_000_007_000),
    }
}

#[test]
fn every_event_variant_round_trips() {
    use pb::event::Body;
    let events = [
        Body::LastTrade(last_trade()),
        Body::Bbo(bbo()),
        Body::OrderBook(order_book()),
        Body::AccountPnl(account_pnl()),
        Body::InstrumentPnl(instrument_pnl()),
        Body::OrderNotification(order_notification()),
        Body::TimeBar(history_bar()),
        Body::Other(pb::OtherEvent {
            type_name: "Unhandled".into(),
            source: "ticker".into(),
        }),
    ];
    assert_eq!(events.len(), 8, "one case per pb::event::Body variant");
    for body in events {
        let wire = pb::Event {
            body: Some(body.clone()),
        };
        let event: PlantEvent = from::plant_event_from_pb(wire.clone()).expect("decodes");
        let (_key, again) = to::plant_event_to_routed(event);
        assert_eq!(again, wire, "{body:?}");
    }
}

#[test]
fn an_event_with_no_body_decodes_to_nothing() {
    assert!(from::plant_event_from_pb(pb::Event { body: None }).is_none());
}

#[test]
fn events_are_routed_to_the_channel_their_poll_reads() {
    use pb::event::Body;
    let cases = [
        (Body::LastTrade(last_trade()), Channel::Market),
        (Body::Bbo(bbo()), Channel::Market),
        (Body::OrderBook(order_book()), Channel::Market),
        (
            Body::Other(pb::OtherEvent {
                type_name: "X".into(),
                source: "ticker".into(),
            }),
            Channel::Market,
        ),
        (Body::TimeBar(history_bar()), Channel::History),
        (Body::AccountPnl(account_pnl()), Channel::Pnl),
        (Body::InstrumentPnl(instrument_pnl()), Channel::Pnl),
        (
            Body::OrderNotification(order_notification()),
            Channel::Order,
        ),
    ];
    for (body, channel) in cases {
        let event = from::plant_event_from_pb(pb::Event {
            body: Some(body.clone()),
        })
        .expect("decodes");
        assert_eq!(Channel::of(&event), channel, "{body:?}");
    }
}

#[test]
fn history_and_reference_rows_round_trip() {
    let tick = history_tick();
    assert_eq!(
        to::history_tick_to_pb(from::history_tick_from_pb(tick.clone())),
        tick
    );
    let bar = history_bar();
    assert_eq!(
        to::history_bar_to_pb(from::history_bar_from_pb(bar.clone())),
        bar
    );

    let front = pb::FrontMonthResponse {
        symbol: s("NQ"),
        exchange: s("CME"),
        trading_symbol: s("NQH6"),
        trading_exchange: s("CME"),
        symbol_name: s("E-mini Nasdaq"),
        is_front_month_symbol: Some(true),
    };
    assert_eq!(
        to::front_month_to_pb(from::front_month_from_pb(front.clone())),
        front
    );

    let reference = pb::ReferenceDataResponse {
        symbol: s("NQH6"),
        exchange: s("CME"),
        trading_symbol: s("NQH6"),
        trading_exchange: s("CME"),
        symbol_name: s("name"),
        product_code: s("NQ"),
        instrument_type: s("Future"),
        underlying: s("NQ"),
        currency: s("USD"),
        expiration_date: s("20260319"),
        tick_size: Some(0.25),
        point_value: Some(20.0),
        price_precision: Some(2),
        is_tradable: Some(true),
    };
    assert_eq!(
        to::reference_data_to_pb(from::reference_data_from_pb(reference.clone())),
        reference
    );

    let probe = pb::TimeBarProbeRow {
        variant: "TimeBar".into(),
        source: "history".into(),
        error: s("e"),
        rp_code: vec!["0".into(), "7".into()],
        parsed: true,
        skip_reason: s("why"),
        symbol: s("NQH6"),
        exchange: s("CME"),
        bar_type: Some(2),
        period: s("1"),
        marker: Some(1_700_000_060),
        open_price: Some(1.0),
        high_price: Some(2.0),
        low_price: Some(0.5),
        close_price: Some(1.5),
        settlement_price: Some(1.25),
        has_settlement_price: Some(true),
        volume: Some(10),
        num_trades: Some(4),
    };
    assert_eq!(
        to::time_bar_probe_row_to_pb(from::time_bar_probe_row_from_pb(probe.clone())),
        probe
    );
}

#[test]
fn rms_rows_and_orders_round_trip() {
    let product = pb::ProductRmsInfo {
        product_code: s("NQ"),
        commission_fill_rate: Some(2.5),
        presence_bits: Some(3),
    };
    assert_eq!(
        to::product_rms_info_to_pb(from::product_rms_info_from_pb(product.clone())),
        product
    );
    let account = pb::AccountRmsInfo {
        account_id: s("ACCT"),
        default_commission: Some(1.25),
        presence_bits: Some(1),
    };
    assert_eq!(
        to::account_rms_info_to_pb(from::account_rms_info_from_pb(account.clone())),
        account
    );
    let order = order_notification();
    assert_eq!(
        to::order_notification_to_pb(from::order_notification_from_pb(order.clone())),
        order
    );
}

#[test]
fn resolved_account_none_until_the_parent_resolves_one() {
    assert!(from::resolved_account_from_pb(pb::ResolvedAccountResponse::default()).is_none());
    let resolved = from::resolved_account_from_pb(pb::ResolvedAccountResponse {
        account_id: s("ACCT"),
        fcm_id: s("FCM"),
        ib_id: s("IB"),
    })
    .expect("resolved");
    assert_eq!(
        (resolved.account_id, resolved.fcm_id, resolved.ib_id),
        ("ACCT".into(), "FCM".into(), "IB".into())
    );
}

#[test]
fn unset_optional_fields_stay_unset() {
    // The all-default message round-trips too: absent stays absent, not zero.
    let empty = pb::LastTrade::default();
    assert_eq!(
        to::last_trade_to_pb(from::last_trade_from_pb(empty.clone())),
        empty
    );
    let reference = from::reference_data_from_pb(pb::ReferenceDataResponse::default());
    assert_eq!(reference.price_precision, 0);
    assert!(!reference.is_tradable);
}
