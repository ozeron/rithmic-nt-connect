//! One scenario list, identical assertions through both host attachments.
//! Transport-only cases and pre-existing recovery characterizations are separate.
mod common;
#[path = "client_scenarios/harness.rs"]
mod harness;
#[path = "client_scenarios/legacy_recovery.rs"]
mod legacy_recovery;
#[path = "client_scenarios/remote_only.rs"]
mod remote_only;

use common::*;
use harness::{Harness, LocalHarness, RemoteHarness};
use rithmic_gateway::testing as scripted;
use rithmic_host::dto::PlantEvent;
use rithmic_host::{
    fake::FakeFailure, AccountTriple, Error, GapDto, ModifyOrder, OrderBookDto, PlaceBracketOrder,
    PlaceOrder, Session, Stream,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn place(tag: &str) -> PlaceOrder {
    PlaceOrder {
        symbol: NQ.into(),
        exchange: CME.into(),
        side: "BUY".into(),
        price_type: "LIMIT".into(),
        quantity: 1,
        user_tag: tag.into(),
        price: Some(1.0),
        trigger_price: None,
        duration: "DAY".into(),
        trail_by_ticks: None,
        trail_by_price_id: None,
    }
}

async fn session_for<H: Harness>(parent: &H) -> Arc<dyn Session> {
    let session = parent.attach().await;
    session.connect().await.expect("connect");
    session
}

async fn second_connection<H: Harness>(parent: &H) -> Arc<dyn Session> {
    session_for(parent).await
}

async fn drain(
    session: &dyn Session,
    want: usize,
    channel: for<'a> fn(&'a dyn Session, Duration) -> Fut<'a>,
) -> Vec<PlantEvent> {
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while got.len() < want && Instant::now() < deadline {
        if let Some(event) = channel(session, Duration::from_millis(100))
            .await
            .expect("poll")
        {
            got.push(event);
        }
    }
    got
}

type Fut<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = rithmic_host::Result<Option<PlantEvent>>> + Send + 'a>,
>;

fn market(s: &dyn Session, t: Duration) -> Fut<'_> {
    s.poll_event(t)
}
fn history(s: &dyn Session, t: Duration) -> Fut<'_> {
    s.poll_history_event(t)
}
fn pnl(s: &dyn Session, t: Duration) -> Fut<'_> {
    s.poll_pnl_event(t)
}
fn order(s: &dyn Session, t: Duration) -> Fut<'_> {
    s.poll_order_event(t)
}

fn code(err: &Error) -> &str {
    match err {
        Error::Gateway { code, .. } => code,
        other => panic!("expected a gateway error, got {other:?}"),
    }
}

async fn one_session_stopping_keeps_the_others_order_stream<H: Harness>() {
    let parent = H::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    let a = session_for(&parent).await;
    let b = session_for(&parent).await;
    a.subscribe_order_updates().await.unwrap();
    b.subscribe_order_updates().await.unwrap();

    a.disconnect_order_plant().await.unwrap();
    assert_eq!(
        parent.count("disconnect_order_plant").await,
        0,
        "{:?}",
        parent.calls().await
    );
    parent
        .command("push order accepted basket-1 tag-1 NQH6")
        .await;
    assert_eq!(
        drain(&*b, 1, order).await,
        [scripted::order_notification(
            "accepted", "basket-1", "tag-1", NQ
        )],
        "b's orders still arrive"
    );
    assert!(matches!(
        a.place_order(&place("closed")).await,
        Err(Error::NotSent(_))
    ));
    b.place_order(&place("still-open")).await.unwrap();
    a.ensure_order_plant().await.unwrap();
    a.place_order(&place("ensured")).await.unwrap();
}

async fn a_session_that_disconnects_releases_only_what_it_held<H: Harness>() {
    let parent = H::start(Opts::default()).await;
    let a = session_for(&parent).await;
    let b = session_for(&parent).await;
    a.subscribe("ESZ6", CME).await.unwrap();
    a.subscribe(NQ, CME).await.unwrap();
    b.subscribe(NQ, CME).await.unwrap();

    a.disconnect().await.unwrap();
    assert!(
        wait_until_async(Duration::from_secs(5), || async {
            parent.count(&format!("unsubscribe ESZ6 {CME}")).await == 1
        })
        .await,
        "a's sole intent is released: {:?}",
        parent.calls().await
    );
    assert_eq!(parent.count(&format!("unsubscribe {NQ} {CME}")).await, 0);
    parent.push_last_trades(NQ, 1).await;
    assert_eq!(drain(&*b, 1, market).await.len(), 1);
}

async fn a_dropped_session_releases_only_what_it_held<H: Harness>() {
    let parent = H::start(Opts::default()).await;
    let a = session_for(&parent).await;
    let b = session_for(&parent).await;
    a.subscribe("ESZ6", CME).await.unwrap();
    a.subscribe(NQ, CME).await.unwrap();
    b.subscribe(NQ, CME).await.unwrap();

    drop(a);
    assert!(
        wait_until_async(Duration::from_secs(5), || async {
            parent.count(&format!("unsubscribe ESZ6 {CME}")).await == 1
        })
        .await,
        "the dropped facade's sole intent is released: {:?}",
        parent.calls().await
    );
    assert_eq!(parent.count(&format!("unsubscribe {NQ} {CME}")).await, 0);

    // The dropped attachment no longer holds an intent, so b releases it.
    b.unsubscribe(NQ, CME).await.unwrap();
    assert!(
        wait_until_async(Duration::from_secs(5), || async {
            parent.count(&format!("unsubscribe {NQ} {CME}")).await == 1
        })
        .await,
        "{:?}",
        parent.calls().await
    );
}

async fn two_attachments_share_one_venue_subscription_by_refcount<H: Harness>() {
    let parent = H::start(Opts::default()).await;
    let a = session_for(&parent).await;
    let b = second_connection(&parent).await;
    a.subscribe(NQ, CME).await.unwrap();
    b.subscribe(NQ, CME).await.unwrap();
    assert_eq!(
        parent.count(&format!("subscribe {NQ} {CME}")).await,
        1,
        "venue joined once (0→1)"
    );

    parent.push_last_trades(NQ, 3).await;
    for session in [&a, &b] {
        let events = drain(session.as_ref(), 3, market).await;
        assert_eq!(
            events,
            (0..3)
                .map(|i| scripted::last_trade(NQ, CME, 100.25 + i as f64, 1))
                .collect::<Vec<_>>()
        );
    }

    a.unsubscribe(NQ, CME).await.unwrap();
    assert_eq!(
        parent.count("unsubscribe").await,
        0,
        "a peer still subscribed"
    );
    b.unsubscribe(NQ, CME).await.unwrap();
    assert_eq!(parent.count(&format!("unsubscribe {NQ} {CME}")).await, 1);
}

async fn last_attachment_releases_its_intents_and_the_host_accepts_a_new_one<H: Harness>() {
    let parent = H::start(Opts::default()).await;
    let a = session_for(&parent).await;
    a.subscribe(NQ, CME).await.unwrap();
    a.disconnect().await.unwrap();
    assert!(
        parent
            .wait_for_call(&format!("unsubscribe {NQ} {CME}"), Duration::from_secs(10))
            .await,
        "{:?}",
        parent.calls().await
    );
    // The harness retains its host and can accept a new attachment.
    let again = session_for(&parent).await;
    again.get_front_month("NQ", CME).await.unwrap();
}

async fn cancel_all_has_its_own_gate_independent_of_trading<H: Harness>() {
    let parent = H::start(Opts {
        trading: false,
        cancel_all: true,
    })
    .await;
    let s = session_for(&parent).await;
    s.cancel_all_orders().await.unwrap();
    assert_eq!(parent.count("cancel_all_orders").await, 1);
    assert_eq!(
        code(&s.place_order(&place("t")).await.unwrap_err()),
        "trading_disabled"
    );
    drop(parent);

    let parent = H::start(Opts {
        trading: true,
        cancel_all: false,
    })
    .await;
    let s = session_for(&parent).await;
    assert_eq!(
        code(&s.cancel_all_orders().await.unwrap_err()),
        "cancel_all_denied"
    );
    assert_eq!(parent.count("cancel_all_orders").await, 0);
}

async fn history_is_refused_under_a_live_ticker_intent<H: Harness>() {
    let parent = H::start(Opts::default()).await;
    let s = session_for(&parent).await;
    s.subscribe(NQ, CME).await.unwrap();
    let err = s.load_ticks(NQ, CME, 0, 10).await.unwrap_err();
    assert_eq!(code(&err), "history_denied_live_md", "returned unchanged");
    assert_eq!(
        parent.count("load_ticks").await,
        0,
        "refused before the plant"
    );

    s.unsubscribe(NQ, CME).await.unwrap();
    assert!(s.load_ticks(NQ, CME, 0, 10).await.unwrap().is_empty());
    assert_eq!(parent.count("load_ticks").await, 1);
}

async fn bracket_updates_reach_only_connections_that_subscribed_to_them<H: Harness>() {
    let parent = H::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    let orders_only = session_for(&parent).await;
    let with_brackets = second_connection(&parent).await;
    orders_only.subscribe_order_updates().await.unwrap();
    with_brackets.subscribe_order_updates().await.unwrap();
    with_brackets.subscribe_bracket_updates().await.unwrap();

    parent.command("push bracket basket-9 8 16").await;
    parent
        .command("push order accepted basket-9 tag-9 NQH6")
        .await;

    let got = drain(&*with_brackets, 2, order).await;
    assert_eq!(
        got,
        [
            scripted::bracket_update("basket-9", 8, 16),
            scripted::order_notification("accepted", "basket-9", "tag-9", NQ)
        ]
    );
    // The order-only connection gets the order update and nothing before it.
    assert_eq!(
        drain(&*orders_only, 1, order).await[0],
        scripted::order_notification("accepted", "basket-9", "tag-9", NQ)
    );
}

async fn concurrent_polls_and_rpcs_make_progress_without_deadlock<H: Harness>() {
    let parent = H::start(Opts::default()).await;
    let s = session_for(&parent).await;

    let mut tasks = Vec::new();
    for _ in 0..4 {
        let s = Arc::clone(&s);
        tasks.push(tokio::spawn(async move {
            // Blocks up to the timeout on the market channel, repeatedly.
            for _ in 0..3 {
                let _ = s.poll_event(Duration::from_millis(150)).await;
            }
        }));
    }
    let rpcs = {
        let s = Arc::clone(&s);
        tokio::spawn(async move {
            for _ in 0..25 {
                s.get_front_month("NQ", CME).await.expect("rpc");
            }
        })
    };
    let all = async {
        for task in tasks {
            task.await.expect("poller");
        }
        rpcs.await.expect("rpcs");
    };
    tokio::time::timeout(Duration::from_secs(20), all)
        .await
        .expect("no deadlock between blocking polls and RPCs");
}

async fn a_plant_lag_reaches_the_client_in_order_as_channel_lagged<H: Harness>() {
    let h = H::start(Opts::default()).await;
    let s = session_for(&h).await;
    s.subscribe(NQ, CME).await.unwrap();
    let first = scripted::last_trade(NQ, CME, 1.0, 1);
    let last = scripted::last_trade(NQ, CME, 2.0, 1);
    h.venue().push_event(Stream::Ticker, first.clone());
    h.venue().push_event(
        Stream::Ticker,
        PlantEvent::Gap(GapDto {
            stream: Stream::Ticker,
            skipped: 41,
        }),
    );
    h.venue().push_event(Stream::Ticker, last.clone());
    assert_eq!(
        s.poll_event(Duration::from_secs(5)).await.unwrap(),
        Some(first)
    );
    assert!(matches!(
        s.poll_event(Duration::from_secs(5)).await,
        Err(Error::ChannelLagged {
            plant: "ticker",
            skipped: 41
        })
    ));
    assert_eq!(
        s.poll_event(Duration::from_secs(5)).await.unwrap(),
        Some(last)
    );
}

async fn the_next_events_batch_carries_the_gap_in_order<H: Harness>() {
    let h = H::start(Opts::default()).await;
    let s = session_for(&h).await;
    s.subscribe(NQ, CME).await.unwrap();
    let expected = vec![
        scripted::last_trade(NQ, CME, 1.0, 1),
        PlantEvent::Gap(GapDto {
            stream: Stream::Ticker,
            skipped: 9,
        }),
        scripted::last_trade(NQ, CME, 2.0, 1),
    ];
    for event in &expected {
        h.venue().push_event(Stream::Ticker, event.clone());
    }
    let mut got = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while got.len() < expected.len() {
            got.extend(
                s.next_events(Stream::Ticker, 16, Duration::from_millis(200))
                    .await
                    .unwrap(),
            );
        }
    })
    .await
    .expect("all records including the gap");
    assert_eq!(got, expected);
}

async fn an_order_stream_gap_reaches_order_subscribers<H: Harness>() {
    let parent = H::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    let s = session_for(&parent).await;
    s.subscribe_order_updates().await.unwrap();
    parent.command("push gap order 7").await;
    assert!(matches!(
        s.poll_order_event(Duration::from_secs(5)).await,
        Err(Error::ChannelLagged {
            plant: "order",
            skipped: 7
        })
    ));
}

async fn order_peers_are_told_when_the_order_plant_is_reseated<H: Harness>() {
    let parent = H::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    let brackets_peer = session_for(&parent).await;
    let order_peer = second_connection(&parent).await;
    brackets_peer.subscribe_order_updates().await.unwrap();
    brackets_peer.subscribe_bracket_updates().await.unwrap();
    order_peer.subscribe_order_updates().await.unwrap();

    // The last brackets holder leaves while an order peer remains: the parent
    // re-seats the order plant, and events in that window may be lost.
    brackets_peer.disconnect_order_plant().await.unwrap();
    assert!(
        wait_until_async(Duration::from_secs(5), || async {
            parent.count("subscribe_order_updates").await >= 2
        })
        .await
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut told = false;
    while Instant::now() < deadline && !told {
        told = matches!(
            order_peer
                .poll_order_event(Duration::from_millis(100))
                .await,
            Err(Error::ChannelLagged { plant: "order", .. })
        );
    }
    assert!(told, "the order peer was told to reconcile");
    assert_eq!(parent.count("connect").await, 1, "no full plant rebuild");
}

// Add a scenario here once: both attachments get the same body and assertions.
macro_rules! scenarios {
    ($($name:ident),+ $(,)?) => {
        mod local {
            $(#[tokio::test]
            async fn $name() { super::$name::<super::LocalHarness>().await; })+
        }
        mod remote {
            $(#[tokio::test]
            async fn $name() { super::$name::<super::RemoteHarness>().await; })+
        }
    };
}
scenarios! {
    one_session_stopping_keeps_the_others_order_stream,
    a_session_that_disconnects_releases_only_what_it_held,
    a_dropped_session_releases_only_what_it_held,
    two_attachments_share_one_venue_subscription_by_refcount,
    last_attachment_releases_its_intents_and_the_host_accepts_a_new_one,
    cancel_all_has_its_own_gate_independent_of_trading,
    history_is_refused_under_a_live_ticker_intent,
    bracket_updates_reach_only_connections_that_subscribed_to_them,
    concurrent_polls_and_rpcs_make_progress_without_deadlock,
    a_plant_lag_reaches_the_client_in_order_as_channel_lagged,
    the_next_events_batch_carries_the_gap_in_order,
    an_order_stream_gap_reaches_order_subscribers,
    order_peers_are_told_when_the_order_plant_is_reseated,
    ticker_book_and_time_bar_intents_are_idempotent_and_refcounted,
    all_event_fields_survive_both_attachments,
    pnl_intents_are_shared_and_disconnect_is_per_session,
    reads_return_the_same_records_and_complete_empty,
    all_commands_reach_the_venue_once,
    all_command_outcomes_preserve_the_error_payload,
    all_gated_rpcs_are_denied_before_the_venue,
    subscription_failure_rolls_back_and_can_be_retried,
    a_slow_consumer_gets_a_gap_without_costing_its_peer_events,
}

fn modify() -> ModifyOrder {
    ModifyOrder {
        basket_id: "basket-1".into(),
        symbol: NQ.into(),
        exchange: CME.into(),
        quantity: 2,
        price_type: "LIMIT".into(),
        price: Some(99.75),
        trigger_price: None,
        trail_by_ticks: None,
    }
}
fn bracket() -> PlaceBracketOrder {
    PlaceBracketOrder {
        symbol: NQ.into(),
        exchange: CME.into(),
        side: "BUY".into(),
        price_type: "LIMIT".into(),
        quantity: 1,
        localid: "bracket-1".into(),
        price: Some(100.0),
        trigger_price: None,
        duration: "DAY".into(),
        stop_ticks: Some(8),
        target_ticks: Some(16),
    }
}
const COMMANDS: [&str; 7] = [
    "place_order",
    "place_bracket_order",
    "adjust_bracket_stop",
    "adjust_bracket_target",
    "cancel_order",
    "modify_order",
    "cancel_all_orders",
];
async fn command(s: &dyn Session, method: &str) -> rithmic_host::Result<()> {
    match method {
        "place_order" => s.place_order(&place("tag-1")).await,
        "place_bracket_order" => s.place_bracket_order(&bracket()).await,
        "adjust_bracket_stop" => s.adjust_bracket_stop("basket-1", 7, Some(2)).await,
        "adjust_bracket_target" => s.adjust_bracket_target("basket-1", 15, None).await,
        "cancel_order" => s.cancel_order("basket-1").await,
        "modify_order" => s.modify_order(&modify()).await,
        "cancel_all_orders" => s.cancel_all_orders().await,
        other => panic!("unknown command {other}"),
    }
}
fn enabled() -> Opts {
    Opts {
        trading: true,
        cancel_all: true,
    }
}

fn book() -> PlantEvent {
    PlantEvent::OrderBook(OrderBookDto {
        symbol: Some(NQ.into()),
        exchange: Some(CME.into()),
        update_type: Some(3),
        bid_price: vec![100.0, 99.75],
        bid_size: vec![2, 3],
        ask_price: vec![100.25],
        ask_size: vec![5],
        ssboe: Some(1_700_000_000),
        usecs: Some(321),
        ts_event_ns: Some(1_700_000_000_000_321_000),
    })
}

async fn ticker_book_and_time_bar_intents_are_idempotent_and_refcounted<H: Harness>() {
    let h = H::start(Opts::default()).await;
    let a = session_for(&h).await;
    let b = session_for(&h).await;
    for s in [&a, &b, &a] {
        s.subscribe(NQ, CME).await.unwrap();
        s.subscribe_order_book_summary(NQ, CME).await.unwrap();
        s.subscribe_time_bars(NQ, CME, 2, 1).await.unwrap();
    }
    for prefix in [
        "subscribe NQH6 CME",
        "subscribe_book NQH6 CME",
        "subscribe_time_bars NQH6 CME 2 1",
    ] {
        assert_eq!(
            h.count(prefix).await,
            1,
            "{}: {:?}",
            prefix,
            h.calls().await
        );
    }
    a.unsubscribe(NQ, CME).await.unwrap();
    a.unsubscribe_order_book_summary(NQ, CME).await.unwrap();
    a.unsubscribe_time_bars(NQ, CME, 2, 1).await.unwrap();
    assert_eq!(h.count("unsubscribe").await, 0);
    let tick = scripted::last_trade(NQ, CME, 100.25, 1);
    let bar = scripted::time_bar(NQ, CME, 101.0);
    h.venue().push_event(Stream::Ticker, tick.clone());
    h.venue().push_event(Stream::Ticker, book());
    h.venue().push_event(Stream::History, bar.clone());
    assert_eq!(drain(&*b, 2, market).await, [tick, book()]);
    assert_eq!(drain(&*b, 1, history).await, [bar]);
    b.unsubscribe(NQ, CME).await.unwrap();
    b.unsubscribe_order_book_summary(NQ, CME).await.unwrap();
    b.unsubscribe_time_bars(NQ, CME, 2, 1).await.unwrap();
    let released: Vec<_> = h
        .calls()
        .await
        .into_iter()
        .filter(|c| c.starts_with("unsubscribe"))
        .collect();
    assert_eq!(
        released,
        [
            "unsubscribe NQH6 CME",
            "unsubscribe_book NQH6 CME",
            "unsubscribe_time_bars NQH6 CME 2 1"
        ]
    );
    // Each kind can be rejoined after the host's count returns to zero.
    b.subscribe(NQ, CME).await.unwrap();
    b.subscribe_order_book_summary(NQ, CME).await.unwrap();
    b.subscribe_time_bars(NQ, CME, 2, 1).await.unwrap();
    assert_eq!(h.count("subscribe NQH6 CME").await, 2);
    assert_eq!(h.count("subscribe_book NQH6 CME").await, 2);
    assert_eq!(h.count("subscribe_time_bars NQH6 CME 2 1").await, 2);
}

async fn all_event_fields_survive_both_attachments<H: Harness>() {
    let h = H::start(enabled()).await;
    let s = session_for(&h).await;
    s.subscribe(NQ, CME).await.unwrap();
    s.subscribe_order_book_summary(NQ, CME).await.unwrap();
    s.subscribe_time_bars(NQ, CME, 2, 1).await.unwrap();
    s.subscribe_pnl().await.unwrap();
    s.subscribe_order_updates().await.unwrap();
    s.subscribe_bracket_updates().await.unwrap();
    let mut trade = scripted::last_trade(NQ, CME, 100.25, 1);
    if let PlantEvent::LastTrade(t) = &mut trade {
        t.aggressor = None;
        t.usecs = Some(123);
        t.is_snapshot = None;
        t.ts_event_ns = Some(1_700_000_000_000_123_000);
    }
    let mut quote = scripted::bbo(NQ, CME, 99.75, 0, 100.25, 7);
    if let PlantEvent::Bbo(q) = &mut quote {
        q.bid_size = None;
        q.is_snapshot = Some(true);
    }
    let mut position = rithmic_host::InstrumentPnlDto {
        account_id: Some("ACCT".into()),
        symbol: Some(NQ.into()),
        exchange: Some(CME.into()),
        product_code: Some("NQ".into()),
        instrument_type: Some("Future".into()),
        open_position_pnl: Some("-12.50".into()),
        closed_position_pnl: Some("0".into()),
        mtm_security: None,
        open_position_quantity: Some(2),
        closed_position_quantity: Some(0),
        net_quantity: Some(-2),
        avg_open_fill_price: Some(100.25),
        is_snapshot: Some(true),
        ssboe: Some(1_700_000_000),
        usecs: Some(987),
    };
    // Explicit zero/false and omitted values must both survive the wire.
    let first_position = PlantEvent::InstrumentPnl(position.clone());
    position.net_quantity = None;
    position.is_snapshot = Some(false);
    let events = [
        (Stream::Ticker, vec![trade, quote, book()]),
        (Stream::History, vec![scripted::time_bar(NQ, CME, 101.5)]),
        (
            Stream::Pnl,
            vec![
                scripted::account_pnl("ACCT", "-12.50"),
                first_position,
                PlantEvent::InstrumentPnl(position),
            ],
        ),
        (
            Stream::Order,
            vec![
                scripted::order_notification("accepted", "basket-9", "tag-9", NQ),
                scripted::bracket_update("basket-9", 8, 16),
            ],
        ),
    ];
    for (stream, expected) in events {
        for event in &expected {
            h.venue().push_event(stream, event.clone());
        }
        let poll = match stream {
            Stream::Ticker => market,
            Stream::History => history,
            Stream::Pnl => pnl,
            Stream::Order => order,
        };
        assert_eq!(
            drain(&*s, expected.len(), poll).await,
            expected,
            "{stream:?}: every field"
        );
    }
}

async fn reads_return_the_same_records_and_complete_empty<H: Harness>() {
    let h = H::start(enabled()).await;
    let account = AccountTriple {
        account_id: "ACCT".into(),
        fcm_id: "FCM".into(),
        ib_id: "IB".into(),
    };
    h.venue().set_account(account.clone());
    let s = session_for(&h).await;
    assert_eq!(s.resolved_account().await, Some(account));
    assert_eq!(
        s.get_front_month("NQ", CME).await.unwrap(),
        rithmic_host::FrontMonthDto {
            symbol: Some("NQ".into()),
            exchange: Some(CME.into()),
            trading_symbol: Some(NQ.into()),
            trading_exchange: Some(CME.into()),
            symbol_name: None,
            is_front_month_symbol: Some(true),
        }
    );
    assert_eq!(
        s.get_reference_data(NQ, CME).await.unwrap(),
        rithmic_host::ReferenceDataDto {
            symbol: Some(NQ.into()),
            exchange: Some(CME.into()),
            trading_symbol: Some(NQ.into()),
            trading_exchange: Some(CME.into()),
            symbol_name: None,
            product_code: Some("NQ".into()),
            instrument_type: Some("Future".into()),
            underlying: None,
            currency: Some("USD".into()),
            expiration_date: None,
            tick_size: Some(0.25),
            point_value: Some(20.0),
            price_precision: 2,
            is_tradable: true,
        }
    );
    h.clear_calls().await;
    assert!(s.load_ticks(NQ, CME, 0, 10).await.unwrap().is_empty());
    assert!(s
        .load_time_bars(NQ, CME, 0, 60, 2, 1)
        .await
        .unwrap()
        .is_empty());
    assert!(s
        .probe_time_bars(NQ, CME, 0, 60, 2, 1)
        .await
        .unwrap()
        .is_empty());
    assert!(s.load_orders(0, 10).await.unwrap().is_empty());
    assert!(s.load_product_rms_info().await.unwrap().is_empty());
    assert!(s.load_account_rms_info().await.unwrap().is_empty());
    assert!(s
        .load_fill_history(20260901, 20260930)
        .await
        .unwrap()
        .is_empty());
    assert!(s.load_order_history_dates().await.unwrap().is_empty());
    assert!(s.load_order_history("20260930").await.unwrap().is_empty());
    assert_eq!(
        h.calls().await,
        [
            "load_ticks NQH6 CME 0 10",
            "load_time_bars NQH6 CME 0 60 2 1",
            "probe_time_bars NQH6 CME 0 60 2 1",
            "load_orders",
            "load_product_rms_info",
            "load_account_rms_info",
            "load_fill_history 20260901 20260930",
            "load_order_history_dates",
            "load_order_history 20260930",
        ]
    );
}

async fn all_commands_reach_the_venue_once<H: Harness>() {
    let h = H::start(enabled()).await;
    let s = session_for(&h).await;
    h.clear_calls().await;
    for method in COMMANDS {
        command(&*s, method).await.unwrap();
    }
    assert_eq!(
        h.calls().await,
        [
            "place_order tag-1",
            "place_bracket_order bracket-1",
            "adjust_bracket_stop basket-1 7 Some(2)",
            "adjust_bracket_target basket-1 15 None",
            "cancel_order basket-1",
            "modify_order basket-1",
            "cancel_all_orders",
        ]
    );
}

async fn all_command_outcomes_preserve_the_error_payload<H: Harness>() {
    let h = H::start(enabled()).await;
    let s = session_for(&h).await;
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for method in COMMANDS {
        for (failure, error) in [
            (
                FakeFailure::Rejected("price refused".into()),
                Error::Rejected("price refused".into()),
            ),
            (
                FakeFailure::NotSent("no route".into()),
                Error::NotSent("no route".into()),
            ),
            (
                FakeFailure::Unknown("write failed".into()),
                Error::OutcomeUnknown("write failed".into()),
            ),
        ] {
            h.venue().fail_next(method, failure);
            let result = command(&*s, method).await.unwrap_err();
            actual.push((method, format!("{result:?}")));
            expected.push((method, format!("{error:?}")));
        }
        assert_eq!(h.count(method).await, 3, "no command retry");
    }
    // Read barrier also proves the connection remains usable after unknown outcomes.
    s.get_front_month("NQ", CME).await.unwrap();
    // Deliberately strict: source error messages must not acquire display prefixes
    // on the wire. This exposes a pre-existing RemoteSession parity defect.
    assert_eq!(actual, expected, "typed error class AND original payload");
}

async fn all_gated_rpcs_are_denied_before_the_venue<H: Harness>() {
    let h = H::start(Opts::default()).await;
    let s = session_for(&h).await;
    h.clear_calls().await;
    let mut errors = Vec::new();
    for method in COMMANDS {
        errors.push((method, command(&*s, method).await.unwrap_err()));
    }
    errors.extend([
        ("ensure_order", s.ensure_order_plant().await.unwrap_err()),
        (
            "subscribe_order_updates",
            s.subscribe_order_updates().await.unwrap_err(),
        ),
        (
            "subscribe_bracket_updates",
            s.subscribe_bracket_updates().await.unwrap_err(),
        ),
        ("load_orders", s.load_orders(0, 1).await.unwrap_err()),
        (
            "load_product_rms_info",
            s.load_product_rms_info().await.unwrap_err(),
        ),
        (
            "load_account_rms_info",
            s.load_account_rms_info().await.unwrap_err(),
        ),
        (
            "load_fill_history",
            s.load_fill_history(20260901, 20260930).await.unwrap_err(),
        ),
        (
            "load_order_history_dates",
            s.load_order_history_dates().await.unwrap_err(),
        ),
        (
            "load_order_history",
            s.load_order_history("20260930").await.unwrap_err(),
        ),
    ]);
    for (method, error) in errors {
        let (code, reason) = if method == "cancel_all_orders" {
            ("cancel_all_denied", "cancel_all")
        } else {
            ("trading_disabled", "trading")
        };
        assert_eq!(
            format!("{error:?}"),
            format!(
                "{:?}",
                Error::Gateway {
                    code: code.into(),
                    message: format!("{method} denied: parent {reason} disabled"),
                }
            )
        );
    }
    assert!(
        h.calls().await.is_empty(),
        "gates must run before ALL venue calls"
    );
}

async fn subscription_failure_rolls_back_and_can_be_retried<H: Harness>() {
    let h = H::start(Opts::default()).await;
    let s = session_for(&h).await;
    h.venue()
        .fail_next("subscribe", FakeFailure::Config("invalid contract".into()));
    let error = s.subscribe(NQ, CME).await.unwrap_err();
    assert_eq!(
        format!("{error:?}"),
        format!(
            "{:?}",
            Error::Gateway {
                code: "subscribe_failed".into(),
                message: "config error: invalid contract".into(),
            }
        )
    );
    s.subscribe(NQ, CME).await.unwrap();
    assert_eq!(
        h.count("subscribe NQH6 CME").await,
        2,
        "failed intent must roll back"
    );
    h.venue()
        .push_event(Stream::Ticker, scripted::last_trade(NQ, CME, 100.25, 1));
    assert_eq!(
        drain(&*s, 1, market).await,
        [scripted::last_trade(NQ, CME, 100.25, 1)]
    );
    s.unsubscribe(NQ, CME).await.unwrap();
    assert_eq!(h.count("unsubscribe NQH6 CME").await, 1);
}

async fn a_slow_consumer_gets_a_gap_without_costing_its_peer_events<H: Harness>() {
    let h = H::start(Opts::default()).await;
    let fast = session_for(&h).await;
    let slow = session_for(&h).await;
    fast.subscribe(NQ, CME).await.unwrap();
    slow.subscribe(NQ, CME).await.unwrap();
    // A local attachment starts native delivery on its first poll; leave it
    // unpolled so its outbound queue + broadcast ring are the bounded backlog.
    // The session-backend spec requirement "Identical event records from both
    // modes" preserves each typed gap and source order; gap counts depend on
    // queue depth (host per-session 65,536, remote per-stream EVENT_QUEUE_CAP).
    // Both modes must account for every event, without loss to the healthy peer.
    let total = 2 * rithmic_host::fanout::DEFAULT_QUEUE_CAP + 512;
    for start in (0..total).step_by(128) {
        let expected: Vec<_> = (start..(start + 128).min(total))
            .map(|i| scripted::last_trade(NQ, CME, i as f64, 1))
            .collect();
        for event in &expected {
            h.venue().push_event(Stream::Ticker, event.clone());
        }
        assert_eq!(
            drain(&*fast, expected.len(), market).await,
            expected,
            "fast peer at {start}"
        );
    }
    let mut accounted = 0;
    let mut skipped = 0;
    let mut delivered = 0;
    tokio::time::timeout(Duration::from_secs(15), async {
        while accounted < total {
            let batch = slow
                .next_events(Stream::Ticker, 1024, Duration::from_millis(100))
                .await
                .unwrap();
            for event in batch {
                match event {
                    PlantEvent::Gap(GapDto {
                        stream: Stream::Ticker,
                        skipped: n,
                    }) => {
                        assert!(n > 0, "real queue overflow has an exact count");
                        accounted += n as usize;
                        skipped += n;
                    }
                    event => {
                        assert_eq!(
                            event,
                            scripted::last_trade(NQ, CME, accounted as f64, 1),
                            "gap/event order at {accounted}"
                        );
                        accounted += 1;
                        delivered += 1;
                    }
                }
            }
        }
    })
    .await
    .expect("slow queue drains after the producer stops");
    assert_eq!(
        accounted, total,
        "gaps and records exhaust the source in order"
    );
    assert_eq!(
        delivered + skipped,
        total as u64,
        "delivered + skipped = sent"
    );
    assert!(skipped > 0, "slow consumer must receive a typed gap");
    assert!(
        slow.next_events(Stream::Ticker, 1024, Duration::from_millis(20))
            .await
            .unwrap()
            .is_empty(),
        "no duplicate or trailing records after the source is exhausted"
    );
    eprintln!(
        "{}: slow consumer skipped {skipped} of {total}",
        std::any::type_name::<H>()
    );
    slow.unsubscribe(NQ, CME).await.unwrap();
    fast.unsubscribe(NQ, CME).await.unwrap();
    assert_eq!(h.count("unsubscribe NQH6 CME").await, 1);
}

async fn pnl_intents_are_shared_and_disconnect_is_per_session<H: Harness>() {
    let h = H::start(Opts::default()).await;
    let a = session_for(&h).await;
    let b = session_for(&h).await;
    for s in [&a, &b, &a] {
        s.subscribe_pnl().await.unwrap();
    }
    assert_eq!(h.count("subscribe_pnl").await, 1);
    a.disconnect_pnl_plant().await.unwrap();
    assert_eq!(h.count("disconnect_pnl_plant").await, 0);
    let event = scripted::account_pnl("ACCT", "-12.50");
    h.venue().push_event(Stream::Pnl, event.clone());
    assert_eq!(drain(&*b, 1, pnl).await, [event]);
    assert_eq!(
        a.poll_pnl_event(Duration::from_millis(10)).await.unwrap(),
        None
    );
    b.disconnect_pnl_plant().await.unwrap();
    assert_eq!(h.count("disconnect_pnl_plant").await, 1);
}
