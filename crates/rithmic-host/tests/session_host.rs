//! Host behavior without sockets, protobuf, credentials or live plants.
use rithmic_host::fake::{FakeFailure, FakeVenue};
use rithmic_host::fanout::SubKey;
use rithmic_host::intents::MdIntent;
use rithmic_host::{
    Error, HistoryPolicy, LastTradeDto, LocalSession, ParentGates, PlaceOrder, PlantEvent, Session,
    SessionHost, Stream, Timing, VenueHandle,
};
use std::sync::Arc;
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(3);
fn timing() -> Timing {
    Timing {
        error_backoff: Duration::from_millis(1),
        connect_retry: Duration::from_millis(1),
        restore_backoff: Duration::from_millis(1),
    }
}
fn make(
    policy: HistoryPolicy,
    gates: ParentGates,
    capacity: usize,
) -> (Arc<SessionHost<FakeVenue>>, Arc<FakeVenue>) {
    let fake = Arc::new(FakeVenue::new());
    let host = SessionHost::with_handle(
        Some(VenueHandle::new(fake.clone())),
        gates,
        timing(),
        policy,
        false,
        capacity,
    );
    (host, fake)
}
async fn running() -> (
    Arc<SessionHost<FakeVenue>>,
    Arc<FakeVenue>,
    LocalSession<FakeVenue>,
) {
    let (host, fake) = make(
        HistoryPolicy::Allow,
        ParentGates {
            trading_enabled: true,
            cancel_all_enabled: false,
        },
        16,
    );
    let session = host.attach();
    session.connect().await.unwrap();
    (host, fake, session)
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(WAIT, async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition deadline");
}
fn key() -> SubKey {
    SubKey {
        symbol: "NQU6".into(),
        exchange: "CME".into(),
    }
}
fn tick(n: i32) -> PlantEvent {
    PlantEvent::LastTrade(LastTradeDto {
        symbol: Some("NQU6".into()),
        exchange: Some("CME".into()),
        trade_price: Some(n as f64),
        trade_size: Some(1),
        aggressor: None,
        ssboe: None,
        usecs: None,
        ts_event_ns: None,
        is_snapshot: None,
    })
}
fn order() -> PlaceOrder {
    PlaceOrder {
        symbol: "NQU6".into(),
        exchange: "CME".into(),
        side: "BUY".into(),
        price_type: "LIMIT".into(),
        quantity: 1,
        user_tag: "host-test".into(),
        price: Some(100.0),
        trigger_price: None,
        duration: "DAY".into(),
        trail_by_ticks: None,
        trail_by_price_id: None,
    }
}
fn code(e: Error) -> String {
    match e {
        Error::Gateway { code, .. } => code,
        other => panic!("expected coded error: {other}"),
    }
}

#[tokio::test]
async fn concurrent_connect_is_one_login_and_repeat_connect_is_idempotent() {
    let (host, fake) = make(HistoryPolicy::Allow, ParentGates::default(), 8);
    let a = host.attach();
    let b = host.attach();
    let (a_result, b_result) = tokio::join!(a.connect(), b.connect());
    a_result.unwrap();
    b_result.unwrap();
    a.connect().await.unwrap();
    assert_ne!(a.id(), b.id());
    assert_eq!(fake.count("connect"), 1);
    host.shutdown().await;
}
#[tokio::test]
async fn a_failed_initial_connect_cleans_up_and_can_be_retried() {
    let (host, fake) = make(HistoryPolicy::Allow, ParentGates::default(), 8);
    fake.fail_next("connect", FakeFailure::Config("no login".into()));
    let a = host.attach();
    assert!(a.connect().await.is_err());
    assert_eq!(fake.count("disconnect"), 1);
    a.connect().await.unwrap();
    host.shutdown().await;
}
#[tokio::test]
async fn repeated_md_subscribes_are_sets_and_other_sessions_keep_the_join() {
    let (host, fake, a) = running().await;
    let b = host.attach();
    for s in [&a, &b, &a] {
        s.subscribe("NQU6", "CME").await.unwrap();
        s.subscribe_order_book_summary("NQU6", "CME").await.unwrap();
        s.subscribe_time_bars("NQU6", "CME", 2, 1).await.unwrap();
    }
    for prefix in ["subscribe NQU6", "subscribe_book", "subscribe_time_bars"] {
        assert_eq!(fake.count(prefix), 1);
    }
    a.disconnect().await.unwrap();
    assert_eq!(fake.count("unsubscribe"), 0);
    b.unsubscribe("NQU6", "CME").await.unwrap();
    b.unsubscribe("NQU6", "CME").await.unwrap();
    b.unsubscribe_order_book_summary("NQU6", "CME")
        .await
        .unwrap();
    b.unsubscribe_time_bars("NQU6", "CME", 2, 1).await.unwrap();
    assert_eq!(fake.count("unsubscribe NQU6"), 1);
    assert_eq!(fake.count("unsubscribe_book"), 1);
    assert_eq!(fake.count("unsubscribe_time_bars"), 1);
    assert_eq!(host.state.reconnect.remembered_count().await, 0);
    host.shutdown().await;
}
#[tokio::test]
async fn failed_md_join_rolls_back_but_failed_leave_keeps_intent() {
    let (host, fake, a) = running().await;
    fake.fail_next("subscribe", FakeFailure::Config("join refused".into()));
    assert_eq!(
        code(a.subscribe("NQU6", "CME").await.unwrap_err()),
        "subscribe_failed"
    );
    assert_eq!(host.state.reconnect.remembered_count().await, 0);
    a.subscribe("NQU6", "CME").await.unwrap();
    fake.fail_next("unsubscribe", FakeFailure::Config("leave refused".into()));
    assert_eq!(
        code(a.unsubscribe("NQU6", "CME").await.unwrap_err()),
        "unsubscribe_failed"
    );
    assert_eq!(host.state.reconnect.remembered_count().await, 1);
    a.unsubscribe("NQU6", "CME").await.unwrap();
    assert_eq!(host.state.reconnect.remembered_count().await, 0);
    host.shutdown().await;
}
#[tokio::test]
async fn concurrent_sessions_join_once_and_failed_first_join_does_not_poison_peer() {
    let (host, fake, a) = running().await;
    let b = host.attach();
    fake.fail_next("subscribe", FakeFailure::Config("first fails".into()));
    let (ar, br) = tokio::join!(a.subscribe("NQU6", "CME"), b.subscribe("NQU6", "CME"));
    assert!(ar.is_err() ^ br.is_err());
    assert_eq!(fake.count("subscribe NQU6"), 2);
    let survivor = if ar.is_ok() { &a } else { &b };
    survivor.unsubscribe("NQU6", "CME").await.unwrap();
    assert_eq!(host.state.reconnect.remembered_count().await, 0);
    host.shutdown().await;
}
#[tokio::test]
async fn repeated_subscribe_retries_degraded_join_without_adding_a_holder() {
    let (host, fake, a) = running().await;
    a.subscribe("NQU6", "CME").await.unwrap();
    host.state
        .reconnect
        .mark_degraded(MdIntent::Ticker(key()))
        .await;
    fake.fail_next("subscribe", FakeFailure::Config("expired".into()));
    assert_eq!(
        code(a.subscribe("NQU6", "CME").await.unwrap_err()),
        "subscribe_failed"
    );
    assert!(
        host.state
            .reconnect
            .is_degraded(&MdIntent::Ticker(key()))
            .await
    );
    a.subscribe("NQU6", "CME").await.unwrap();
    assert!(
        !host
            .state
            .reconnect
            .is_degraded(&MdIntent::Ticker(key()))
            .await
    );
    a.unsubscribe("NQU6", "CME").await.unwrap();
    assert_eq!(fake.count("unsubscribe NQU6"), 1);
    host.shutdown().await;
}
#[tokio::test]
async fn newer_session_can_heal_an_existing_degraded_join() {
    let (host, fake, a) = running().await;
    a.subscribe("NQU6", "CME").await.unwrap();
    host.state
        .reconnect
        .mark_degraded(MdIntent::Ticker(key()))
        .await;
    let b = host.attach();
    b.subscribe("NQU6", "CME").await.unwrap();
    assert_eq!(fake.count("subscribe NQU6"), 2);
    a.disconnect().await.unwrap();
    assert_eq!(fake.count("unsubscribe"), 0);
    b.disconnect().await.unwrap();
    assert_eq!(fake.count("unsubscribe NQU6"), 1);
    host.shutdown().await;
}
#[tokio::test]
async fn native_sessions_receive_independent_equal_events() {
    let (host, fake, a) = running().await;
    let b = host.attach();
    a.subscribe("NQU6", "CME").await.unwrap();
    b.subscribe("NQU6", "CME").await.unwrap();
    fake.push_event(Stream::Ticker, tick(7));
    let (ar, br) = tokio::join!(
        a.next_events(Stream::Ticker, 16, WAIT),
        b.next_events(Stream::Ticker, 16, WAIT)
    );
    assert_eq!(format!("{:?}", ar.unwrap()), format!("{:?}", br.unwrap()));
    a.disconnect().await.unwrap();
    fake.push_event(Stream::Ticker, tick(8));
    assert_eq!(
        format!("{:?}", b.poll_event(WAIT).await.unwrap().unwrap()),
        format!("{:?}", tick(8))
    );
    host.shutdown().await;
}

#[tokio::test]
async fn mixed_delivery_keeps_native_events_and_shares_remote_encoding() {
    let (host, fake, native) = running().await;
    assert!(host
        .state
        .hub
        .set_encoder(|_| bytes::Bytes::from_static(b"projected")));
    let remote_a = host.attach();
    let remote_b = host.attach();
    let mut a_events = remote_a.take_events().unwrap();
    let mut b_events = remote_b.take_events().unwrap();
    native.subscribe("NQU6", "CME").await.unwrap();
    remote_a.subscribe("NQU6", "CME").await.unwrap();
    remote_b.subscribe("NQU6", "CME").await.unwrap();

    fake.push_event(Stream::Ticker, tick(7));
    let a = tokio::time::timeout(WAIT, a_events.recv())
        .await
        .unwrap()
        .unwrap();
    let b = tokio::time::timeout(WAIT, b_events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(a.event().is_some());
    let encoded = a.encoded_with(|_| bytes::Bytes::from_static(b"shared"));
    assert_eq!(
        b.encoded_with(|_| panic!("each record must be encoded once")),
        encoded
    );
    assert_eq!(
        format!("{:?}", native.poll_event(WAIT).await.unwrap().unwrap()),
        format!("{:?}", tick(7))
    );

    native.disconnect().await.unwrap();
    drop(native);
    fake.push_event(Stream::Ticker, tick(8));
    let projected = tokio::time::timeout(WAIT, a_events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(projected.event().is_none());
    assert_eq!(
        projected.encoded_with(|_| unreachable!()).as_ref(),
        b"projected"
    );
    host.shutdown().await;
}
#[tokio::test]
async fn explicit_disconnect_and_drop_release_only_their_own_intents() {
    let (host, fake, a) = running().await;
    let b = host.attach();
    for s in [&a, &b] {
        s.subscribe("NQU6", "CME").await.unwrap();
        s.subscribe_pnl().await.unwrap();
        s.subscribe_bracket_updates().await.unwrap();
    }
    a.disconnect().await.unwrap();
    assert_eq!(fake.count("unsubscribe"), 0);
    assert_eq!(fake.count("disconnect_order"), 0);
    drop(b);
    until(|| fake.count("unsubscribe NQU6") == 1).await;
    until(|| fake.count("disconnect_order_plant") == 1).await;
    assert_eq!(host.state.reconnect.remembered_count().await, 0);
    host.shutdown().await;
}
#[tokio::test]
async fn dropping_a_session_does_not_wait_for_venue_io() {
    let (host, fake, a) = running().await;
    a.subscribe_order_updates().await.unwrap();
    fake.set_delay("disconnect_order_plant", Duration::from_secs(1));
    let started = std::time::Instant::now();
    drop(a);
    assert!(started.elapsed() < Duration::from_millis(100));
    until(|| fake.count("disconnect_order_plant") == 1).await;
    host.shutdown().await;
}
#[tokio::test]
async fn one_session_disconnects_orders_while_another_still_trades() {
    let (host, fake, a) = running().await;
    let b = host.attach();
    a.subscribe_order_updates().await.unwrap();
    b.subscribe_order_updates().await.unwrap();
    a.disconnect_order_plant().await.unwrap();
    assert_eq!(fake.count("disconnect_order_plant"), 0);
    assert!(matches!(
        a.place_order(&order()).await,
        Err(Error::NotSent(_))
    ));
    b.place_order(&order()).await.unwrap();
    a.ensure_order_plant().await.unwrap();
    a.place_order(&order()).await.unwrap();
    assert_eq!(fake.count("place_order"), 2);
    host.shutdown().await;
}
#[tokio::test]
async fn disconnect_without_an_intent_still_closes_this_sessions_command_gate() {
    let (host, fake, a) = running().await;
    a.disconnect_order_plant().await.unwrap();
    assert!(matches!(
        a.place_order(&order()).await,
        Err(Error::NotSent(_))
    ));
    assert_eq!(fake.count("place_order"), 0);
    a.subscribe_order_updates().await.unwrap();
    a.place_order(&order()).await.unwrap();
    host.shutdown().await;
}
#[tokio::test]
async fn repeated_order_bracket_and_pnl_subscriptions_are_sets() {
    let (host, fake, a) = running().await;
    for _ in 0..2 {
        a.subscribe_order_updates().await.unwrap();
        a.subscribe_bracket_updates().await.unwrap();
        a.subscribe_pnl().await.unwrap();
    }
    for method in [
        "subscribe_order_updates",
        "subscribe_bracket_updates",
        "subscribe_pnl",
    ] {
        assert_eq!(fake.count(method), 1);
    }
    a.disconnect_order_plant().await.unwrap();
    a.disconnect_pnl_plant().await.unwrap();
    assert_eq!(host.state.reconnect.remembered_count().await, 0);
    host.shutdown().await;
}
#[tokio::test]
async fn a_failed_bracket_join_rolls_back_its_order_and_bracket_introductions() {
    let (host, fake, a) = running().await;
    fake.fail_next(
        "subscribe_bracket_updates",
        FakeFailure::Config("no brackets".into()),
    );
    assert_eq!(
        code(a.subscribe_bracket_updates().await.unwrap_err()),
        "subscribe_bracket_updates_failed"
    );
    assert_eq!(host.state.reconnect.remembered_count().await, 0);
    a.subscribe_bracket_updates().await.unwrap();
    assert_eq!(fake.count("subscribe_order_updates"), 2);
    host.shutdown().await;
}
#[tokio::test]
async fn failed_explicit_account_leave_retains_restore_intent() {
    let (host, fake, a) = running().await;
    a.subscribe_bracket_updates().await.unwrap();
    a.subscribe_pnl().await.unwrap();
    fake.fail_next("disconnect_order_plant", FakeFailure::Config("busy".into()));
    assert_eq!(
        code(a.disconnect_order_plant().await.unwrap_err()),
        "disconnect_order_failed"
    );
    let plan = host.state.reconnect.restore_plan().await;
    assert!(plan.order && plan.brackets);
    assert!(matches!(
        a.place_order(&order()).await,
        Err(Error::NotSent(_))
    ));
    fake.fail_next("disconnect_pnl_plant", FakeFailure::Config("busy".into()));
    assert_eq!(
        code(a.disconnect_pnl_plant().await.unwrap_err()),
        "disconnect_pnl_failed"
    );
    assert!(host.state.reconnect.restore_plan().await.pnl);
    host.shutdown().await;
}
#[tokio::test]
async fn history_policy_is_chosen_by_the_root_and_live_md_state_is_host_wide() {
    for policy in [HistoryPolicy::Allow, HistoryPolicy::RefuseUnderLiveMd] {
        let (host, fake) = make(policy, ParentGates::default(), 8);
        let a = host.attach();
        let b = host.attach();
        a.connect().await.unwrap();
        a.subscribe("NQU6", "CME").await.unwrap();
        let result = b.load_ticks("NQU6", "CME", 0, 1).await;
        if policy == HistoryPolicy::Allow {
            result.unwrap();
            assert_eq!(fake.count("load_ticks"), 1);
        } else {
            assert_eq!(code(result.unwrap_err()), "history_denied_live_md");
            assert_eq!(fake.count("load_ticks"), 0);
        }
        host.shutdown().await;
    }
}
#[tokio::test]
async fn closed_host_gates_cannot_be_bypassed_by_rust_consumers() {
    let (host, fake) = make(HistoryPolicy::Allow, ParentGates::default(), 8);
    let a = host.attach();
    a.connect().await.unwrap();
    fake.clear_calls();
    for result in [
        a.place_order(&order()).await,
        a.cancel_order("basket").await,
        a.ensure_order_plant().await,
        a.subscribe_order_updates().await,
        a.subscribe_bracket_updates().await,
        a.load_orders(0, 0).await.map(|_| ()),
        a.load_fill_history(0, 0).await.map(|_| ()),
        a.load_order_history("20261015").await.map(|_| ()),
        a.load_order_history_dates().await.map(|_| ()),
        a.load_product_rms_info().await.map(|_| ()),
        a.load_account_rms_info().await.map(|_| ()),
    ] {
        assert_eq!(code(result.unwrap_err()), "trading_disabled");
    }
    assert_eq!(
        code(a.cancel_all_orders().await.unwrap_err()),
        "cancel_all_denied"
    );
    assert!(fake.calls().is_empty());
    host.shutdown().await;
}
#[tokio::test]
async fn cancel_all_has_its_own_gate_and_one_shots_never_note_intent() {
    let (host, fake) = make(
        HistoryPolicy::Allow,
        ParentGates {
            trading_enabled: false,
            cancel_all_enabled: true,
        },
        8,
    );
    let a = host.attach();
    a.connect().await.unwrap();
    a.cancel_all_orders().await.unwrap();
    assert_eq!(fake.count("cancel_all_orders"), 1);
    assert_eq!(
        code(a.place_order(&order()).await.unwrap_err()),
        "trading_disabled"
    );
    assert_eq!(host.state.reconnect.remembered_count().await, 0);
    host.shutdown().await;
}
#[tokio::test]
async fn a_slow_session_gets_an_exact_gap_without_losing_healthy_peers_events() {
    let (host, fake) = make(HistoryPolicy::Allow, ParentGates::default(), 2);
    let slow = host.attach();
    let fast = host.attach();
    slow.connect().await.unwrap();
    slow.subscribe("NQU6", "CME").await.unwrap();
    fast.subscribe("NQU6", "CME").await.unwrap();
    let mut slow_rx = slow.take_events().unwrap();
    let mut fast_rx = fast.take_events().unwrap();
    for i in 0..32 {
        fake.push_event(Stream::Ticker, tick(i));
        let fan = tokio::time::timeout(WAIT, fast_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            format!("{:?}", fan.event().unwrap()),
            format!("{:?}", tick(i))
        );
    }
    let mut received = 0;
    let mut skipped = 0;
    while received + skipped < 32 {
        let fan = tokio::time::timeout(WAIT, slow_rx.recv())
            .await
            .unwrap()
            .unwrap();
        match fan.event().unwrap() {
            PlantEvent::Gap(g) => skipped += g.skipped,
            _ => received += 1,
        }
    }
    assert!(skipped > 0);
    assert_eq!(received + skipped, 32);
    host.shutdown().await;
}
#[tokio::test]
async fn a_ticker_failure_still_rebuilds_all_plants_and_restores_every_intent() {
    let (host, fake, a) = running().await;
    a.subscribe("NQU6", "CME").await.unwrap();
    a.subscribe_order_book_summary("NQU6", "CME").await.unwrap();
    a.subscribe_time_bars("NQU6", "CME", 2, 1).await.unwrap();
    a.subscribe_pnl().await.unwrap();
    a.subscribe_bracket_updates().await.unwrap();
    fake.clear_calls();
    fake.fail_stream(Stream::Ticker, "ticker dropped");
    until(|| host.generation() == 2).await;
    for method in [
        "disconnect",
        "connect",
        "subscribe NQU6",
        "subscribe_book",
        "subscribe_time_bars",
        "subscribe_pnl",
        "subscribe_order_updates",
        "subscribe_bracket_updates",
    ] {
        assert_eq!(fake.count(method), 1, "{method}: {:?}", fake.calls());
    }
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn connect_during_restore_waits_for_the_host_without_an_extra_login() {
    let (host, fake, a) = running().await;
    a.subscribe("NQU6", "CME").await.unwrap();
    fake.clear_calls();
    fake.fail_next(
        "subscribe",
        FakeFailure::Config("retry this restore".into()),
    );
    fake.fail_stream(Stream::Ticker, "lost ticker");
    until(|| fake.count("subscribe NQU6") == 1).await;
    assert!(!host.state.ready.load(std::sync::atomic::Ordering::SeqCst));
    let caller = a.clone();
    let waiter = tokio::spawn(async move { caller.connect().await });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    tokio::time::advance(Duration::from_millis(2)).await;
    waiter.await.unwrap().unwrap();
    assert_eq!(fake.count("connect"), 1);
    assert_eq!(host.generation(), 2);
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn overlapping_order_disconnects_keep_commands_closed_until_both_finish() {
    let (host, fake, a) = running().await;
    a.subscribe_order_updates().await.unwrap();
    fake.set_delay("disconnect_order_plant", Duration::from_secs(1));
    let first = a.clone();
    let first = tokio::spawn(async move { first.disconnect_order_plant().await });
    tokio::task::yield_now().await;
    let second = a.clone();
    let second = tokio::spawn(async move { second.disconnect_order_plant().await });
    tokio::task::yield_now().await;
    assert!(!first.is_finished() && !second.is_finished());
    assert!(matches!(
        a.place_order(&order()).await,
        Err(Error::NotSent(_))
    ));
    tokio::time::advance(Duration::from_secs(1)).await;
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert!(matches!(
        a.place_order(&order()).await,
        Err(Error::NotSent(_))
    ));
    a.ensure_order_plant().await.unwrap();
    a.place_order(&order()).await.unwrap();
    host.shutdown().await;
}

#[tokio::test]
async fn restore_retries_only_the_failed_md_intent_and_then_degrades_it() {
    let (host, fake, a) = running().await;
    a.subscribe("NQU6", "CME").await.unwrap();
    a.subscribe_order_book_summary("NQU6", "CME").await.unwrap();
    fake.clear_calls();
    for _ in 0..3 {
        fake.fail_next("subscribe", FakeFailure::Config("expired contract".into()));
    }
    fake.fail_stream(Stream::Ticker, "lost ticker");
    until(|| host.generation() == 2).await;
    assert_eq!(fake.count("subscribe NQU6"), 3);
    assert_eq!(fake.count("subscribe_book"), 1);
    assert!(
        host.state
            .reconnect
            .is_degraded(&MdIntent::Ticker(key()))
            .await
    );
    a.subscribe("NQU6", "CME").await.unwrap();
    assert_eq!(fake.count("subscribe NQU6"), 4);
    assert_eq!(host.state.reconnect.degraded_count().await, 0);
    host.shutdown().await;
}

#[tokio::test]
async fn account_restore_failure_escalates_to_another_full_reconnect() {
    let (host, fake, a) = running().await;
    a.subscribe_bracket_updates().await.unwrap();
    fake.clear_calls();
    for _ in 0..3 {
        fake.fail_next(
            "subscribe_order_updates",
            FakeFailure::Config("order unavailable".into()),
        );
    }
    fake.fail_stream(Stream::Ticker, "lost ticker");
    until(|| host.generation() == 2).await;
    assert_eq!(fake.count("connect"), 2);
    assert_eq!(fake.count("subscribe_order_updates"), 4);
    assert_eq!(
        fake.count("subscribe_bracket_updates"),
        1,
        "brackets must wait for order updates to restore"
    );
    host.shutdown().await;
}

#[tokio::test]
async fn queued_events_drain_before_the_detached_session_closes() {
    let (host, fake, a) = running().await;
    a.subscribe("NQU6", "CME").await.unwrap();
    fake.push_event(Stream::Ticker, tick(9));
    // Ensure it reached this session's queue before detaching it.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    a.disconnect().await.unwrap();
    assert_eq!(
        format!("{:?}", a.poll_event(WAIT).await.unwrap().unwrap()),
        format!("{:?}", tick(9))
    );
    assert!(matches!(
        a.poll_event(WAIT).await,
        Err(Error::ChannelClosed { .. })
    ));
    host.shutdown().await;
}
