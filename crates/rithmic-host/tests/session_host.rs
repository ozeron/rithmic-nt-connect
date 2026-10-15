//! Host behavior without sockets, protobuf, credentials or live plants.
use rithmic_host::fake::{FakeFailure, FakeVenue};
use rithmic_host::fanout::SubKey;
use rithmic_host::intents::MdIntent;
use rithmic_host::{
    Error, HistoryPolicy, LastTradeDto, LocalSession, ParentGates, PlaceOrder, PlantEvent, Session,
    SessionEvent, SessionHost, Stream, Timing, VenueHandle,
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
#[tokio::test(start_paused = true)]
async fn ticker_failure_restores_only_ticker_and_book_once() {
    let (host, fake, a) = running().await;
    a.subscribe("NQU6", "CME").await.unwrap();
    a.subscribe_order_book_summary("NQU6", "CME").await.unwrap();
    a.subscribe_time_bars("NQU6", "CME", 2, 1).await.unwrap();
    a.subscribe_pnl().await.unwrap();
    a.subscribe_bracket_updates().await.unwrap();
    fake.clear_calls();
    fake.fail_stream(Stream::Ticker, "ticker dropped");
    until(|| host.generation() == 2).await;
    assert_eq!(
        fake.calls(),
        [
            "reset_ticker",
            "subscribe NQU6 CME",
            "subscribe_book NQU6 CME"
        ]
    );
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
    assert_eq!(fake.count("connect"), 0);
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

#[tokio::test(start_paused = true)]
async fn restore_retries_only_the_failed_md_intent_and_then_degrades_it() {
    let (host, fake, a) = running().await;
    a.subscribe("NQU6", "CME").await.unwrap();
    a.subscribe_order_book_summary("NQU6", "CME").await.unwrap();
    fake.clear_calls();
    for _ in 0..3 {
        fake.fail_next("subscribe", FakeFailure::Config("expired contract".into()));
    }
    let mut events = a.session_events();
    fake.fail_stream(Stream::Ticker, "lost ticker");
    resumed(&mut events, 2).await;
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

#[tokio::test(start_paused = true)]
async fn restore_does_not_retry_intents_after_the_last_holder_detaches() {
    for (stream, operation) in [
        (Stream::Ticker, "subscribe NQU6"),
        (Stream::Pnl, "subscribe_pnl"),
    ] {
        let (host, fake, a) = running().await;
        let b = host.attach();
        let mut events = b.session_events();
        match stream {
            Stream::Ticker => a.subscribe("NQU6", "CME").await.unwrap(),
            Stream::Pnl => a.subscribe_pnl().await.unwrap(),
            _ => unreachable!(),
        }
        fake.clear_calls();
        fake.fail_next(
            if stream == Stream::Ticker {
                "subscribe"
            } else {
                operation
            },
            FakeFailure::Config("retry after backoff".into()),
        );
        fake.fail_stream(stream, "lost plant");
        until(|| fake.count(operation) == 1).await;
        a.disconnect().await.unwrap();
        resumed(&mut events, 2).await;
        assert_eq!(
            fake.count(operation),
            1,
            "detached {stream:?} intent replayed"
        );
        assert_eq!(host.state.reconnect.remembered_count().await, 0);
        assert_eq!(fake.count("connect"), 0);
        host.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn account_restore_failure_escalates_to_full_reconnect() {
    let (host, fake, a) = running().await;
    a.subscribe_bracket_updates().await.unwrap();
    fake.clear_calls();
    for _ in 0..3 {
        fake.fail_next(
            "subscribe_order_updates",
            FakeFailure::Config("order unavailable".into()),
        );
    }
    let mut events = a.session_events();
    fake.fail_stream(Stream::Order, "lost order");
    resumed(&mut events, 2).await;
    assert_eq!(fake.count("connect"), 1);
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

async fn resumed(events: &mut tokio::sync::broadcast::Receiver<SessionEvent>, generation: u64) {
    let interrupted = tokio::time::timeout(WAIT, events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(interrupted, SessionEvent::Interrupted { generation: g, .. } if g == generation - 1),
        "{interrupted:?}"
    );
    assert_eq!(
        tokio::time::timeout(WAIT, events.recv())
            .await
            .unwrap()
            .unwrap(),
        SessionEvent::Resumed { generation }
    );
}

#[tokio::test(start_paused = true)]
async fn history_failure_recreates_only_history_and_restores_bars_once() {
    let (host, fake, a) = running().await;
    a.subscribe("NQU6", "CME").await.unwrap();
    a.subscribe_time_bars("NQU6", "CME", 2, 1).await.unwrap();
    a.subscribe_pnl().await.unwrap();
    a.subscribe_bracket_updates().await.unwrap();
    let mut events = a.session_events();
    fake.clear_calls();
    fake.fail_stream(Stream::History, "history lost");
    resumed(&mut events, 2).await;
    assert_eq!(
        fake.calls(),
        ["reset_history", "subscribe_time_bars NQU6 CME 2 1"]
    );
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn pnl_failure_only_resubscribes_pnl() {
    let (host, fake, a) = running().await;
    a.subscribe_pnl().await.unwrap();
    a.subscribe_bracket_updates().await.unwrap();
    let mut events = a.session_events();
    fake.clear_calls();
    fake.fail_stream(Stream::Pnl, "pnl lost");
    resumed(&mut events, 2).await;
    assert_eq!(fake.calls(), ["ensure_pnl_plant", "subscribe_pnl"]);
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn order_failure_reseats_only_order_and_restores_orders_and_brackets_once() {
    let (host, fake, a) = running().await;
    a.subscribe_pnl().await.unwrap();
    a.subscribe_bracket_updates().await.unwrap();
    let mut events = a.session_events();
    fake.clear_calls();
    fake.fail_stream(Stream::Order, "order lost");
    resumed(&mut events, 2).await;
    assert_eq!(
        fake.calls(),
        [
            "disconnect_order_plant",
            "ensure_order_plant",
            "subscribe_order_updates",
            "subscribe_bracket_updates"
        ]
    );
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn login_failure_rebuilds_all_and_restores_each_intent_once() {
    let (host, fake, a) = running().await;
    a.subscribe("NQU6", "CME").await.unwrap();
    a.subscribe_order_book_summary("NQU6", "CME").await.unwrap();
    a.subscribe_time_bars("NQU6", "CME", 2, 1).await.unwrap();
    a.subscribe_pnl().await.unwrap();
    a.subscribe_bracket_updates().await.unwrap();
    let mut events = a.session_events();
    fake.clear_calls();
    fake.drop_plants();
    resumed(&mut events, 2).await;
    assert_eq!(
        fake.calls(),
        [
            "disconnect",
            "connect",
            "subscribe NQU6 CME",
            "subscribe_book NQU6 CME",
            "subscribe_time_bars NQU6 CME 2 1",
            "ensure_pnl_plant",
            "subscribe_pnl",
            "subscribe_order_updates",
            "subscribe_bracket_updates"
        ]
    );
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn every_session_including_an_idle_attachment_gets_ordered_generations() {
    let (host, fake, a) = running().await;
    let b = host.attach();
    let mut ae = a.session_events();
    let mut be = b.session_events();
    for generation in [2, 3] {
        fake.fail_stream(Stream::Ticker, "lost");
        resumed(&mut ae, generation).await;
        resumed(&mut be, generation).await;
    }
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn queued_events_survive_and_next_events_is_empty_while_recovering() {
    let (host, fake, a) = running().await;
    a.subscribe("NQU6", "CME").await.unwrap();
    let mut events = a.session_events();
    fake.set_delay("reset_ticker", Duration::from_secs(10));
    fake.push_event(Stream::Ticker, tick(42));
    fake.fail_stream(Stream::Ticker, "ticker lost after tick");
    assert!(matches!(
        events.recv().await.unwrap(),
        SessionEvent::Interrupted { .. }
    ));
    let batch = a
        .next_events(Stream::Ticker, 100, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(format!("{:?}", batch[0]), format!("{:?}", tick(42)));
    assert!(a
        .next_events(Stream::Ticker, 100, Duration::from_secs(1))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(host.generation(), 1);
    assert_eq!(
        events.recv().await.unwrap(),
        SessionEvent::Resumed { generation: 2 }
    );
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn bounded_recovery_failure_closes_streams_after_queued_events_and_wakes_connect() {
    let (host, fake, a) = running().await;
    a.subscribe("NQU6", "CME").await.unwrap();
    let mut events = a.session_events();
    for _ in 0..3 {
        fake.fail_next("reset_ticker", FakeFailure::Config("offline".into()));
    }
    fake.push_event(Stream::Ticker, tick(7));
    fake.fail_stream(Stream::Ticker, "lost");
    assert!(matches!(
        events.recv().await.unwrap(),
        SessionEvent::Interrupted { generation: 1, .. }
    ));
    let b = a.clone();
    let waiter = tokio::spawn(async move { b.connect().await });
    assert!(matches!(
        events.recv().await.unwrap(),
        SessionEvent::RecoveryFailed {
            generation: 1,
            attempts: 3,
            ..
        }
    ));
    assert!(waiter.await.unwrap().is_err());
    assert_eq!(fake.count("reset_ticker"), 3);
    assert_eq!(host.generation(), 1);
    assert_eq!(
        a.next_events(Stream::Ticker, 10, Duration::ZERO)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        a.next_events(Stream::Ticker, 10, Duration::ZERO).await,
        Err(Error::ChannelClosed { .. })
    ));
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn command_in_flight_at_break_is_unknown_and_never_retried() {
    let (host, fake, a) = running().await;
    a.subscribe_order_updates().await.unwrap();
    fake.set_delay("place_order", Duration::from_secs(20));
    let caller = a.clone();
    let command = tokio::spawn(async move { caller.place_order(&order()).await });
    until(|| fake.count("place_order") == 1).await;
    let mut events = a.session_events();
    fake.fail_stream(Stream::Order, "order lost before reply");
    assert!(matches!(
        command.await.unwrap(),
        Err(Error::OutcomeUnknown(_))
    ));
    resumed(&mut events, 2).await;
    assert_eq!(fake.count("place_order"), 1);
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn joining_and_rejoining_live_pnl_and_book_request_snapshots_without_new_holders() {
    let (host, fake, a) = running().await;
    let b = host.attach();
    a.subscribe_pnl().await.unwrap();
    a.subscribe_order_book_summary("NQU6", "CME").await.unwrap();
    fake.clear_calls();
    b.subscribe_pnl().await.unwrap();
    b.subscribe_order_book_summary("NQU6", "CME").await.unwrap();
    b.subscribe_order_book_summary("NQU6", "CME").await.unwrap(); // gap resync
    b.subscribe_pnl().await.unwrap(); // exec re-arm barrier
    assert_eq!(
        fake.calls(),
        [
            "pnl_snapshot",
            "refresh_book NQU6 CME",
            "refresh_book NQU6 CME",
            "pnl_snapshot"
        ]
    );
    b.disconnect().await.unwrap();
    assert_eq!(fake.count("disconnect_pnl"), 0);
    assert_eq!(fake.count("unsubscribe_book"), 0);
    a.disconnect().await.unwrap();
    assert_eq!(fake.count("disconnect_pnl"), 1);
    assert_eq!(fake.count("unsubscribe_book"), 1);
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn account_retry_does_not_repeat_successful_sibling_joins() {
    let (host, fake, a) = running().await;
    a.subscribe_pnl().await.unwrap();
    a.subscribe_bracket_updates().await.unwrap();
    let mut events = a.session_events();
    fake.clear_calls();
    fake.fail_next(
        "subscribe_bracket_updates",
        FakeFailure::Config("retry".into()),
    );
    fake.drop_plants();
    resumed(&mut events, 2).await;
    assert_eq!(fake.count("subscribe_pnl"), 1);
    assert_eq!(fake.count("subscribe_order_updates"), 1);
    assert_eq!(fake.count("subscribe_bracket_updates"), 2);
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn persistent_account_failure_escalates_then_gives_up_with_no_resume() {
    let (host, fake, a) = running().await;
    a.subscribe_pnl().await.unwrap();
    let mut events = a.session_events();
    fake.clear_calls();
    for _ in 0..9 {
        fake.fail_next("subscribe_pnl", FakeFailure::Config("offline".into()));
    }
    fake.fail_stream(Stream::Pnl, "lost pnl");
    assert!(matches!(
        events.recv().await.unwrap(),
        SessionEvent::Interrupted { .. }
    ));
    assert!(matches!(
        events.recv().await.unwrap(),
        SessionEvent::RecoveryFailed { attempts: 3, .. }
    ));
    assert_eq!(host.generation(), 1);
    assert_eq!(fake.count("connect"), 2);
    assert_eq!(fake.count("subscribe_pnl"), 9);
    assert!(events.try_recv().is_err());
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn failed_join_snapshot_rolls_back_only_the_new_session() {
    let (host, fake, a) = running().await;
    let b = host.attach();
    a.subscribe_pnl().await.unwrap();
    a.subscribe_order_book_summary("NQU6", "CME").await.unwrap();
    fake.fail_next(
        "pnl_snapshot",
        FakeFailure::Config("snapshot unavailable".into()),
    );
    assert!(b.subscribe_pnl().await.is_err());
    fake.fail_next(
        "refresh_book",
        FakeFailure::Config("book unavailable".into()),
    );
    assert!(b.subscribe_order_book_summary("NQU6", "CME").await.is_err());
    assert!(
        host.state
            .reconnect
            .is_degraded(&MdIntent::Book(key()))
            .await
    );
    b.disconnect().await.unwrap();
    assert_eq!(fake.count("disconnect_pnl"), 0);
    assert_eq!(fake.count("unsubscribe_book"), 0);
    // The existing owner can heal a refresh that lost the shared book join.
    a.subscribe_order_book_summary("NQU6", "CME").await.unwrap();
    assert!(
        !host
            .state
            .reconnect
            .is_degraded(&MdIntent::Book(key()))
            .await
    );
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn requested_login_recovery_before_the_pump_starts_is_not_lost() {
    let (host, fake, a) = running().await;
    let mut events = a.session_events();
    fake.clear_calls();
    // No yield since connect: the pump may still be in its initial wait.
    host.state.request_reconnect();
    resumed(&mut events, 2).await;
    assert_eq!(fake.calls(), ["disconnect", "connect"]);
    host.shutdown().await;
}

#[tokio::test]
async fn stalled_local_session_has_exact_gaps_while_its_peer_loses_nothing() {
    let (host, fake, slow) = running().await;
    let fast = host.attach();
    slow.subscribe("NQU6", "CME").await.unwrap();
    fast.subscribe("NQU6", "CME").await.unwrap();
    // Exceed even the old native StreamHub plus forwarding queues. Keep the
    // venue and healthy peer drained so any loss belongs only to this session.
    const TOTAL: i32 = 70_000;
    for n in 0..TOTAL {
        fake.push_event(Stream::Ticker, tick(n));
        let batch = fast.next_events(Stream::Ticker, 1, WAIT).await.unwrap();
        let PlantEvent::LastTrade(trade) = &batch[0] else {
            panic!("unexpected peer record")
        };
        assert_eq!(trade.trade_price, Some(n as f64));
    }
    let mut accounted = 0;
    let mut skipped = 0;
    while accounted < TOTAL as u64 {
        let batch = slow.next_events(Stream::Ticker, 1024, WAIT).await.unwrap();
        assert!(!batch.is_empty(), "missing records at {accounted}");
        for event in batch {
            match event {
                PlantEvent::Gap(gap) => {
                    assert_eq!(gap.stream, Stream::Ticker);
                    assert!(gap.skipped > 0);
                    accounted += gap.skipped;
                    skipped += gap.skipped;
                }
                PlantEvent::LastTrade(trade) => {
                    assert_eq!(trade.trade_price, Some(accounted as f64));
                    accounted += 1;
                }
                other => panic!("unexpected record: {other:?}"),
            }
        }
    }
    assert_eq!(accounted, TOTAL as u64);
    assert!(skipped > 0);
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn lifecycle_barriers_leave_every_topic_queued_before_signalling() {
    let (host, fake, session) = running().await;
    session.subscribe("NQU6", "CME").await.unwrap();
    session.subscribe("ESU6", "CME").await.unwrap();
    let mut signals = session.session_events();
    for n in 0..8 {
        fake.push_event(Stream::Ticker, tick(n));
        let PlantEvent::LastTrade(mut trade) = tick(n) else {
            unreachable!()
        };
        trade.symbol = Some("ESU6".into());
        fake.push_event(Stream::Ticker, PlantEvent::LastTrade(trade));
    }
    fake.fail_stream(Stream::Ticker, "failure after both topics");
    resumed(&mut signals, 2).await;
    // Both signals must follow enqueue, even with no native polling during
    // recovery. A non-blocking read must already have every pre-failure record.
    let mut nq = Vec::new();
    let mut es = Vec::new();
    for _ in 0..16 {
        let batch = session
            .next_events(Stream::Ticker, 1, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(batch.len(), 1);
        let PlantEvent::LastTrade(trade) = &batch[0] else {
            panic!("unexpected record")
        };
        match trade.symbol.as_deref() {
            Some("NQU6") => nq.push(trade.trade_price.unwrap() as i32),
            Some("ESU6") => es.push(trade.trade_price.unwrap() as i32),
            other => panic!("unexpected symbol: {other:?}"),
        }
    }
    assert_eq!(nq, (0..8).collect::<Vec<_>>());
    assert_eq!(es, nq);
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_native_batch_overflow_is_exact_and_partial_takes_keep_source_order() {
    let (host, fake, session) = running().await;
    session.subscribe("NQU6", "CME").await.unwrap();
    let mut signals = session.session_events();
    for n in 0..32 {
        fake.push_event(Stream::Ticker, tick(n));
    }
    fake.fail_stream(Stream::Ticker, "after native batch");
    resumed(&mut signals, 2).await;
    let batch = session
        .next_events(Stream::Ticker, 1, Duration::ZERO)
        .await
        .unwrap();
    assert!(
        matches!(&batch[..], [PlantEvent::Gap(gap)] if gap.stream == Stream::Ticker && gap.skipped == 16)
    );
    for n in 16..32 {
        let batch = session
            .next_events(Stream::Ticker, 1, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(format!("{:?}", batch[0]), format!("{:?}", tick(n)));
    }
    assert!(session
        .next_events(Stream::Ticker, 1, Duration::ZERO)
        .await
        .unwrap()
        .is_empty());
    host.shutdown().await;
}

#[tokio::test]
async fn concurrent_native_readers_wake_for_every_record_in_one_batch() {
    let (host, fake, session) = running().await;
    session.subscribe("NQU6", "CME").await.unwrap();
    let a = session.clone();
    let a = tokio::spawn(async move { a.next_events(Stream::Ticker, 1, WAIT).await.unwrap() });
    let b = session.clone();
    let b = tokio::spawn(async move { b.next_events(Stream::Ticker, 1, WAIT).await.unwrap() });
    tokio::task::yield_now().await;
    fake.push_event(Stream::Ticker, tick(1));
    fake.push_event(Stream::Ticker, tick(2));
    let mut prices = Vec::new();
    for batch in [a.await.unwrap(), b.await.unwrap()] {
        assert_eq!(batch.len(), 1, "a waiting reader missed the batch wakeup");
        let PlantEvent::LastTrade(trade) = &batch[0] else {
            panic!("unexpected record")
        };
        prices.push(trade.trade_price.unwrap() as i32);
    }
    prices.sort_unstable();
    assert_eq!(prices, [1, 2]);
    host.shutdown().await;
}

// Behavioral replacements for the removed topic-forwarder gap helper tests:
// overflow the real attachment queues and check the channels consumers read.
async fn overflow_channels(streams: &[Stream]) {
    let (host, fake) = make(
        HistoryPolicy::Allow,
        ParentGates {
            trading_enabled: true,
            cancel_all_enabled: false,
        },
        2,
    );
    let slow = host.attach();
    let fast = host.attach();
    slow.connect().await.unwrap();
    for session in [&slow, &fast] {
        for stream in streams {
            match stream {
                Stream::Ticker => session.subscribe("NQU6", "CME").await.unwrap(),
                Stream::History => session
                    .subscribe_time_bars("NQU6", "CME", 2, 1)
                    .await
                    .unwrap(),
                Stream::Pnl => session.subscribe_pnl().await.unwrap(),
                Stream::Order => session.subscribe_bracket_updates().await.unwrap(),
            }
        }
    }
    for stream in streams {
        for n in 0..8 {
            let event = match stream {
                Stream::Ticker => tick(n),
                Stream::History => PlantEvent::TimeBar(rithmic_host::HistoryBarDto {
                    symbol: Some("NQU6".into()),
                    exchange: Some("CME".into()),
                    bar_type: Some(2),
                    period: Some("1".into()),
                    marker: Some(n),
                    open_price: Some(1.0),
                    high_price: Some(1.0),
                    low_price: Some(1.0),
                    close_price: Some(1.0),
                    volume: Some(1),
                    num_trades: Some(1),
                    bid_volume: None,
                    ask_volume: None,
                    ts_event_ns: None,
                }),
                Stream::Pnl => PlantEvent::AccountPnl(rithmic_host::AccountPnlDto {
                    account_id: Some("test-account".into()),
                    fcm_id: None,
                    ib_id: None,
                    account_balance: None,
                    cash_on_hand: None,
                    margin_balance: None,
                    day_pnl: Some(n.to_string()),
                    open_position_pnl: None,
                    closed_position_pnl: None,
                    available_buying_power: None,
                    used_buying_power: None,
                    is_snapshot: Some(false),
                    ssboe: None,
                    usecs: None,
                }),
                Stream::Order => PlantEvent::BracketUpdate(rithmic_host::BracketUpdateDto {
                    basket_id: Some(n.to_string()),
                    stop_ticks: Some(10),
                    ..Default::default()
                }),
            };
            fake.push_event(*stream, event.clone());
            assert_eq!(fast.next_events(*stream, 1, WAIT).await.unwrap(), [event]);
        }
    }
    for stream in Stream::ALL {
        let batch = slow.next_events(stream, 16, Duration::ZERO).await.unwrap();
        if streams.contains(&stream) {
            assert!(
                matches!(&batch[..], [PlantEvent::Gap(gap), _, _] if gap.stream == stream && gap.skipped == 6),
                "{stream:?}: {batch:?}"
            );
        } else {
            assert!(batch.is_empty(), "overflow leaked onto {stream:?}");
        }
    }
    host.shutdown().await;
}

#[tokio::test]
async fn a_bar_subscriber_hears_about_the_overflow_on_its_bar_channel() {
    overflow_channels(&[Stream::Ticker, Stream::History]).await;
}

#[tokio::test]
async fn a_ticker_only_subscriber_gets_one_gap() {
    overflow_channels(&[Stream::Ticker]).await;
}

#[tokio::test]
async fn account_topics_report_on_their_own_stream() {
    overflow_channels(&[Stream::Pnl, Stream::Order]).await;
}
