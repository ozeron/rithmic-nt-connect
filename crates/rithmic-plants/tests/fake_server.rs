//! The unmodified direct session against the fake Rithmic venue.

use rithmic_host::{PlantSet, Venue};
use rithmic_plants::fake_server::{template, FakeRithmicServer, Plant};
use rithmic_plants::RithmicVenue;

/// A venue that answers the history readiness probe (front month + empty replay).
async fn venue() -> FakeRithmicServer {
    let venue = FakeRithmicServer::start().await;
    venue.front_month("ES", "CME", "ESZ6");
    venue
}

#[tokio::test]
async fn direct_session_connects_logs_in_and_disconnects() {
    let venue = venue().await;
    let session = RithmicVenue::new(venue.session_config(), PlantSet::MARKET_DATA);

    session
        .connect()
        .await
        .expect("connect against the fake venue");

    let logins = venue.received_of(template::LOGIN);
    let plants: Vec<_> = logins.iter().filter_map(|r| r.plant).collect();
    assert!(
        plants.contains(&Plant::Ticker),
        "ticker logged in: {plants:?}"
    );
    assert!(
        plants.contains(&Plant::History),
        "history logged in: {plants:?}"
    );
    // The history readiness probe ran before connect returned (an empty replay
    // is retried, so it may run more than once).
    assert!(!venue.received_of(template::TIME_BAR_REPLAY).is_empty());

    session.disconnect().await.expect("disconnect");
    venue.wait_for(template::LOGOUT, 2).await;
}

use std::time::Duration;

use rithmic_host::{Error, PlaceOrder, PlantEvent, Stream};
use rithmic_plants::fake_server::{frames, Reply};

const WAIT: Duration = Duration::from_secs(5);

async fn connected(venue: &FakeRithmicServer, plants: PlantSet) -> RithmicVenue {
    let session = RithmicVenue::new(venue.session_config(), plants).with_gates(true, false);
    session
        .connect()
        .await
        .expect("connect against the fake venue");
    session
}

async fn next_event(session: &RithmicVenue, stream: Stream) -> PlantEvent {
    let events = session
        .next_events(stream, 1, WAIT)
        .await
        .expect("next_events");
    events.into_iter().next().expect("an event within the wait")
}

fn order(tag: &str) -> PlaceOrder {
    PlaceOrder {
        symbol: "ESZ6".into(),
        exchange: "CME".into(),
        side: "BUY".into(),
        price_type: "LIMIT".into(),
        quantity: 1,
        user_tag: tag.into(),
        price: Some(5000.0),
        trigger_price: None,
        duration: "DAY".into(),
        trail_by_ticks: None,
        trail_by_price_id: None,
    }
}

#[tokio::test]
async fn ticker_subscribe_and_trade_through_the_real_decode() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::MARKET_DATA).await;

    session.subscribe("ESZ6", "CME").await.expect("subscribe");
    assert_eq!(venue.received_of(template::MARKET_DATA_UPDATE).len(), 1);

    venue.push(
        Plant::Ticker,
        &frames::last_trade("ESZ6", "CME", 5001.25, 3, 1_790_000_000),
    );
    match next_event(&session, Stream::Ticker).await {
        PlantEvent::LastTrade(t) => {
            assert_eq!(t.symbol.as_deref(), Some("ESZ6"));
            assert_eq!(t.trade_price, Some(5001.25));
            assert_eq!(t.trade_size, Some(3));
            assert_eq!(t.ts_event_ns, Some(1_790_000_000_000_000_000));
        }
        other => panic!("expected a trade, got {other:?}"),
    }
    session.disconnect().await.unwrap();
}

#[tokio::test]
async fn order_plant_place_and_notification() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::MARKET_DATA).await;

    session
        .subscribe_order_updates()
        .await
        .expect("subscribe order updates");
    assert_eq!(venue.connections(Plant::Order), 1);
    session.place_order(&order("t1")).await.expect("place");
    let placed = venue.received_of(template::NEW_ORDER);
    assert_eq!(placed.len(), 1);
    let req: rithmic_rs::rti::RequestNewOrder = placed[0].decode();
    assert_eq!(req.user_tag.as_deref(), Some("t1"));

    venue.push(Plant::Order, &frames::rithmic_order("B1", "t1", 13, "open"));
    match next_event(&session, Stream::Order).await {
        PlantEvent::OrderNotification(n) => {
            assert_eq!(n.basket_id.as_deref(), Some("B1"));
            assert_eq!(n.user_tag.as_deref(), Some("t1"));
        }
        other => panic!("expected an order notification, got {other:?}"),
    }
    session.disconnect().await.unwrap();
}

#[tokio::test]
async fn venue_reject_is_rejected_and_silence_is_unknown() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::MARKET_DATA).await;
    session.ensure_order_plant().await.expect("order plant");

    venue.script(
        template::NEW_ORDER,
        Reply::reject(template::NEW_ORDER, "3", "bad price"),
    );
    let err = session.place_order(&order("rej")).await.unwrap_err();
    assert!(matches!(err, Error::Rejected(_)), "reject class: {err:?}");

    venue.script(template::CANCEL_ORDER, Reply::drop_connection());
    let err = session.cancel_order("B1").await.unwrap_err();
    assert!(
        matches!(err, Error::OutcomeUnknown(_)),
        "unknown class: {err:?}"
    );
}

#[tokio::test]
async fn history_ticks_replay_rows() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::MARKET_DATA).await;

    venue.script(
        template::TICK_BAR_REPLAY,
        Reply::rows(
            template::TICK_BAR_REPLAY + 1,
            &[
                frames::tick_row("ESZ6", 5000.0, 1, 1_790_000_000, 0),
                frames::tick_row("ESZ6", 5000.25, 2, 1_790_000_001, 0),
            ],
        ),
    );
    let ticks = session
        .load_ticks("ESZ6", "CME", 1_790_000_000, 1_790_000_100)
        .await
        .expect("load ticks");
    assert_eq!(ticks.len(), 2);
    assert_eq!(ticks[1].close_price, Some(5000.25));
}

#[tokio::test]
async fn pnl_subscribe_snapshot_and_update() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::EXECUTION).await;

    session.subscribe_pnl().await.expect("subscribe pnl");
    assert_eq!(venue.received_of(template::PNL_POSITION_UPDATES).len(), 1);
    assert_eq!(venue.received_of(template::PNL_POSITION_SNAPSHOT).len(), 1);

    venue.push(Plant::Pnl, &frames::account_pnl("ACCOUNT", "12.5", true));
    match next_event(&session, Stream::Pnl).await {
        PlantEvent::AccountPnl(p) => assert_eq!(p.day_pnl.as_deref(), Some("12.5")),
        other => panic!("expected account pnl, got {other:?}"),
    }
}

#[tokio::test]
async fn ticker_drop_surfaces_as_a_stream_failure() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::MARKET_DATA).await;

    venue.drop_plant(Plant::Ticker);
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        match session
            .next_events(Stream::Ticker, 8, Duration::from_millis(200))
            .await
        {
            Err(_) => break,
            Ok(_) => assert!(
                tokio::time::Instant::now() < deadline,
                "no failure after the drop"
            ),
        }
    }
}

#[tokio::test]
async fn partial_connect_logs_out_what_already_logged_in() {
    let venue = venue().await;
    venue.refuse_logins(Plant::History, 1);
    let session = RithmicVenue::new(venue.session_config(), PlantSet::MARKET_DATA);

    session.connect().await.expect_err("history login refused");

    let logouts = venue.wait_for(template::LOGOUT, 1).await;
    assert!(
        logouts.iter().any(|r| r.plant == Some(Plant::Ticker)),
        "the ticker that logged in is logged out again"
    );
    venue.wait_connections(Plant::Ticker, 0).await;
}

/// A ticker reconnect whose login takes `delay`, started in the background.
fn slow_ticker_reset(
    venue: &FakeRithmicServer,
    session: &std::sync::Arc<RithmicVenue>,
    delay: Duration,
) -> tokio::task::JoinHandle<rithmic_host::Result<()>> {
    venue.script(template::LOGIN, Reply::ok(template::LOGIN).after(delay));
    let s = session.clone();
    tokio::spawn(async move { s.reset_ticker().await })
}

#[tokio::test]
async fn cancel_does_not_wait_behind_a_ticker_reset() {
    let venue = venue().await;
    let session = std::sync::Arc::new(connected(&venue, PlantSet::MARKET_DATA).await);
    session.ensure_order_plant().await.unwrap();

    let reset = slow_ticker_reset(&venue, &session, Duration::from_secs(3));
    venue.wait_for(template::LOGOUT, 1).await; // the reset is under way

    let started = tokio::time::Instant::now();
    session.cancel_order("B1").await.expect("cancel");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "cancel waited {:?} behind the ticker reset",
        started.elapsed()
    );
    reset.await.unwrap().expect("reset completes");
}

#[tokio::test]
async fn resolved_account_does_not_wait_behind_a_ticker_reset() {
    let venue = venue().await;
    let session = std::sync::Arc::new(connected(&venue, PlantSet::MARKET_DATA).await);

    let reset = slow_ticker_reset(&venue, &session, Duration::from_secs(3));
    venue.wait_for(template::LOGOUT, 1).await;

    let account = tokio::time::timeout(Duration::from_secs(1), session.resolved_account())
        .await
        .expect("resolved_account answered during the reset");
    assert_eq!(account.map(|a| a.account_id), Some("ACCOUNT".into()));
    reset.await.unwrap().expect("reset completes");
}

#[tokio::test]
async fn failed_resubscribe_keeps_a_live_order_plant() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::MARKET_DATA).await;
    session
        .subscribe_order_updates()
        .await
        .expect("first subscribe");
    session.subscribe_bracket_updates().await.expect("brackets");

    venue.script(
        template::SUBSCRIBE_ORDER_UPDATES,
        Reply::reject(template::SUBSCRIBE_ORDER_UPDATES, "5", "try later"),
    );
    session
        .subscribe_order_updates()
        .await
        .expect_err("re-subscribe refused");

    // The plant this call did not open stays up: no order-plant logout, and
    // orders still go out on it.
    assert!(venue
        .received_of(template::LOGOUT)
        .iter()
        .all(|r| r.plant != Some(Plant::Order)));
    assert_eq!(venue.connections(Plant::Order), 1);
    session
        .place_order(&order("after"))
        .await
        .expect("place on the live plant");
}

#[tokio::test]
async fn failed_first_subscribe_closes_the_plant_it_opened() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::MARKET_DATA).await;
    venue.script(
        template::SUBSCRIBE_ORDER_UPDATES,
        Reply::reject(template::SUBSCRIBE_ORDER_UPDATES, "5", "try later"),
    );
    session
        .subscribe_order_updates()
        .await
        .expect_err("subscribe refused");
    venue.wait_connections(Plant::Order, 0).await;
}

#[tokio::test]
async fn tick_replay_with_an_error_part_way_is_an_error() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::MARKET_DATA).await;
    venue.script(
        template::TICK_BAR_REPLAY,
        Reply::rows_then(
            template::TICK_BAR_REPLAY + 1,
            &[frames::tick_row("ESZ6", 5000.0, 1, 1_790_000_000, 0)],
            &["3", "replay aborted"],
        ),
    );
    let err = session
        .load_ticks("ESZ6", "CME", 1_790_000_000, 1_790_000_100)
        .await
        .expect_err("a partial replay is not a complete answer");
    assert!(err.to_string().contains("partial"), "{err}");
}

#[tokio::test]
async fn fill_history_with_an_error_part_way_is_unavailable() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::MARKET_DATA).await;
    venue.script(
        template::SHOW_FILL_HISTORY,
        Reply::rows_then(
            template::SHOW_FILL_HISTORY + 1,
            &[frames::fill_row("F1", "B1", 5000.0, 1)],
            &["3", "replay aborted"],
        ),
    );
    let err = session
        .load_fill_history(20261014, 20261014)
        .await
        .expect_err("partial fill history");
    assert!(
        matches!(err, Error::ReconciliationUnavailable(_)),
        "{err:?}"
    );
}

#[tokio::test]
async fn complete_fill_history_is_returned() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::MARKET_DATA).await;
    venue.script(
        template::SHOW_FILL_HISTORY,
        Reply::rows(
            template::SHOW_FILL_HISTORY + 1,
            &[frames::fill_row("F1", "B1", 5000.0, 1)],
        ),
    );
    let fills = session.load_fill_history(20261014, 20261014).await.unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].fill_id.as_deref(), Some("F1"));
}

#[tokio::test]
async fn concurrent_order_loads_do_not_collect_each_others_rows() {
    let venue = venue().await;
    let session = std::sync::Arc::new(connected(&venue, PlantSet::MARKET_DATA).await);
    session.subscribe_order_updates().await.unwrap();

    venue.script(
        template::SHOW_ORDERS,
        Reply::ok_then_replay(
            template::SHOW_ORDERS,
            &[frames::rithmic_order("WORKING-1", "w", 13, "open")],
        )
        .after(Duration::from_millis(100)),
    );
    venue.script(
        template::SHOW_ORDER_HISTORY_SUMMARY,
        Reply::ok_then_replay(
            template::SHOW_ORDER_HISTORY_SUMMARY,
            &[frames::rithmic_order("HISTORY-1", "h", 15, "complete")],
        )
        .after(Duration::from_millis(100)),
    );
    let (working, history) = tokio::join!(
        session.load_orders(0, 0),
        session.load_order_history("20261013")
    );
    let ids = |rows: Vec<rithmic_host::OrderNotificationDto>| -> Vec<String> {
        rows.into_iter().filter_map(|r| r.basket_id).collect()
    };
    assert_eq!(ids(working.unwrap()), vec!["WORKING-1".to_string()]);
    assert_eq!(ids(history.unwrap()), vec!["HISTORY-1".to_string()]);
}

#[tokio::test]
async fn trade_time_is_the_exchange_time_when_the_venue_sends_it() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::MARKET_DATA).await;
    session.subscribe("ESZ6", "CME").await.unwrap();

    let mut trade = frames::last_trade("ESZ6", "CME", 5001.0, 1, 1_790_000_001);
    trade.source_ssboe = Some(1_790_000_000);
    trade.source_usecs = Some(999_000);
    trade.source_nsecs = Some(999_000_123);
    venue.push(Plant::Ticker, &trade);
    match next_event(&session, Stream::Ticker).await {
        PlantEvent::LastTrade(t) => {
            assert_eq!(t.ts_event_ns, Some(1_790_000_000_999_000_123));
            // Rithmic's own stamp is still carried for diagnostics.
            assert_eq!(t.ssboe, Some(1_790_000_001));
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn history_added_mid_session_is_probed_before_use() {
    let venue = venue().await;
    let ticker_only = PlantSet {
        ticker: true,
        history: false,
        pnl: false,
    };
    let session = connected(&venue, ticker_only).await;
    assert!(venue.received_of(template::TIME_BAR_REPLAY).is_empty());

    session
        .request_plants(PlantSet::MARKET_DATA)
        .await
        .expect("history added");
    assert!(
        !venue.received_of(template::TIME_BAR_REPLAY).is_empty(),
        "the readiness probe ran before history was advertised"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deliberate_order_plant_disconnect_never_reads_as_a_closed_channel() {
    let venue = venue().await;
    let session = std::sync::Arc::new(connected(&venue, PlantSet::MARKET_DATA).await);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    // The gateway pump reads the order lane the whole time; a closed channel
    // there means "plant connection lost" and rebuilds every plant.
    let consumer = {
        let (session, stop) = (session.clone(), stop.clone());
        tokio::spawn(async move {
            let mut errors = Vec::new();
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                if let Err(e) = session
                    .next_events(Stream::Order, 64, Duration::from_millis(20))
                    .await
                {
                    errors.push(e.to_string());
                }
            }
            errors
        })
    };
    for _ in 0..15 {
        session.subscribe_order_updates().await.unwrap();
        session.disconnect_order_plant().await.unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let errors = consumer.await.unwrap();
    assert!(errors.is_empty(), "the order lane reported: {errors:?}");
}

#[tokio::test]
async fn the_first_order_command_during_a_ticker_reset_waits_for_it() {
    let venue = venue().await;
    // No order plant yet: the place has to bring it up, which needs the ticker.
    let session = std::sync::Arc::new(connected(&venue, PlantSet::MARKET_DATA).await);

    let reset = slow_ticker_reset(&venue, &session, Duration::from_millis(500));
    venue.wait_for(template::LOGOUT, 1).await;

    session
        .place_order(&order("during-reset"))
        .await
        .expect("waited for the reset, then placed");
    reset.await.unwrap().expect("reset completes");
}

#[tokio::test]
async fn plants_requested_during_a_ticker_reset_are_attached() {
    let venue = venue().await;
    let ticker_only = PlantSet {
        ticker: true,
        history: false,
        pnl: false,
    };
    let session = std::sync::Arc::new(connected(&venue, ticker_only).await);
    assert_eq!(venue.connections(Plant::History), 0);

    let reset = slow_ticker_reset(&venue, &session, Duration::from_millis(500));
    venue.wait_for(template::LOGOUT, 1).await;

    session
        .request_plants(PlantSet::MARKET_DATA)
        .await
        .expect("request plants");
    venue.wait_connections(Plant::History, 1).await;
    reset.await.unwrap().expect("reset completes");
}

#[tokio::test]
async fn order_plant_reads_after_an_explicit_disconnect_do_not_reopen_it() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::MARKET_DATA).await;
    session.ensure_order_plant().await.expect("ensure");
    session
        .disconnect_order_plant()
        .await
        .expect("disconnect order plant");
    venue.wait_connections(Plant::Order, 0).await;

    let err = session
        .load_fill_history(1_790_000_000, 1_790_000_100)
        .await
        .expect_err("refused while the plant is down on request");
    assert!(matches!(err, Error::NotSent(_)), "{err}");
    assert_eq!(
        venue.connections(Plant::Order),
        0,
        "the read re-opened the plant"
    );

    session.ensure_order_plant().await.expect("ensure again");
    session
        .load_fill_history(1_790_000_000, 1_790_000_100)
        .await
        .expect("reads work once the plant is ensured");
}

#[tokio::test]
async fn history_reset_logs_out_and_logs_in_only_history() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::EXECUTION).await;
    session.subscribe_order_updates().await.unwrap();
    let logins = venue.received_of(template::LOGIN).len();
    let logouts = venue.received_of(template::LOGOUT).len();
    rithmic_host::Venue::reset_history(&session).await.unwrap();
    let new_logins = venue.received_of(template::LOGIN);
    let new_logouts = venue.received_of(template::LOGOUT);
    assert_eq!(new_logins.len(), logins + 1);
    assert_eq!(new_logouts.len(), logouts + 1);
    assert_eq!(new_logins.last().unwrap().plant, Some(Plant::History));
    assert_eq!(new_logouts.last().unwrap().plant, Some(Plant::History));
    session.cancel_order("B1").await.unwrap();
    session.disconnect().await.unwrap();
}

#[tokio::test]
async fn join_snapshot_requests_use_the_real_pnl_and_book_protocol() {
    let venue = venue().await;
    let session = connected(&venue, PlantSet::EXECUTION).await;
    session.subscribe_pnl().await.unwrap();
    rithmic_host::Venue::pnl_snapshot(&session).await.unwrap();
    assert_eq!(venue.received_of(template::PNL_POSITION_UPDATES).len(), 1);
    assert_eq!(venue.received_of(template::PNL_POSITION_SNAPSHOT).len(), 2);
    session
        .subscribe_order_book_summary("ESZ6", "CME")
        .await
        .unwrap();
    rithmic_host::Venue::refresh_book(&session, "ESZ6", "CME")
        .await
        .unwrap();
    let requests = venue.received_of(template::MARKET_DATA_UPDATE);
    assert_eq!(requests.len(), 3);
    use rithmic_rs::rti::{request_market_data_update::Request, RequestMarketDataUpdate};
    let requests: Vec<RequestMarketDataUpdate> = requests.iter().map(|r| r.decode()).collect();
    assert_eq!(
        requests.iter().map(|r| r.request).collect::<Vec<_>>(),
        [
            Some(Request::Subscribe as i32),
            Some(Request::Unsubscribe as i32),
            Some(Request::Subscribe as i32)
        ]
    );
    session.disconnect().await.unwrap();
}

/// The live fault hook must traverse rithmic-rs, not synthesize a host failure.
#[tokio::test]
async fn injected_ticker_abort_restores_host_intent_without_reconnecting_siblings() {
    use rithmic_host::gates::ParentGates;
    use rithmic_host::host::HistoryPolicy;
    use rithmic_host::{Session, SessionEvent, SessionHost, Timing};

    let server = venue().await;
    let host = SessionHost::start(
        RithmicVenue::new(server.session_config(), PlantSet::EXECUTION).with_gates(true, false),
        ParentGates {
            trading_enabled: true,
            cancel_all_enabled: false,
        },
        Timing::default(),
        HistoryPolicy::Allow,
    );
    let session = host.attach();
    let mut events = session.session_events();
    session.connect().await.unwrap();
    session.subscribe("ESZ6", "CME").await.unwrap();
    session.subscribe_order_updates().await.unwrap();
    session.subscribe_pnl().await.unwrap();

    server.push(
        Plant::Ticker,
        &frames::last_trade("ESZ6", "CME", 5001.25, 3, 1_790_000_000),
    );
    assert!(matches!(
        session.poll_event(WAIT).await.unwrap(),
        Some(PlantEvent::LastTrade(_))
    ));
    let before = server.received();
    let generation = host.generation();
    let venue = host.state.session.as_ref().unwrap().checkout().await;
    venue.fault_inject_drop_plant(Stream::Ticker).await.unwrap();

    let interrupted = tokio::time::timeout(WAIT, events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(interrupted, SessionEvent::Interrupted { generation: g, .. } if g == generation),
        "{interrupted:?}"
    );
    let resumed = tokio::time::timeout(WAIT, events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        resumed,
        SessionEvent::Resumed {
            generation: generation + 1
        }
    );
    let after = server.received();
    let recovery = &after[before.len()..];
    assert_eq!(
        recovery
            .iter()
            .filter(|r| r.template_id == template::LOGIN && r.plant == Some(Plant::Ticker))
            .count(),
        1
    );
    for plant in [Plant::History, Plant::Order, Plant::Pnl] {
        assert!(
            before
                .iter()
                .any(|r| r.template_id == template::LOGIN && r.plant == Some(plant)),
            "{plant:?} was connected before the fault"
        );
        assert!(
            !recovery.iter().any(|r| r.plant == Some(plant)
                && [template::LOGIN, template::LOGOUT].contains(&r.template_id)),
            "{plant:?} must stay connected: {recovery:?}"
        );
        assert_eq!(server.connections(plant), 1);
    }
    let subscriptions = server.received_of(template::MARKET_DATA_UPDATE);
    assert_eq!(
        subscriptions.len(),
        2,
        "initial subscribe plus exactly one restore"
    );
    for request in subscriptions {
        let request: rithmic_rs::rti::RequestMarketDataUpdate = request.decode();
        assert_eq!(request.symbol.as_deref(), Some("ESZ6"));
        assert_eq!(request.exchange.as_deref(), Some("CME"));
        assert_eq!(request.request, Some(1)); // SUBSCRIBE, never unsubscribe
    }
    server.push(
        Plant::Ticker,
        &frames::last_trade("ESZ6", "CME", 5002.0, 1, 1_790_000_001),
    );
    match session.poll_event(WAIT).await.unwrap() {
        Some(PlantEvent::LastTrade(tick)) => {
            assert_eq!(tick.symbol.as_deref(), Some("ESZ6"));
            assert_eq!(tick.trade_price, Some(5002.0));
            assert_eq!(tick.ts_event_ns, Some(1_790_000_001_000_000_000));
        }
        other => panic!("expected a fresh post-recovery trade, got {other:?}"),
    }
    host.shutdown().await;
}

#[tokio::test]
async fn account_plant_abort_restores_updates_without_reconnecting_siblings() {
    use rithmic_host::{HistoryPolicy, ParentGates, Session, SessionEvent, SessionHost, Timing};

    for (stream, plant) in [(Stream::Pnl, Plant::Pnl), (Stream::Order, Plant::Order)] {
        let server = venue().await;
        let host = SessionHost::start(
            RithmicVenue::new(server.session_config(), PlantSet::EXECUTION).with_gates(true, false),
            ParentGates {
                trading_enabled: true,
                cancel_all_enabled: false,
            },
            Timing {
                error_backoff: Duration::from_millis(1),
                connect_retry: Duration::from_millis(1),
                restore_backoff: Duration::from_millis(1),
            },
            HistoryPolicy::Allow,
        );
        let session = host.attach();
        session.connect().await.unwrap();
        session.subscribe("ESZ6", "CME").await.unwrap();
        session.subscribe_pnl().await.unwrap();
        session.subscribe_bracket_updates().await.unwrap();
        let mut events = session.session_events();
        let before = server.received().len();
        host.state
            .session
            .as_ref()
            .unwrap()
            .checkout()
            .await
            .fault_inject_drop_plant(stream)
            .await
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(WAIT, events.recv())
                .await
                .unwrap()
                .unwrap(),
            SessionEvent::Interrupted { .. }
        ));
        assert_eq!(
            tokio::time::timeout(WAIT, events.recv())
                .await
                .unwrap()
                .unwrap(),
            SessionEvent::Resumed { generation: 2 }
        );
        let requests = server.received();
        let logins: Vec<_> = requests[before..]
            .iter()
            .filter(|r| r.template_id == template::LOGIN)
            .map(|r| r.plant)
            .collect();
        assert_eq!(logins, [Some(plant)], "only the failed plant is recreated");
        for sibling in [Plant::Ticker, Plant::History, Plant::Pnl, Plant::Order] {
            assert_eq!(server.connections(sibling), 1);
        }
        let expected = if stream == Stream::Pnl {
            server.push(Plant::Pnl, &frames::account_pnl("ACCOUNT", "42.5", false));
            "pnl"
        } else {
            server.push(
                Plant::Order,
                &frames::rithmic_order("B2", "post-recovery", 13, "open"),
            );
            "order"
        };
        match session
            .next_events(stream, 8, WAIT)
            .await
            .unwrap()
            .as_slice()
        {
            [PlantEvent::AccountPnl(p)] if stream == Stream::Pnl => {
                assert_eq!(p.day_pnl.as_deref(), Some("42.5"))
            }
            [PlantEvent::OrderNotification(n)] if stream == Stream::Order => {
                assert_eq!(n.user_tag.as_deref(), Some("post-recovery"))
            }
            other => panic!("expected fresh {expected} update, got {other:?}"),
        }
        host.shutdown().await;
    }
}
