//! Rust gateway client: outcome classes, retry rules, single-flight recovery,
//! bounded event queues, and chunked history. Uses a scriptable mock parent so
//! failures happen at exactly the point under test.
//! (Ports of the Python transport-recovery / mux / range-load behaviors.)

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::*;
use rithmic_gateway::client::{GatewaySession, TransportEvent, EVENT_QUEUE_CAP};
use rithmic_gateway::pb::{self, frame::Body};
use rithmic_host::dto::PlantEvent;
use rithmic_host::{Error, PlaceOrder, Session};

fn place_order() -> PlaceOrder {
    PlaceOrder {
        symbol: NQ.into(),
        exchange: CME.into(),
        side: "BUY".into(),
        price_type: "LIMIT".into(),
        quantity: 1,
        user_tag: "tag-1".into(),
        price: Some(1.0),
        trigger_price: None,
        duration: "DAY".into(),
        trail_by_ticks: None,
        trail_by_price_id: None,
    }
}

fn front_month_reply() -> Action {
    Action::Reply(Body::FrontMonthResponse(pb::FrontMonthResponse {
        trading_symbol: Some(NQ.into()),
        ..Default::default()
    }))
}

fn count_of(mock: &MockParent, pred: impl Fn(&Body) -> bool) -> usize {
    mock.request_bodies().iter().filter(|b| pred(b)).count()
}

async fn connected(
    path: &std::path::Path,
    handler: impl Fn(&pb::Frame, usize) -> Action + Send + Sync + 'static,
    tune: impl FnOnce(&mut rithmic_gateway::client::GatewayConfig),
) -> (MockParent, GatewaySession) {
    let mock = MockParent::spawn(path, mock_ready(true, true), 0, handler).await;
    let mut cfg = mock_config(path);
    tune(&mut cfg);
    let session = GatewaySession::new(cfg);
    session.connect().await.expect("connect");
    (mock, session)
}

// --- outcome classes -----------------------------------------------------------

#[tokio::test]
async fn a_state_changing_rpc_lost_after_send_is_unknown_and_never_resent() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(
        &path,
        |frame, _| match frame.body {
            Some(Body::PlaceOrder(_)) => Action::Close,
            _ => Action::Ack,
        },
        |_| {},
    )
    .await;

    let err = session.place_order(&place_order()).await.expect_err("lost");
    assert!(matches!(err, Error::OutcomeUnknown(_)), "{err:?}");
    // Not a venue rejection and not a plain transport error.
    assert!(!matches!(err, Error::Gateway { .. } | Error::Transport(_)));

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        count_of(&mock, |b| matches!(b, Body::PlaceOrder(_))),
        1,
        "a place is never re-sent"
    );
}

#[tokio::test]
async fn every_state_changing_rpc_is_unknown_not_retried_when_the_reply_times_out() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(
        &path,
        |_, _| Action::Hang,
        |cfg| {
            cfg.rpc_timeout = Duration::from_millis(300);
        },
    )
    .await;

    let err = session.cancel_order("basket-1").await.expect_err("timeout");
    assert!(matches!(err, Error::OutcomeUnknown(_)), "{err:?}");
    let err = session.cancel_all_orders().await.expect_err("timeout");
    assert!(matches!(err, Error::OutcomeUnknown(_)), "{err:?}");
    assert_eq!(count_of(&mock, |b| matches!(b, Body::CancelOrder(_))), 1);
    assert_eq!(
        count_of(&mock, |b| matches!(b, Body::CancelAllOrders(_))),
        1
    );
}

#[tokio::test]
async fn a_read_lost_after_send_is_retried_once_on_a_fresh_connection() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(
        &path,
        |frame, connection| match frame.body {
            Some(Body::GetFrontMonth(_)) if connection == 0 => Action::Close,
            Some(Body::GetFrontMonth(_)) => front_month_reply(),
            _ => Action::Ack,
        },
        |_| {},
    )
    .await;

    let dto = session.get_front_month("NQ", CME).await.expect("retried");
    assert_eq!(dto.trading_symbol.as_deref(), Some(NQ));
    assert_eq!(count_of(&mock, |b| matches!(b, Body::GetFrontMonth(_))), 2);
    assert_eq!(mock.connections.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_read_is_retried_at_most_once() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(
        &path,
        |frame, _| match frame.body {
            Some(Body::GetFrontMonth(_)) => Action::Close,
            _ => Action::Ack,
        },
        |_| {},
    )
    .await;

    let err = session.get_front_month("NQ", CME).await.expect_err("fails");
    assert!(matches!(err, Error::Transport(_)), "{err:?}");
    assert_eq!(count_of(&mock, |b| matches!(b, Body::GetFrontMonth(_))), 2);
}

#[tokio::test]
async fn a_silent_parent_times_out_instead_of_hanging() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (_mock, session) = connected(
        &path,
        |_, _| Action::Hang,
        |cfg| {
            cfg.rpc_timeout = Duration::from_millis(250);
        },
    )
    .await;
    let started = std::time::Instant::now();
    let err = session
        .get_front_month("NQ", CME)
        .await
        .expect_err("timeout");
    assert!(matches!(err, Error::Transport(_)), "{err:?}");
    assert!(err.to_string().contains("timed out"), "{err}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn typed_gateway_errors_map_to_typed_errors() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (_mock, session) = connected(
        &path,
        |frame, _| match frame.body {
            Some(Body::PlaceOrder(_)) => Action::Reply(Body::Error(pb::ErrorResponse {
                code: "trading_disabled".into(),
                message: "place_order denied: parent trading disabled".into(),
            })),
            Some(Body::LoadOrders(_)) => Action::Reply(Body::Error(pb::ErrorResponse {
                code: "reconciliation_unavailable".into(),
                message: "drain is not authoritative".into(),
            })),
            _ => Action::Ack,
        },
        |_| {},
    )
    .await;

    match session.place_order(&place_order()).await {
        Err(Error::Gateway { code, .. }) => assert_eq!(code, "trading_disabled"),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        session.load_orders(0, 0).await,
        Err(Error::ReconciliationUnavailable(_))
    ));
}

// --- recovery --------------------------------------------------------------------

#[tokio::test]
async fn concurrent_callers_during_a_drop_share_one_redial() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(
        &path,
        |frame, connection| match frame.body {
            Some(Body::EnsurePnl(_)) if connection == 0 => Action::Close,
            Some(Body::GetFrontMonth(_)) => front_month_reply(),
            _ => Action::Ack,
        },
        |_| {},
    )
    .await;
    let session = Arc::new(session);
    let mut events = session.subscribe_transport();

    // Drop the connection (an intent RPC is lost; intents are not retried).
    assert!(session.ensure_pnl_plant().await.is_err());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("down event")
            .expect("open"),
        TransportEvent::Down { generation: 1 }
    );

    let mut tasks = Vec::new();
    for _ in 0..10 {
        let s = Arc::clone(&session);
        tasks.push(tokio::spawn(
            async move { s.get_front_month("NQ", CME).await },
        ));
    }
    for task in tasks {
        task.await.expect("join").expect("recovered");
    }
    assert_eq!(
        mock.connections.load(Ordering::SeqCst),
        2,
        "exactly one re-dial for ten waiters"
    );
    assert_eq!(session.transport_generation(), 2);
    assert_eq!(
        events.recv().await.expect("up event"),
        TransportEvent::Up { generation: 2 }
    );
}

#[tokio::test]
async fn ensure_transport_live_is_a_no_op_while_the_socket_is_up() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(&path, |_, _| Action::Ack, |_| {}).await;
    assert_eq!(session.ensure_transport_live().await.unwrap(), 1);
    assert_eq!(session.ensure_transport_live().await.unwrap(), 1);
    assert_eq!(mock.connections.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_failed_redial_fails_the_rpc_instead_of_hanging() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let user_cfg = {
        let mut cfg = mock_config(&path);
        cfg.attach_timeout = Duration::from_millis(400);
        cfg
    };
    let mock = MockParent::spawn(&path, mock_ready(false, false), 0, |frame, _| {
        match frame.body {
            Some(Body::GetFrontMonth(_)) => Action::Close,
            _ => Action::Ack,
        }
    })
    .await;
    let session = GatewaySession::new(user_cfg);
    session.connect().await.expect("connect");

    // The parent goes away entirely: dropping the mock closes its listener.
    drop(mock);
    std::fs::remove_file(&path).ok();
    let started = std::time::Instant::now();
    let err = session.get_front_month("NQ", CME).await.expect_err("down");
    assert!(
        matches!(err, Error::Gateway { .. } | Error::Transport(_)),
        "{err:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(10));
}

#[tokio::test]
async fn one_socket_serves_every_facade_in_the_process() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let mock = MockParent::spawn(&path, mock_ready(true, true), 0, |_, _| Action::Ack).await;
    // One fingerprint: every facade built from it shares the process's socket.
    let cfg = mock_config(&path);
    let first = GatewaySession::new(cfg.clone());
    first.connect().await.expect("first facade");
    let second = GatewaySession::new(cfg.clone());
    second.connect().await.expect("second facade");
    let third = GatewaySession::new(cfg.clone());
    third.connect().await.expect("third facade");

    assert_eq!(
        mock.connections.load(Ordering::SeqCst),
        1,
        "one parent connection"
    );
    first.subscribe(NQ, CME).await.unwrap();
    second.subscribe(NQ, CME).await.unwrap();

    // Each facade has its own queues: all receive the pushed event. A queue is
    // filled once its facade has subscribed or polled the channel; `third` polls.
    assert!(third.poll_event(Duration::ZERO).await.unwrap().is_none());
    mock.push_last_trade(NQ, 10.0);
    for session in [&first, &second, &third] {
        let event = session
            .poll_event(Duration::from_secs(5))
            .await
            .expect("poll")
            .expect("event");
        assert!(matches!(event, PlantEvent::LastTrade(_)));
    }

    // The socket closes only when the last facade detaches; the parent stays up.
    first.disconnect().await.unwrap();
    second.disconnect().await.unwrap();
    assert!(third.transport_connected());
    third.disconnect().await.unwrap();
    let again = GatewaySession::new(cfg);
    again.connect().await.expect("a new client can attach");
    assert_eq!(mock.connections.load(Ordering::SeqCst), 2);
}

// --- bounded event queues ------------------------------------------------------

#[tokio::test]
async fn a_consumer_that_lags_past_capacity_gets_channel_lagged_not_silent_loss() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(&path, |_, _| Action::Ack, |_| {}).await;

    // Polling the channel arms its queue (a real consumer subscribes first).
    assert!(session.poll_event(Duration::ZERO).await.unwrap().is_none());
    let total = EVENT_QUEUE_CAP + 904;
    for i in 0..total {
        mock.push_last_trade(NQ, i as f64);
    }
    // Wait until the reader has delivered everything (the last price lands).
    tokio::time::sleep(Duration::from_millis(800)).await;

    let err = session
        .poll_event(Duration::from_secs(2))
        .await
        .expect_err("the lag is reported first");
    match err {
        Error::ChannelLagged { plant, skipped } => {
            assert_eq!(plant, "ticker");
            assert_eq!(skipped, 904, "exactly the dropped (oldest) events");
        }
        other => panic!("{other:?}"),
    }
    // The newest EVENT_QUEUE_CAP events remain, in order.
    let mut first_price = None;
    let mut n = 0;
    while let Some(PlantEvent::LastTrade(t)) = session.poll_event(Duration::ZERO).await.unwrap() {
        first_price.get_or_insert(t.trade_price.unwrap());
        n += 1;
    }
    assert_eq!(n, EVENT_QUEUE_CAP);
    assert_eq!(first_price, Some(904.0));
}

#[tokio::test]
async fn a_busy_channel_does_not_block_another() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(&path, |_, _| Action::Ack, |_| {}).await;

    for i in 0..2_000 {
        mock.push_last_trade(NQ, i as f64);
    }
    let _ = mock.events.send(Body::Event(pb::Event {
        body: Some(pb::event::Body::OrderNotification(pb::OrderNotification {
            source: "rithmic".into(),
            basket_id: Some("basket-1".into()),
            ..Default::default()
        })),
    }));
    // The order event is delivered even though the market queue is full of ticks.
    let order = session
        .poll_order_event(Duration::from_secs(5))
        .await
        .expect("poll")
        .expect("order event");
    assert!(matches!(order, PlantEvent::OrderNotification(_)));
}

#[tokio::test]
async fn polling_a_dropped_transport_drains_queued_events_then_reports_the_close() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(
        &path,
        |frame, _| match frame.body {
            Some(Body::EnsurePnl(_)) => Action::Close,
            _ => Action::Ack,
        },
        // No automatic recovery: the closed state stays observable.
        |cfg| cfg.recovery_attempts = 0,
    )
    .await;
    assert!(session.poll_event(Duration::ZERO).await.unwrap().is_none());
    mock.push_last_trade(NQ, 1.0);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(session.ensure_pnl_plant().await.is_err());
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(matches!(
        session.poll_event(Duration::ZERO).await.unwrap(),
        Some(PlantEvent::LastTrade(_))
    ));
    assert!(matches!(
        session.poll_event(Duration::from_millis(100)).await,
        Err(Error::ChannelClosed { plant: "ticker" })
    ));
}

#[tokio::test]
async fn a_blocked_poll_wakes_when_an_event_arrives() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(&path, |_, _| Action::Ack, |_| {}).await;
    let session = Arc::new(session);
    let poller = {
        let s = Arc::clone(&session);
        tokio::spawn(async move { s.poll_event(Duration::from_secs(10)).await })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;
    let started = std::time::Instant::now();
    mock.push_last_trade(NQ, 5.0);
    let event = poller.await.expect("join").expect("poll").expect("event");
    assert!(matches!(event, PlantEvent::LastTrade(_)));
    assert!(started.elapsed() < Duration::from_secs(2), "woken promptly");
}

// --- chunked history -----------------------------------------------------------

const DAY: i32 = 86_400;

fn bar(marker: i32) -> pb::HistoryBar {
    pb::HistoryBar {
        symbol: Some(NQ.into()),
        exchange: Some(CME.into()),
        bar_type: Some(2),
        period: Some("1".into()),
        marker: Some(marker),
        close_price: Some(f64::from(marker)),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_wide_window_is_chunked_merged_sorted_and_deduplicated() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(
        &path,
        |frame, _| match &frame.body {
            Some(Body::LoadTimeBars(req)) => {
                // Adjacent slices share their boundary second; return unsorted.
                Action::Reply(Body::LoadTimeBarsResponse(pb::LoadTimeBarsResponse {
                    bars: vec![bar(req.end_time_sec), bar(req.start_time_sec)],
                }))
            }
            _ => Action::Ack,
        },
        |_| {},
    )
    .await;

    let bars = session
        .load_time_bars(NQ, CME, 0, 100 * DAY, 2, 1)
        .await
        .expect("history");

    let slices: Vec<(i32, i32)> = mock
        .request_bodies()
        .iter()
        .filter_map(|b| match b {
            Body::LoadTimeBars(r) => Some((r.start_time_sec, r.end_time_sec)),
            _ => None,
        })
        .collect();
    // 1m bars slice in 40-day steps: [0,40d] [40d,80d] [80d,100d].
    assert_eq!(
        slices,
        [(0, 40 * DAY), (40 * DAY, 80 * DAY), (80 * DAY, 100 * DAY)]
    );

    let markers: Vec<i32> = bars.iter().map(|b| b.marker.unwrap()).collect();
    assert_eq!(
        markers,
        [0, 40 * DAY, 80 * DAY, 100 * DAY],
        "sorted, no duplicates"
    );
}

#[tokio::test]
async fn a_transient_slice_timeout_is_retried_without_failing_the_request() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let hung = Arc::new(AtomicUsize::new(0));
    let hung_in_handler = Arc::clone(&hung);
    let (mock, session) = connected(
        &path,
        move |frame, _| match &frame.body {
            Some(Body::LoadTimeBars(req))
                if req.start_time_sec == 40 * DAY
                    && hung_in_handler.fetch_add(1, Ordering::SeqCst) == 0 =>
            {
                Action::Hang
            }
            Some(Body::LoadTimeBars(req)) => {
                Action::Reply(Body::LoadTimeBarsResponse(pb::LoadTimeBarsResponse {
                    bars: vec![bar(req.start_time_sec)],
                }))
            }
            _ => Action::Ack,
        },
        |cfg| cfg.history_rpc_timeout = Duration::from_millis(300),
    )
    .await;

    let bars = session
        .load_time_bars(NQ, CME, 0, 100 * DAY, 2, 1)
        .await
        .expect("the slice is retried");
    assert_eq!(bars.len(), 3);
    assert!(count_of(&mock, |b| matches!(b, Body::LoadTimeBars(_))) >= 4);
}

#[tokio::test]
async fn a_slice_that_keeps_timing_out_fails_the_whole_request_boundedly() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(
        &path,
        |frame, _| match &frame.body {
            Some(Body::LoadTimeBars(req)) if req.start_time_sec == 40 * DAY => Action::Hang,
            Some(Body::LoadTimeBars(req)) => {
                Action::Reply(Body::LoadTimeBarsResponse(pb::LoadTimeBarsResponse {
                    bars: vec![bar(req.start_time_sec)],
                }))
            }
            _ => Action::Ack,
        },
        |cfg| cfg.history_rpc_timeout = Duration::from_millis(200),
    )
    .await;

    let err = session
        .load_time_bars(NQ, CME, 0, 100 * DAY, 2, 1)
        .await
        .expect_err("fails");
    assert!(matches!(err, Error::Transport(_)), "{err:?}");
    let attempts = count_of(
        &mock,
        |b| matches!(b, Body::LoadTimeBars(r) if r.start_time_sec == 40 * DAY),
    );
    assert!(attempts <= 6, "bounded retries, got {attempts}");
}

#[tokio::test]
async fn an_invalid_bar_type_fails_like_the_direct_path_before_any_rpc() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(&path, |_, _| Action::Ack, |_| {}).await;
    let err = session
        .load_time_bars(NQ, CME, 0, 10, 99, 1)
        .await
        .expect_err("bad bar type");
    // The same error RithmicVenue raises (both go through parse_time_bar_type).
    assert!(matches!(err, Error::Protocol(_)), "{err:?}");
    assert_eq!(count_of(&mock, |b| matches!(b, Body::LoadTimeBars(_))), 0);
}

#[tokio::test]
async fn an_empty_or_inverted_window_makes_no_rpc() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(&path, |_, _| Action::Ack, |_| {}).await;
    assert!(session
        .load_time_bars(NQ, CME, 100, 10, 2, 1)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(count_of(&mock, |b| matches!(b, Body::LoadTimeBars(_))), 0);
}

// --- automatic recovery ----------------------------------------------------------

fn fast_recovery(cfg: &mut rithmic_gateway::client::GatewayConfig) {
    cfg.recovery_initial_backoff = Duration::from_millis(20);
    cfg.recovery_max_backoff = Duration::from_millis(80);
}

#[tokio::test]
async fn the_first_connect_emits_no_up_event() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let mock = MockParent::spawn(&path, mock_ready(false, false), 0, |_, _| Action::Ack).await;
    let session = GatewaySession::new(mock_config(&path));
    let mut events = session.subscribe_transport();
    session.connect().await.expect("connect");
    assert_eq!(session.transport_generation(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), events.recv())
            .await
            .is_err(),
        "a first connect is not a recovery"
    );
    drop(mock);
}

#[tokio::test]
async fn a_lost_transport_recovers_automatically_without_any_caller() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(
        &path,
        |frame, connection| match frame.body {
            Some(Body::EnsurePnl(_)) if connection == 0 => Action::Close,
            _ => Action::Ack,
        },
        fast_recovery,
    )
    .await;
    let mut events = session.subscribe_transport();

    assert!(session.ensure_pnl_plant().await.is_err());
    // Nobody calls ensure_transport_live or issues another RPC.
    let down = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("down")
        .expect("open");
    assert_eq!(down, TransportEvent::Down { generation: 1 });
    let up = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("up")
        .expect("open");
    assert_eq!(up, TransportEvent::Up { generation: 2 });
    assert_eq!(mock.connections.load(Ordering::SeqCst), 2);
    assert!(session.transport_connected());
}

#[tokio::test]
async fn recovery_gives_up_with_recovery_failed_after_its_attempt_budget() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(
        &path,
        |frame, _| match frame.body {
            Some(Body::EnsurePnl(_)) => Action::Close,
            _ => Action::Ack,
        },
        |cfg| {
            fast_recovery(cfg);
            cfg.recovery_attempts = 3;
            cfg.attach_timeout = Duration::from_millis(100);
        },
    )
    .await;
    let mut failed = session.subscribe_recovery_failed();

    // The parent goes away for good, then the transport drops.
    drop(mock);
    std::fs::remove_file(&path).ok();
    let _ = session.ensure_pnl_plant().await;

    let report = tokio::time::timeout(Duration::from_secs(10), failed.recv())
        .await
        .expect("recovery_failed")
        .expect("open");
    assert_eq!(report.attempts, 3);
    assert_eq!(report.generation, 1);
    assert!(!report.cause.is_empty());
    assert!(!session.transport_connected());
}

#[tokio::test]
async fn a_deliberate_disconnect_is_never_recovered() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(&path, |_, _| Action::Ack, fast_recovery).await;
    let mut events = session.subscribe_transport();
    session.disconnect().await.unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(400), events.recv())
            .await
            .is_err(),
        "no Down / Up after a deliberate close"
    );
    assert_eq!(mock.connections.load(Ordering::SeqCst), 1, "no re-dial");
}

#[tokio::test]
async fn automatic_recovery_can_be_disabled() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(
        &path,
        |frame, _| match frame.body {
            Some(Body::EnsurePnl(_)) => Action::Close,
            _ => Action::Ack,
        },
        |cfg| cfg.recovery_attempts = 0,
    )
    .await;
    assert!(session.ensure_pnl_plant().await.is_err());
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !session.transport_connected(),
        "stays down until a caller recovers"
    );
    assert_eq!(mock.connections.load(Ordering::SeqCst), 1);
    assert_eq!(session.ensure_transport_live().await.unwrap(), 2);
}

#[tokio::test]
async fn a_channel_nobody_subscribed_or_polled_queues_nothing() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let mock = MockParent::spawn(&path, mock_ready(true, true), 0, |_, _| Action::Ack).await;
    let cfg = mock_config(&path);
    let reader = GatewaySession::new(cfg.clone());
    reader.connect().await.unwrap();
    reader.subscribe(NQ, CME).await.unwrap();
    let idle = GatewaySession::new(cfg);
    idle.connect().await.unwrap();

    mock.push_last_trade(NQ, 1.0);
    // The subscribing facade has it; the idle facade (never subscribed, never
    // polled) was not handed a copy, and its first poll finds nothing from before.
    assert!(matches!(
        reader.poll_event(Duration::from_secs(5)).await.unwrap(),
        Some(PlantEvent::LastTrade(_))
    ));
    assert!(idle.poll_event(Duration::ZERO).await.unwrap().is_none());
}

#[tokio::test]
async fn subscribing_arms_the_channel_before_the_first_event() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(&path, |_, _| Action::Ack, |_| {}).await;
    // No poll yet: the subscribe itself arms the queue, so an event that arrives
    // before the first poll is kept.
    session.subscribe(NQ, CME).await.unwrap();
    mock.push_last_trade(NQ, 1.0);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(matches!(
        session.poll_event(Duration::from_secs(2)).await.unwrap(),
        Some(PlantEvent::LastTrade(_))
    ));
}

#[tokio::test]
async fn a_timed_out_history_slice_costs_at_most_two_attempts() {
    let dir = short_dir();
    let path = dir.path().join("gw.sock");
    let (mock, session) = connected(
        &path,
        |frame, _| match frame.body {
            Some(Body::LoadTimeBars(_)) => Action::Hang,
            _ => Action::Ack,
        },
        |cfg| cfg.history_rpc_timeout = Duration::from_millis(150),
    )
    .await;
    let err = session
        .load_time_bars("NQH6", "CME", 0, 3_600, 2, 1)
        .await
        .expect_err("every attempt times out");
    assert!(matches!(err, Error::Transport(_)), "{err:?}");
    let attempts = mock
        .request_bodies()
        .iter()
        .filter(|b| matches!(b, Body::LoadTimeBars(_)))
        .count();
    assert!(
        attempts <= 2,
        "one read retry at most, not a retry loop on top: {attempts}"
    );
}
