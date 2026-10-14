//! The Rust gateway client against the real server over a fake plant.
//!
//! This is the Rust twin of `tests/test_gateway_scenarios.py`: the same scenario
//! list drives the production server path (handshake, gates, fan-out, event
//! pump, reconnect + intent restore, credential flock), so Python and Rust
//! clients are held to identical behavior.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use rithmic_gateway::client::{GatewayConfig, GatewaySession};
use rithmic_gateway::pb::{self, frame::Body};
use rithmic_gateway::testing as scripted;
use rithmic_plants::dto::PlantEvent;
use rithmic_plants::{Error, PlaceOrder, Session};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

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

async fn session_for(parent: &FakeParent) -> GatewaySession {
    let session = GatewaySession::new(parent.config());
    session.connect().await.expect("connect");
    session
}

/// A second, independent parent connection: a symlink gives the same socket a
/// different path, hence a separate client runtime.
async fn second_connection(parent: &FakeParent) -> GatewaySession {
    let alias = parent.dir.path().join("alias.sock");
    if !alias.exists() {
        std::os::unix::fs::symlink(&parent.listen, &alias).expect("symlink");
    }
    let cfg = GatewayConfig::new(&parent.user, SYSTEM, URL, "Live")
        .with_listen(&format!("unix://{}", alias.display()))
        .expect("listen");
    let session = GatewaySession::new(cfg);
    session.connect().await.expect("connect");
    session
}

async fn drain(
    session: &GatewaySession,
    want: usize,
    channel: fn(&GatewaySession, Duration) -> Fut,
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
    Box<dyn std::future::Future<Output = rithmic_plants::Result<Option<PlantEvent>>> + Send + 'a>,
>;

fn market(s: &GatewaySession, t: Duration) -> Fut<'_> {
    s.poll_event(t)
}
fn history(s: &GatewaySession, t: Duration) -> Fut<'_> {
    s.poll_history_event(t)
}
fn pnl(s: &GatewaySession, t: Duration) -> Fut<'_> {
    s.poll_pnl_event(t)
}
fn order(s: &GatewaySession, t: Duration) -> Fut<'_> {
    s.poll_order_event(t)
}

fn code(err: &Error) -> &str {
    match err {
        Error::Gateway { code, .. } => code,
        other => panic!("expected a gateway error, got {other:?}"),
    }
}

// --- subscribe + fan-out + refcount ---------------------------------------------

// --- facades sharing one socket ------------------------------------------------

#[tokio::test]
async fn one_facades_unsubscribe_does_not_end_another_facades_stream() {
    let parent = FakeParent::start(Opts::default()).await;
    // Same config: two facades on one process socket (data clients of two nodes
    // in one process, say).
    let a = session_for(&parent).await;
    let b = session_for(&parent).await;
    a.subscribe(NQ, CME).await.unwrap();
    b.subscribe(NQ, CME).await.unwrap();

    a.unsubscribe(NQ, CME).await.unwrap();
    assert_eq!(
        parent.count("unsubscribe").await,
        0,
        "b still holds the intent: {:?}",
        parent.calls().await
    );
    parent.push_last_trades(NQ, 1).await;
    assert_eq!(drain(&b, 1, market).await.len(), 1, "b keeps its stream");

    b.unsubscribe(NQ, CME).await.unwrap();
    assert!(
        wait_until_async(Duration::from_secs(5), || async {
            parent.count(&format!("unsubscribe {NQ} {CME}")).await == 1
        })
        .await,
        "the last holder leaves the venue"
    );
}

#[tokio::test]
async fn one_exec_facade_stopping_keeps_the_others_order_stream() {
    let parent = FakeParent::start(Opts {
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
        drain(&b, 1, order).await.len(),
        1,
        "b's orders still arrive"
    );
}

#[tokio::test]
async fn a_facade_that_disconnects_releases_only_what_it_held() {
    let parent = FakeParent::start(Opts::default()).await;
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
    assert_eq!(drain(&b, 1, market).await.len(), 1);
}

#[tokio::test]
async fn two_connections_share_one_venue_subscription_by_refcount() {
    let parent = FakeParent::start(Opts::default()).await;
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
        let events = drain(session, 3, market).await;
        assert_eq!(events.len(), 3);
        assert!(events
            .iter()
            .all(|e| matches!(e, PlantEvent::LastTrade(t) if t.symbol.as_deref() == Some(NQ))));
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

#[tokio::test]
async fn disconnect_leaves_the_venue_when_the_last_peer_goes_and_the_parent_stays() {
    let mut parent = FakeParent::start(Opts::default()).await;
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
    // The parent keeps running and accepts a new client.
    let again = session_for(&parent).await;
    assert!(again.transport_connected());
    parent.stop();
}

// --- trading gate parity --------------------------------------------------------

#[tokio::test]
async fn history_reads_reach_the_plant_when_trading_is_on() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    let s = session_for(&parent).await;
    assert!(s
        .load_fill_history(20260901, 20260930)
        .await
        .unwrap()
        .is_empty());
    assert!(s.load_order_history_dates().await.unwrap().is_empty());
    assert!(s.load_order_history("20260930").await.unwrap().is_empty());
    let calls = parent.calls().await;
    for want in [
        "load_fill_history 20260901 20260930",
        "load_order_history_dates",
        "load_order_history 20260930",
    ] {
        assert!(calls.contains(&want.to_string()), "{want}: {calls:?}");
    }
}

#[tokio::test]
async fn trading_off_denies_every_gated_rpc_without_reaching_the_plant() {
    let parent = FakeParent::start(Opts::default()).await;
    let s = session_for(&parent).await;
    assert!(!s.trading_enabled());

    let denied = [
        s.place_order(&place("t")).await.unwrap_err(),
        s.cancel_order("basket-1").await.unwrap_err(),
        s.subscribe_order_updates().await.unwrap_err(),
        s.subscribe_bracket_updates().await.unwrap_err(),
        s.load_orders(0, 1).await.unwrap_err(),
        s.load_product_rms_info().await.unwrap_err(),
        s.load_account_rms_info().await.unwrap_err(),
        s.load_fill_history(20260901, 20260930).await.unwrap_err(),
        s.load_order_history_dates().await.unwrap_err(),
        s.load_order_history("20260930").await.unwrap_err(),
    ];
    for err in &denied {
        assert_eq!(code(err), "trading_disabled");
    }
    assert_eq!(parent.count("place_order").await, 0);
    assert_eq!(parent.count("cancel_order").await, 0);
    assert_eq!(parent.count("subscribe_order_updates").await, 0);
}

#[tokio::test]
async fn trading_on_reaches_the_plant() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    let s = session_for(&parent).await;
    assert!(s.trading_enabled());
    s.place_order(&place("tag-1")).await.unwrap();
    s.cancel_order("basket-1").await.unwrap();
    let calls = parent.calls().await;
    assert!(
        calls.contains(&"place_order tag-1".to_string()),
        "{calls:?}"
    );
    assert!(
        calls.contains(&"cancel_order basket-1".to_string()),
        "{calls:?}"
    );
}

#[tokio::test]
async fn cancel_all_has_its_own_gate_independent_of_trading() {
    let parent = FakeParent::start(Opts {
        trading: false,
        cancel_all: true,
    })
    .await;
    let s = session_for(&parent).await;
    assert!(s.cancel_all_enabled());
    s.cancel_all_orders().await.unwrap();
    assert_eq!(parent.count("cancel_all_orders").await, 1);
    assert_eq!(
        code(&s.place_order(&place("t")).await.unwrap_err()),
        "trading_disabled"
    );
    drop(parent);

    let parent = FakeParent::start(Opts {
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

// --- restore after plant drop -----------------------------------------------------

#[tokio::test]
async fn every_intent_is_restored_by_the_parent_and_clients_stay_attached() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    let s = session_for(&parent).await;
    s.subscribe(NQ, CME).await.unwrap();
    s.subscribe_order_book_summary(NQ, CME).await.unwrap();
    s.subscribe_time_bars(NQ, CME, 2, 1).await.unwrap();
    s.subscribe_pnl().await.unwrap();
    s.subscribe_order_updates().await.unwrap();
    s.subscribe_bracket_updates().await.unwrap();
    parent.clear_calls().await;

    parent.command("drop_plants").await;
    assert!(
        parent
            .wait_for_call("subscribe_bracket_updates", Duration::from_secs(15))
            .await
    );
    let calls = parent.calls().await;
    for expected in [
        "connect".to_string(),
        format!("subscribe {NQ} {CME}"),
        format!("subscribe_book {NQ} {CME}"),
        format!("subscribe_time_bars {NQ} {CME} 2 1"),
        "subscribe_pnl".to_string(),
        "subscribe_order_updates".to_string(),
        "subscribe_bracket_updates".to_string(),
    ] {
        assert!(calls.contains(&expected), "{expected}: {calls:?}");
    }

    // The parent restored the subscriptions itself and told this client; the
    // client stayed attached and keeps its intents, so there is nothing to replay.
    assert!(wait_until(Duration::from_secs(10), || s.transport_generation() == 2).await);
    assert!(s.transport_connected());
    assert_eq!(s.ensure_transport_live().await.expect("live"), 2);
    parent.push_last_trades(NQ, 1).await;
    parent
        .command("push order accepted basket-9 tag-9 NQH6")
        .await;
    assert_eq!(drain(&s, 1, market).await.len(), 1);
    let orders = drain(&s, 1, order).await;
    assert!(matches!(
        &orders[0],
        PlantEvent::OrderNotification(n) if n.basket_id.as_deref() == Some("basket-9")
    ));
    // The rebuild cost the venue exactly one join per intent: nothing was torn
    // down and re-issued by a client replay.
    let after = parent.calls().await;
    for join in [
        format!("subscribe {NQ} {CME}"),
        format!("subscribe_book {NQ} {CME}"),
        format!("subscribe_time_bars {NQ} {CME} 2 1"),
        "subscribe_pnl".to_string(),
        "subscribe_order_updates".to_string(),
    ] {
        assert_eq!(
            after.iter().filter(|c| **c == join).count(),
            1,
            "{join} issued once: {after:?}"
        );
    }
    assert!(
        !after.iter().any(|c| c.starts_with("unsubscribe")),
        "no venue unsubscribe from client teardown: {after:?}"
    );
}

#[tokio::test]
async fn one_shot_commands_register_no_intent() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    let s = session_for(&parent).await;
    s.place_order(&place("tag-1")).await.unwrap();
    s.cancel_order("basket-1").await.unwrap();
    parent.clear_calls().await;
    parent.command("drop_plants").await;
    assert!(
        parent
            .wait_for_call("connect", Duration::from_secs(15))
            .await
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    let calls = parent.calls().await;
    assert!(
        !calls
            .iter()
            .any(|c| c.starts_with("place_order") || c.starts_with("cancel_order")),
        "{calls:?}"
    );
    assert!(
        !calls.iter().any(|c| c.starts_with("subscribe")),
        "{calls:?}"
    );
}

// --- ticker reset, history refusal ------------------------------------------------

#[tokio::test]
async fn ticker_reset_reissues_ticker_intents() {
    let parent = FakeParent::start(Opts::default()).await;
    let s = session_for(&parent).await;
    s.subscribe(NQ, CME).await.unwrap();
    parent.clear_calls().await;
    s.reset_ticker().await.unwrap();
    let calls = parent.calls().await;
    assert_eq!(calls[0], "reset_ticker", "{calls:?}");
    assert!(
        calls.contains(&format!("subscribe {NQ} {CME}")),
        "{calls:?}"
    );
}

#[tokio::test]
async fn history_is_refused_under_a_live_ticker_intent() {
    let parent = FakeParent::start(Opts::default()).await;
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

// --- a stuck client never hurts its peer ------------------------------------------

#[tokio::test]
async fn a_client_that_never_reads_is_dropped_without_hurting_its_peer() {
    let parent = FakeParent::start(Opts::default()).await;
    let fast = session_for(&parent).await;
    fast.subscribe(NQ, CME).await.unwrap();

    // A raw client that handshakes, subscribes, and then never reads again.
    let mut stuck = UnixStream::connect(&parent.listen).await.unwrap();
    let hello = pb::Frame {
        request_id: 0,
        body: Some(Body::Handshake(pb::Handshake {
            user: parent.user.clone(),
            system_name: SYSTEM.into(),
            url: URL.into(),
            env: "Live".into(),
            ..Default::default()
        })),
    };
    stuck
        .write_all(&rithmic_gateway::codec::encode(&hello).unwrap())
        .await
        .unwrap();
    let (ready, _) = read_frame(&mut stuck).await.unwrap();
    assert!(matches!(ready.body, Some(Body::Ready(_))));
    let sub = pb::Frame {
        request_id: 1,
        body: Some(Body::Subscribe(pb::SubscribeRequest {
            symbol: NQ.into(),
            exchange: CME.into(),
        })),
    };
    stuck
        .write_all(&rithmic_gateway::codec::encode(&sub).unwrap())
        .await
        .unwrap();

    parent.push_last_trades(NQ, 20_000).await;
    let mut received = 0usize;
    let deadline = Instant::now() + Duration::from_secs(30);
    while received < 1_000 && Instant::now() < deadline {
        match fast.poll_event(Duration::from_millis(100)).await {
            Ok(Some(_)) => received += 1,
            Ok(None) => {}
            // A 20k burst can outrun this one-event-at-a-time consumer's own
            // bounded queue; that is reported (never silent) and it keeps going.
            Err(rithmic_plants::Error::ChannelLagged { .. }) => {}
            Err(e) => panic!("healthy peer poll failed: {e}"),
        }
    }
    assert!(
        received >= 1_000,
        "the healthy peer keeps receiving ({received})"
    );
    // The healthy peer is still fully usable.
    fast.get_front_month("NQ", CME)
        .await
        .expect("rpc still works");
}

// --- parent restart / transport recovery -------------------------------------------

#[tokio::test]
async fn the_client_recovers_after_the_parent_restarts() {
    let mut parent = FakeParent::start(Opts::default()).await;
    let s = session_for(&parent).await;
    s.subscribe(NQ, CME).await.unwrap();
    let before = s.transport_generation();
    let mut events = s.subscribe_transport();

    parent.restart().await;
    let after = s.ensure_transport_live().await.expect("re-dial");
    assert!(after > before, "generation increases: {before} → {after}");
    // A restarted parent has no memory of this client: the adapter replays intent.
    s.subscribe(NQ, CME).await.unwrap();
    parent.push_last_trades(NQ, 1).await;
    assert_eq!(drain(&s, 1, market).await.len(), 1);

    let mut saw_down = false;
    while let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(200), events.recv()).await
    {
        saw_down |= matches!(event, rithmic_gateway::client::TransportEvent::Down { .. });
    }
    assert!(saw_down, "a Down event preceded the recovery");
}

// --- identical records ---------------------------------------------------------------

#[tokio::test]
async fn events_arrive_equal_to_what_the_plant_emitted() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    let s = session_for(&parent).await;
    s.subscribe(NQ, CME).await.unwrap();
    s.subscribe_time_bars(NQ, CME, 2, 1).await.unwrap();
    s.subscribe_pnl().await.unwrap();
    s.subscribe_order_updates().await.unwrap();
    s.subscribe_bracket_updates().await.unwrap();

    parent.command("push last_trade NQH6 CME 100.25 1").await;
    parent.command("push bbo NQH6 CME 100.0 5 100.25 7").await;
    parent.command("push time_bar NQH6 CME 101.5").await;
    parent.command("push account_pnl ACCT -12.5").await;
    parent
        .command("push order accepted basket-9 tag-9 NQH6")
        .await;

    let market_events = drain(&s, 2, market).await;
    assert_eq!(market_events[0], scripted::last_trade(NQ, CME, 100.25, 1));
    assert_eq!(
        market_events[1],
        scripted::bbo(NQ, CME, 100.0, 5, 100.25, 7)
    );
    assert_eq!(
        drain(&s, 1, history).await[0],
        scripted::time_bar(NQ, CME, 101.5)
    );
    assert_eq!(
        drain(&s, 1, pnl).await[0],
        scripted::account_pnl("ACCT", "-12.5")
    );
    assert_eq!(
        drain(&s, 1, order).await[0],
        scripted::order_notification("accepted", "basket-9", "tag-9", NQ)
    );
    // A bracket update rides the order stream with the same record shape.
    parent.command("push bracket basket-9 8 16").await;
    assert_eq!(
        drain(&s, 1, order).await[0],
        scripted::bracket_update("basket-9", 8, 16)
    );
}

// --- handshake gates -------------------------------------------------------------------

#[tokio::test]
async fn a_fingerprint_for_a_different_credential_set_is_refused_quickly() {
    let parent = FakeParent::start(Opts::default()).await;
    let mut cfg = GatewayConfig::new("someone-else", SYSTEM, URL, "Live")
        .with_listen(&format!("unix://{}", parent.listen.display()))
        .expect("listen");
    cfg.attest_flock = false;
    let started = Instant::now();
    let err = GatewaySession::new(cfg).connect().await.unwrap_err();
    assert_eq!(code(&err), "fingerprint_mismatch");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "refused, not retried"
    );
}

// --- concurrency ------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_polls_and_rpcs_make_progress_without_deadlock() {
    let parent = FakeParent::start(Opts::default()).await;
    let s = Arc::new(session_for(&parent).await);

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

// --- a plant lag is a typed gap, never silence ---------------------------------------

/// The outcome of each poll, described so two backends can be compared.
async fn describe_polls(
    s: &dyn Session,
    stream: rithmic_plants::Stream,
    polls: usize,
) -> Vec<String> {
    let mut out = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while out.len() < polls && Instant::now() < deadline {
        let polled = match stream {
            rithmic_plants::Stream::Ticker => s.poll_event(Duration::from_millis(100)).await,
            rithmic_plants::Stream::Order => s.poll_order_event(Duration::from_millis(100)).await,
            other => panic!("not exercised: {other:?}"),
        };
        match polled {
            Ok(Some(PlantEvent::LastTrade(_))) => out.push("trade".into()),
            Ok(Some(PlantEvent::OrderNotification(_))) => out.push("order".into()),
            Ok(Some(other)) => out.push(format!("other {other:?}")),
            Ok(None) => {}
            Err(Error::ChannelLagged { plant, skipped }) => {
                out.push(format!("lagged {plant} {skipped}"))
            }
            Err(e) => out.push(format!("error {e}")),
        }
    }
    out
}

#[tokio::test]
async fn a_plant_lag_reaches_the_client_in_order_as_channel_lagged() {
    let parent = FakeParent::start(Opts::default()).await;
    let s = session_for(&parent).await;
    s.subscribe(NQ, CME).await.unwrap();
    parent.push_last_trades(NQ, 1).await;
    parent.command("push gap ticker 41").await;
    parent.push_last_trades(NQ, 1).await;
    assert_eq!(
        describe_polls(&s, rithmic_plants::Stream::Ticker, 3).await,
        ["trade", "lagged ticker 41", "trade"]
    );
}

#[tokio::test]
async fn the_next_events_batch_carries_the_gap_in_order() {
    let parent = FakeParent::start(Opts::default()).await;
    let s = session_for(&parent).await;
    s.subscribe(NQ, CME).await.unwrap();
    parent.push_last_trades(NQ, 1).await;
    parent.command("push gap ticker 9").await;
    parent.push_last_trades(NQ, 1).await;
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while got.len() < 3 && Instant::now() < deadline {
        got.extend(
            s.next_events(
                rithmic_plants::Stream::Ticker,
                16,
                Duration::from_millis(200),
            )
            .await
            .unwrap(),
        );
    }
    assert_eq!(got.len(), 3, "{got:?}");
    assert!(matches!(got[0], PlantEvent::LastTrade(_)));
    assert!(matches!(
        got[1],
        PlantEvent::Gap(rithmic_plants::GapDto {
            stream: rithmic_plants::Stream::Ticker,
            skipped: 9
        })
    ));
    assert!(matches!(got[2], PlantEvent::LastTrade(_)));
}

#[tokio::test]
async fn an_order_stream_gap_reaches_order_subscribers() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    let s = session_for(&parent).await;
    s.subscribe_order_updates().await.unwrap();
    parent.command("push gap order 7").await;
    assert_eq!(
        describe_polls(&s, rithmic_plants::Stream::Order, 1).await,
        ["lagged order 7"]
    );
}

/// Direct and gateway modes must read the same lag the same way: the fake
/// session uses the same stream lanes as `DirectSession`, so running one script
/// through it and through a gateway client proves the two agree.
#[tokio::test]
async fn the_same_lag_reads_identically_through_direct_and_gateway() {
    let direct = rithmic_plants::fake::FakeSession::new();
    direct.connect().await.unwrap();
    direct.push_event(
        rithmic_plants::Stream::Ticker,
        scripted::last_trade(NQ, CME, 1.0, 1),
    );
    direct.push_event(
        rithmic_plants::Stream::Ticker,
        PlantEvent::Gap(rithmic_plants::GapDto {
            stream: rithmic_plants::Stream::Ticker,
            skipped: 41,
        }),
    );
    direct.push_event(
        rithmic_plants::Stream::Ticker,
        scripted::last_trade(NQ, CME, 2.0, 1),
    );
    let via_direct = describe_polls(&direct, rithmic_plants::Stream::Ticker, 3).await;

    let parent = FakeParent::start(Opts::default()).await;
    let s = session_for(&parent).await;
    s.subscribe(NQ, CME).await.unwrap();
    parent.push_last_trades(NQ, 1).await;
    parent.command("push gap ticker 41").await;
    parent.push_last_trades(NQ, 1).await;
    let via_gateway = describe_polls(&s, rithmic_plants::Stream::Ticker, 3).await;

    assert_eq!(via_direct, ["trade", "lagged ticker 41", "trade"]);
    assert_eq!(via_direct, via_gateway);
}

// --- failure classes are assigned at the source and survive both modes -----------------

/// What a failed `place_order` reads as, class only.
fn class_of(err: &Error) -> &'static str {
    match err {
        Error::Rejected(_) => "rejected",
        Error::NotSent(_) => "not_sent",
        Error::OutcomeUnknown(_) => "unknown",
        Error::Gateway { code, .. } if code == "trading_disabled" => "denied",
        _ => "other",
    }
}

#[tokio::test]
async fn the_same_failure_class_reads_identically_through_direct_and_gateway() {
    for (kind, fail, want) in [
        (
            "rejected",
            rithmic_plants::fake::FakeFailure::Rejected("price".into()),
            "rejected",
        ),
        (
            "not_sent",
            rithmic_plants::fake::FakeFailure::NotSent("no route".into()),
            "not_sent",
        ),
        (
            "unknown",
            rithmic_plants::fake::FakeFailure::Unknown("write failed".into()),
            "unknown",
        ),
    ] {
        // Direct: the fake session stands in for DirectSession's plant calls.
        let direct = rithmic_plants::fake::FakeSession::new();
        direct.connect().await.unwrap();
        direct.fail_next("place_order", fail);
        let via_direct = direct.place_order(&place("t")).await.unwrap_err();

        // Gateway: the same failure scripted at the parent's plant.
        let parent = FakeParent::start(Opts {
            trading: true,
            ..Opts::default()
        })
        .await;
        let s = session_for(&parent).await;
        parent
            .command(&format!("fail_next place_order {kind} boom"))
            .await;
        let via_gateway = s.place_order(&place("t")).await.unwrap_err();

        assert_eq!(class_of(&via_direct), want, "direct {kind}: {via_direct}");
        assert_eq!(
            class_of(&via_gateway),
            want,
            "gateway {kind}: {via_gateway}"
        );
    }
}

#[tokio::test]
async fn a_gateway_denial_is_a_denial_not_a_venue_class() {
    let parent = FakeParent::start(Opts::default()).await; // trading off
    let s = session_for(&parent).await;
    let err = s.place_order(&place("t")).await.unwrap_err();
    assert_eq!(class_of(&err), "denied", "{err}");
}

#[tokio::test]
async fn the_adapter_is_told_with_the_same_down_and_up_events_a_redial_produces() {
    let parent = FakeParent::start(Opts::default()).await;
    let s = session_for(&parent).await;
    s.subscribe(NQ, CME).await.unwrap();
    let mut events = s.subscribe_transport();
    parent.clear_calls().await;
    parent.command("drop_plants").await;
    let down = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("down in time")
        .unwrap();
    let up = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("up in time")
        .unwrap();
    assert!(
        matches!(
            down,
            rithmic_gateway::client::TransportEvent::Down { generation: 1 }
        ),
        "{down:?}"
    );
    assert!(
        matches!(
            up,
            rithmic_gateway::client::TransportEvent::Up { generation: 2 }
        ),
        "{up:?}"
    );
}

#[tokio::test]
async fn a_market_data_join_that_cannot_be_restored_degrades_instead_of_wedging_the_gateway() {
    let parent = FakeParent::start(Opts::default()).await;
    let s = session_for(&parent).await;
    s.subscribe(NQ, CME).await.unwrap();
    parent.clear_calls().await;
    // Every restore pass fails (three passes), so the join cannot be brought back.
    for _ in 0..3 {
        parent
            .command("fail_next subscribe rithmic contract expired")
            .await;
    }
    parent.command("drop_plants").await;

    // The gateway still becomes ready and tells the client, rather than staying
    // not-ready forever behind the stuck join.
    assert!(wait_until(Duration::from_secs(15), || s.transport_generation() == 2).await);
    assert_eq!(s.ensure_transport_live().await.unwrap(), 2);
    let attempts = parent.count("subscribe NQH6").await;
    assert_eq!(attempts, 3, "exactly the bounded passes, no more");

    // The client's own subscribe (what its recovery replay does) retries the
    // degraded join once and heals it.
    s.subscribe(NQ, CME)
        .await
        .expect("the retry reaches the venue");
    assert_eq!(parent.count("subscribe NQH6").await, attempts + 1);
    parent.push_last_trades(NQ, 1).await;
    assert_eq!(drain(&s, 1, market).await.len(), 1);
    // Healed: a further subscribe is a plain no-op again.
    s.subscribe(NQ, CME).await.unwrap();
    assert_eq!(parent.count("subscribe NQH6").await, attempts + 1);
}

#[tokio::test]
async fn a_failed_retry_of_a_degraded_join_reports_the_venue_error_and_stays_degraded() {
    let parent = FakeParent::start(Opts::default()).await;
    let s = session_for(&parent).await;
    s.subscribe(NQ, CME).await.unwrap();
    for _ in 0..3 {
        parent.command("fail_next subscribe rithmic expired").await;
    }
    parent.command("drop_plants").await;
    assert!(wait_until(Duration::from_secs(15), || s.transport_generation() == 2).await);

    parent
        .command("fail_next subscribe rithmic still expired")
        .await;
    let err = s.subscribe(NQ, CME).await.unwrap_err();
    assert!(err.to_string().contains("still expired"), "{err}");
    // Still degraded: the next subscribe tries again, and now succeeds.
    s.subscribe(NQ, CME).await.expect("healed on the next try");
}

#[tokio::test]
async fn a_panicking_handler_answers_unknown_and_leaks_nothing() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    let s = session_for(&parent).await;
    s.subscribe(NQ, CME).await.unwrap();

    parent.command("fail_next place_order panic boom").await;
    let err = s.place_order(&place("panics")).await.unwrap_err();
    assert!(matches!(err, Error::OutcomeUnknown(_)), "{err:?}");
    // The connection keeps serving...
    s.place_order(&place("after"))
        .await
        .expect("next place served");
    // ...and its teardown still releases what it held.
    s.disconnect().await.unwrap();
    assert!(
        wait_until_async(Duration::from_secs(5), || async {
            parent.count(&format!("unsubscribe {NQ} {CME}")).await == 1
        })
        .await,
        "{:?}",
        parent.calls().await
    );
}

#[tokio::test]
async fn order_peers_are_told_when_the_order_plant_is_reseated() {
    let parent = FakeParent::start(Opts {
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
