//! Existing recovery characterizations, retained unchanged for tracks A2/B2.
use super::{common::*, drain, market, order, place, Duration, PlantEvent, Session};
use rithmic_gateway::client::RemoteSession;

async fn session_for(parent: &FakeParent) -> RemoteSession {
    let session = parent.session();
    session.connect().await.expect("connect");
    session
}

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
async fn the_client_recovers_after_the_parent_restarts() {
    let mut parent = unix::UnixParent::start(Opts::default()).await;
    let s = RemoteSession::new(parent.config());
    s.connect().await.unwrap();
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
