use super::*;

async fn settle() {
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
}

async fn degrade_ticker(
    host: &SessionHost<FakeVenue>,
    fake: &FakeVenue,
    session: &LocalSession<FakeVenue>,
    failures: usize,
) {
    fake.clear_calls();
    for _ in 0..failures {
        fake.fail_next(
            "subscribe",
            FakeFailure::Rejected("temporarily unavailable".into()),
        );
    }
    let mut events = session.session_events();
    fake.fail_stream(Stream::Ticker, "ticker lost");
    resumed(&mut events, 2).await;
    assert_eq!(fake.count("subscribe NQU6"), 3);
    assert_eq!(host.state.reconnect.degraded_count().await, 1);
    // Let the pump enter its post-recovery wait before advancing the clock.
    tokio::task::yield_now().await;
}

#[tokio::test(start_paused = true)]
async fn degraded_intent_recovers_without_a_consumer_resubscribe() {
    let (host, fake, a) = running().await;
    let b = host.attach();
    a.subscribe("NQU6", "CME").await.unwrap();
    b.subscribe("NQU6", "CME").await.unwrap();
    degrade_ticker(&host, &fake, &a, 5).await;
    tokio::time::advance(Duration::from_millis(999)).await;
    tokio::task::yield_now().await;
    assert_eq!(fake.count("subscribe NQU6"), 3, "retry must back off");
    for (seconds, expected) in [(1, 4), (2, 5), (4, 6)] {
        tokio::time::advance(Duration::from_secs(seconds)).await;
        settle().await;
        assert_eq!(
            fake.count("subscribe NQU6"),
            expected,
            "host did not retry degraded intent"
        );
    }
    assert_eq!(host.state.reconnect.degraded_count().await, 0);
    fake.push_event(Stream::Ticker, tick(42));
    for holder in [&a, &b] {
        assert_eq!(holder.poll_event(WAIT).await.unwrap(), Some(tick(42)));
    }
    assert_eq!(host.generation(), 2, "no extra plant rebuild");
    assert_eq!(fake.count("connect"), 0);
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn degraded_retries_stop_when_the_last_holder_leaves() {
    for drop_last in [false, true] {
        let (host, fake, a) = running().await;
        let b = host.attach();
        let keep_host_alive = host.attach();
        a.subscribe("NQU6", "CME").await.unwrap();
        b.subscribe("NQU6", "CME").await.unwrap();
        degrade_ticker(&host, &fake, &a, 20).await;
        a.disconnect().await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        settle().await;
        assert_eq!(
            fake.count("subscribe NQU6"),
            4,
            "remaining holder still needs retries"
        );
        if drop_last {
            drop(b);
            until(|| fake.count("unsubscribe NQU6") == 1).await;
        } else {
            b.disconnect().await.unwrap();
        }
        assert_eq!(host.state.reconnect.degraded_count().await, 0);
        for _ in 0..4 {
            tokio::time::advance(Duration::from_secs(60)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(fake.count("subscribe NQU6"), 4, "retry after last release");
        keep_host_alive.disconnect().await.unwrap();
        host.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn degraded_book_refresh_recovers_a_snapshot_for_all_holders() {
    let (host, fake, a) = running().await;
    let b = host.attach();
    for holder in [&a, &b] {
        holder
            .subscribe_order_book_summary("NQU6", "CME")
            .await
            .unwrap();
    }
    fake.clear_calls();
    fake.fail_next(
        "refresh_book",
        FakeFailure::Rejected("snapshot unavailable".into()),
    );
    assert!(a.subscribe_order_book_summary("NQU6", "CME").await.is_err());
    settle().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    settle().await;
    assert_eq!(
        fake.count("refresh_book NQU6"),
        2,
        "host must refresh a degraded book"
    );
    assert_eq!(
        fake.count("subscribe_book"),
        0,
        "bare repeat joins are rejected by Rithmic"
    );
    assert_eq!(host.state.reconnect.degraded_count().await, 0);
    let snapshot = PlantEvent::OrderBook(rithmic_host::OrderBookDto {
        symbol: Some("NQU6".into()),
        exchange: Some("CME".into()),
        update_type: Some(3),
        bid_price: vec![21000.0],
        bid_size: vec![1],
        ask_price: vec![21000.25],
        ask_size: vec![2],
        ssboe: Some(1_790_000_000),
        usecs: Some(0),
        ts_event_ns: Some(1_790_000_000_000_000_000),
    });
    fake.push_event(Stream::Ticker, snapshot.clone());
    for holder in [&a, &b] {
        assert_eq!(
            holder.poll_event(WAIT).await.unwrap(),
            Some(snapshot.clone())
        );
    }
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn degraded_retry_backoff_grows_and_caps_at_sixty_seconds() {
    let (host, fake, a) = running().await;
    a.subscribe("NQU6", "CME").await.unwrap();
    degrade_ticker(&host, &fake, &a, 20).await;
    for (step, seconds) in [1, 2, 4, 8, 16, 32, 60, 60].into_iter().enumerate() {
        let half = Duration::from_secs(seconds) / 2;
        tokio::time::advance(half).await;
        settle().await;
        assert_eq!(fake.count("subscribe NQU6"), 3 + step, "retry too early");
        tokio::time::advance(half).await;
        settle().await;
        assert_eq!(
            fake.count("subscribe NQU6"),
            4 + step,
            "retry exceeded bounded delay"
        );
    }
    host.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn degraded_time_bars_recover_without_a_consumer_resubscribe() {
    let (host, fake, a) = running().await;
    a.subscribe_time_bars("NQU6", "CME", 2, 1).await.unwrap();
    fake.clear_calls();
    for _ in 0..3 {
        fake.fail_next(
            "subscribe_time_bars",
            FakeFailure::Rejected("bar join unavailable".into()),
        );
    }
    let mut events = a.session_events();
    fake.fail_stream(Stream::History, "history lost");
    resumed(&mut events, 2).await;
    settle().await;
    assert_eq!(host.state.reconnect.degraded_count().await, 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    settle().await;
    assert_eq!(fake.count("subscribe_time_bars NQU6"), 4);
    assert_eq!(host.state.reconnect.degraded_count().await, 0);
    let bar = PlantEvent::TimeBar(rithmic_host::HistoryBarDto {
        symbol: Some("NQU6".into()),
        exchange: Some("CME".into()),
        bar_type: Some(2),
        period: Some("60".into()),
        marker: Some(1_790_000_000),
        open_price: Some(21000.0),
        high_price: Some(21001.0),
        low_price: Some(20999.0),
        close_price: Some(21000.25),
        volume: Some(8),
        num_trades: Some(4),
        bid_volume: Some(3),
        ask_volume: Some(5),
        ts_event_ns: Some(1_790_000_000_000_000_000),
    });
    fake.push_event(Stream::History, bar.clone());
    assert_eq!(a.poll_history_event(WAIT).await.unwrap(), Some(bar));
    host.shutdown().await;
}
