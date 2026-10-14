//! The unmodified direct session against the fake Rithmic venue.

use rithmic_plants::fake_venue::{template, FakeVenue, Plant};
use rithmic_plants::{DirectSession, PlantSet, Session};

/// A venue that answers the history readiness probe (front month + empty replay).
async fn venue() -> FakeVenue {
    let venue = FakeVenue::start().await;
    venue.front_month("ES", "CME", "ESZ6");
    venue
}

#[tokio::test]
async fn direct_session_connects_logs_in_and_disconnects() {
    let venue = venue().await;
    let session = DirectSession::new(venue.session_config(), PlantSet::MARKET_DATA);

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

use rithmic_plants::fake_venue::{frames, Reply};
use rithmic_plants::{Error, PlaceOrder, PlantEvent, Stream};

const WAIT: Duration = Duration::from_secs(5);

async fn connected(venue: &FakeVenue, plants: PlantSet) -> DirectSession {
    let session = DirectSession::new(venue.session_config(), plants).with_gates(true, false);
    session
        .connect()
        .await
        .expect("connect against the fake venue");
    session
}

async fn next_event(session: &DirectSession, stream: Stream) -> PlantEvent {
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
    let session = DirectSession::new(venue.session_config(), PlantSet::MARKET_DATA);

    session.connect().await.expect_err("history login refused");

    let logouts = venue.wait_for(template::LOGOUT, 1).await;
    assert!(
        logouts.iter().any(|r| r.plant == Some(Plant::Ticker)),
        "the ticker that logged in is logged out again"
    );
    venue.wait_connections(Plant::Ticker, 0).await;
}
