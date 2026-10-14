//! Golden tests for the venue → `PlantEvent` decode: venue-shaped frames go
//! through the real `rithmic-rs` decoder (via the fake venue) and the session's
//! conversion, and every field the adapter reads is pinned.

use std::time::Duration;

use rithmic_plants::fake_venue::{frames, template, FakeVenue, Plant};
use rithmic_plants::{DirectSession, PlantEvent, PlantSet, Session, Stream};
use rithmic_rs::rti;

const T: i32 = 1_790_000_000;
const T_NS: u64 = 1_790_000_000_000_000_000;

async fn session() -> (FakeVenue, DirectSession) {
    let venue = FakeVenue::start().await;
    venue.front_month("ES", "CME", "ESZ6");
    let session =
        DirectSession::new(venue.session_config(), PlantSet::EXECUTION).with_gates(true, false);
    session.connect().await.unwrap();
    (venue, session)
}

async fn one(session: &DirectSession, stream: Stream) -> PlantEvent {
    session
        .next_events(stream, 1, Duration::from_secs(5))
        .await
        .unwrap()
        .into_iter()
        .next()
        .expect("an event")
}

#[tokio::test]
async fn best_bid_offer() {
    let (venue, s) = session().await;
    let mut m = rti::BestBidOffer::default();
    m.template_id = frames::BEST_BID_OFFER;
    m.symbol = Some("ESZ6".into());
    m.exchange = Some("CME".into());
    m.bid_price = Some(5000.0);
    m.bid_size = Some(12);
    m.ask_price = Some(5000.25);
    m.ask_size = Some(7);
    m.is_snapshot = Some(true);
    m.ssboe = Some(T);
    m.usecs = Some(5);
    venue.push(Plant::Ticker, &m);
    match one(&s, Stream::Ticker).await {
        PlantEvent::Bbo(b) => {
            assert_eq!((b.bid_price, b.bid_size), (Some(5000.0), Some(12)));
            assert_eq!((b.ask_price, b.ask_size), (Some(5000.25), Some(7)));
            assert_eq!(b.is_snapshot, Some(true));
            assert_eq!(b.ts_event_ns, Some(T_NS + 5_000));
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn order_book_levels_and_update_type() {
    let (venue, s) = session().await;
    let mut m = rti::OrderBook::default();
    m.template_id = frames::ORDER_BOOK;
    m.symbol = Some("ESZ6".into());
    m.exchange = Some("CME".into());
    m.update_type = Some(7);
    m.bid_price = vec![5000.0, 4999.75];
    m.bid_size = vec![3, 0];
    m.ask_price = vec![5000.25];
    m.ask_size = vec![4];
    m.ssboe = Some(T);
    m.usecs = Some(0);
    venue.push(Plant::Ticker, &m);
    match one(&s, Stream::Ticker).await {
        PlantEvent::OrderBook(b) => {
            assert_eq!(b.update_type, Some(7));
            assert_eq!(b.bid_price, vec![5000.0, 4999.75]);
            assert_eq!(b.bid_size, vec![3, 0]);
            assert_eq!(b.ask_price, vec![5000.25]);
            assert_eq!(b.ask_size, vec![4]);
            assert_eq!(b.ts_event_ns, Some(T_NS));
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn rithmic_order_notification() {
    let (venue, s) = session().await;
    s.subscribe_order_updates().await.unwrap();
    let mut m = frames::rithmic_order("B7", "tag-7", 15, "complete");
    m.completion_reason = Some("Cancelled".into());
    m.status = Some("cancelled".into());
    venue.push(Plant::Order, &m);
    match one(&s, Stream::Order).await {
        PlantEvent::OrderNotification(n) => {
            assert_eq!(n.source, "rithmic");
            assert_eq!(n.notify_type_name.as_deref(), Some("COMPLETE"));
            assert_eq!(n.kind.as_deref(), Some("canceled"));
            assert_eq!(n.basket_id.as_deref(), Some("B7"));
            assert_eq!(n.user_tag.as_deref(), Some("tag-7"));
            assert_eq!(n.completion_reason.as_deref(), Some("Cancelled"));
            assert_eq!(n.fill_id, None, "rithmic-side notifications carry no fill");
            assert_eq!(n.ts_event_ns, Some(T_NS));
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn exchange_fill_notification() {
    let (venue, s) = session().await;
    s.subscribe_order_updates().await.unwrap();
    let mut m = rti::ExchangeOrderNotification::default();
    m.template_id = frames::EXCHANGE_ORDER_NOTIFICATION;
    m.notify_type = Some(5); // FILL
    m.basket_id = Some("B8".into());
    m.user_tag = Some("tag-8".into());
    m.account_id = Some("ACCOUNT".into());
    m.symbol = Some("ESZ6".into());
    m.exchange = Some("CME".into());
    m.quantity = Some(2);
    m.fill_size = Some(1);
    m.fill_price = Some(5000.5);
    m.fill_id = Some("FILL-1".into());
    m.total_fill_size = Some(1);
    m.total_unfilled_size = Some(1);
    m.ssboe = Some(T);
    m.usecs = Some(1);
    venue.push(Plant::Order, &m);
    match one(&s, Stream::Order).await {
        PlantEvent::OrderNotification(n) => {
            assert_eq!(n.source, "exchange");
            assert_eq!(n.kind.as_deref(), Some("filled"));
            assert_eq!(n.fill_id.as_deref(), Some("FILL-1"));
            assert_eq!((n.fill_size, n.fill_price), (Some(1), Some(5000.5)));
            assert_eq!(
                (n.total_fill_size, n.total_unfilled_size),
                (Some(1), Some(1))
            );
            assert_eq!(n.ts_event_ns, Some(T_NS + 1_000));
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn instrument_pnl() {
    let (venue, s) = session().await;
    s.subscribe_pnl().await.unwrap();
    let mut m = rti::InstrumentPnLPositionUpdate::default();
    m.template_id = frames::INSTRUMENT_PNL;
    m.account_id = Some("ACCOUNT".into());
    m.symbol = Some("ESZ6".into());
    m.exchange = Some("CME".into());
    m.product_code = Some("ES".into());
    m.net_quantity = Some(-2);
    m.avg_open_fill_price = Some(5001.0);
    m.open_position_pnl = Some("-25.0".into());
    m.is_snapshot = Some(true);
    venue.push(Plant::Pnl, &m);
    match one(&s, Stream::Pnl).await {
        PlantEvent::InstrumentPnl(p) => {
            assert_eq!(p.symbol.as_deref(), Some("ESZ6"));
            assert_eq!(p.product_code.as_deref(), Some("ES"));
            assert_eq!(p.net_quantity, Some(-2));
            assert_eq!(p.avg_open_fill_price, Some(5001.0));
            assert_eq!(p.open_position_pnl.as_deref(), Some("-25.0"));
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn live_time_bar_on_the_history_plant() {
    let (venue, s) = session().await;
    s.subscribe_time_bars("ESZ6", "CME", 2, 1).await.unwrap();
    assert_eq!(venue.received_of(template::TIME_BAR_UPDATE).len(), 1);
    let mut m = rti::TimeBar::default();
    m.template_id = 250;
    m.symbol = Some("ESZ6".into());
    m.exchange = Some("CME".into());
    m.r#type = Some(2);
    m.period = Some("60".into());
    m.marker = Some(T + 60);
    m.open_price = Some(5000.0);
    m.high_price = Some(5001.0);
    m.low_price = Some(4999.5);
    m.close_price = Some(5000.75);
    m.volume = Some(321);
    m.num_trades = Some(40);
    venue.push(Plant::History, &m);
    match one(&s, Stream::History).await {
        PlantEvent::TimeBar(b) => {
            assert_eq!(
                (b.open_price, b.high_price, b.low_price, b.close_price),
                (Some(5000.0), Some(5001.0), Some(4999.5), Some(5000.75))
            );
            assert_eq!(b.volume, Some(321));
            // The venue marker is the bar close.
            assert_eq!(b.ts_event_ns, Some(T_NS + 60_000_000_000));
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn bracket_update_is_typed() {
    let (venue, s) = session().await;
    s.subscribe_bracket_updates().await.unwrap();
    let mut m = rti::BracketUpdates::default();
    m.template_id = 353;
    m.account_id = Some("ACCOUNT".into());
    m.basket_id = Some("B9".into());
    m.stop_ticks = Some(8);
    m.stop_quantity = Some(1);
    m.target_ticks = Some(16);
    m.target_quantity = Some(1);
    m.target_quantity_released = Some(0);
    venue.push(Plant::Order, &m);
    match one(&s, Stream::Order).await {
        PlantEvent::BracketUpdate(b) => {
            assert_eq!(b.basket_id.as_deref(), Some("B9"));
            assert_eq!((b.stop_ticks, b.target_ticks), (Some(8), Some(16)));
            assert_eq!((b.stop_quantity, b.target_quantity), (Some(1), Some(1)));
            assert_eq!(b.target_quantity_released, Some(0));
        }
        other => panic!("{other:?}"),
    }
}

fn bar_row(open: Option<f64>) -> rti::ResponseTimeBarReplay {
    let mut m = rti::ResponseTimeBarReplay::default();
    m.template_id = template::TIME_BAR_REPLAY + 1;
    m.symbol = Some("ESZ6".into());
    m.exchange = Some("CME".into());
    m.r#type = Some(2);
    m.period = Some("1".into());
    m.marker = Some(T + 60);
    m.open_price = open;
    m.high_price = Some(5001.0);
    m.low_price = Some(4999.0);
    m.close_price = Some(5000.5);
    m.volume = Some(10);
    m
}

#[tokio::test]
async fn a_probe_row_is_parsed_exactly_when_the_load_keeps_the_bar() {
    let (venue, s) = session().await;
    let rows = [bar_row(Some(5000.0)), bar_row(None)];
    let reply = rithmic_plants::fake_venue::Reply::rows(template::TIME_BAR_REPLAY + 1, &rows);
    venue.script(template::TIME_BAR_REPLAY, reply.clone());
    venue.script(template::TIME_BAR_REPLAY, reply);
    let probe = s
        .probe_time_bars("ESZ6", "CME", T, T + 120, 2, 1)
        .await
        .unwrap();
    let bars = s
        .load_time_bars("ESZ6", "CME", T, T + 120, 2, 1)
        .await
        .unwrap();
    assert_eq!(bars.len(), 1);
    assert!(probe[0].parsed);
    assert!(!probe[1].parsed);
    assert_eq!(
        probe[1].skip_reason.as_deref(),
        Some("missing open/high/low")
    );
}
