//! One contract, two sessions: every scenario here runs against `FakeVenue`
//! and against the real `RithmicVenue` connected to the fake Rithmic venue, so
//! the fake the gateway tests rely on cannot drift from direct-mode behavior.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rithmic_host::fake::{FakeFailure, FakeVenue};
use rithmic_host::{Error, PlaceOrder, PlantSet, Session, Stream};
use rithmic_plants::fake_server::{template, FakeRithmicServer, Reply};
use rithmic_plants::RithmicVenue;

/// How a scenario drives one implementation.
#[async_trait]
trait Harness: Send + Sync + Sized {
    /// A connected session with the trading gate as given.
    async fn open(trading: bool) -> Self;
    fn session(&self) -> Arc<dyn Session>;
    /// The next place reaches the venue and its outcome is unknown.
    fn next_place_unknown(&self);
    /// The venue refuses the next place.
    fn next_place_rejected(&self);
    /// Make `disconnect` take `delay` before it completes.
    fn slow_disconnect(&self, delay: Duration);
    /// How many place requests reached the venue.
    fn places_sent(&self) -> usize;
}

struct Fake(Arc<FakeVenue>);

#[async_trait]
impl Harness for Fake {
    async fn open(trading: bool) -> Self {
        let fake = Arc::new(FakeVenue::new().with_gates(trading, false));
        fake.connect().await.unwrap();
        Self(fake)
    }
    fn session(&self) -> Arc<dyn Session> {
        self.0.clone()
    }
    fn next_place_unknown(&self) {
        self.0.fail_next(
            "place_order",
            FakeFailure::Unknown("connection lost".into()),
        );
    }
    fn next_place_rejected(&self) {
        self.0
            .fail_next("place_order", FakeFailure::Rejected("bad price".into()));
    }
    fn slow_disconnect(&self, delay: Duration) {
        self.0.set_delay("disconnect", delay);
    }
    fn places_sent(&self) -> usize {
        // A failed call is recorded too; only the ok ones count as sent here.
        self.0.count("place_order")
    }
}

struct Direct {
    venue: FakeRithmicServer,
    session: Arc<RithmicVenue>,
}

#[async_trait]
impl Harness for Direct {
    async fn open(trading: bool) -> Self {
        let venue = FakeRithmicServer::start().await;
        venue.front_month("ES", "CME", "ESZ6");
        let session = Arc::new(
            RithmicVenue::new(venue.session_config(), PlantSet::MARKET_DATA)
                .with_gates(trading, false),
        );
        session.connect().await.unwrap();
        Self { venue, session }
    }
    fn session(&self) -> Arc<dyn Session> {
        self.session.clone()
    }
    fn next_place_unknown(&self) {
        self.venue
            .script(template::NEW_ORDER, Reply::drop_connection());
    }
    fn next_place_rejected(&self) {
        self.venue.script(
            template::NEW_ORDER,
            Reply::reject(template::NEW_ORDER, "3", "bad price"),
        );
    }
    fn slow_disconnect(&self, delay: Duration) {
        self.venue
            .always(template::LOGOUT, Reply::ok(template::LOGOUT).after(delay));
    }
    fn places_sent(&self) -> usize {
        self.venue.received_of(template::NEW_ORDER).len()
    }
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

fn is_trading_disabled(err: &Error) -> bool {
    matches!(err, Error::Gateway { code, .. } if code == "trading_disabled")
}

async fn trading_gate_denies_before_anything_is_sent<H: Harness>() {
    let h = H::open(false).await;
    let s = h.session();
    for err in [
        s.place_order(&order("t")).await.unwrap_err(),
        s.cancel_order("B1").await.unwrap_err(),
        s.subscribe_order_updates().await.unwrap_err(),
        s.ensure_order_plant().await.unwrap_err(),
        s.load_orders(0, 0).await.unwrap_err(),
    ] {
        assert!(
            is_trading_disabled(&err),
            "expected trading_disabled, got {err:?}"
        );
    }
    assert_eq!(h.places_sent(), 0);
}

async fn outcome_classes_survive_the_session<H: Harness>() {
    let h = H::open(true).await;
    let s = h.session();
    s.subscribe_order_updates().await.unwrap();

    h.next_place_rejected();
    let err = s.place_order(&order("rej")).await.unwrap_err();
    assert!(matches!(err, Error::Rejected(_)), "reject: {err:?}");

    h.next_place_unknown();
    let err = s.place_order(&order("unk")).await.unwrap_err();
    assert!(matches!(err, Error::OutcomeUnknown(_)), "unknown: {err:?}");
}

async fn cancel_brings_up_the_order_plant<H: Harness>() {
    let h = H::open(true).await;
    let s = h.session();
    let err = s.poll_order_event(Duration::ZERO).await.unwrap_err();
    assert!(
        matches!(err, Error::NotConnected { plant: "order" }),
        "no order plant yet: {err:?}"
    );
    s.cancel_order("B1").await.unwrap();
    assert!(s
        .next_events(Stream::Order, 1, Duration::ZERO)
        .await
        .is_ok());
}

async fn order_op_during_disconnect_is_not_sent<H: Harness>() {
    let h = H::open(true).await;
    let s = h.session();
    s.ensure_order_plant().await.unwrap();
    h.slow_disconnect(Duration::from_millis(400));

    let closing = {
        let s = s.clone();
        tokio::spawn(async move { s.disconnect().await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let err = s.place_order(&order("late")).await.unwrap_err();
    assert!(
        matches!(err, Error::NotSent(_)),
        "during disconnect: {err:?}"
    );
    closing.await.unwrap().unwrap();
}

async fn an_explicitly_disconnected_order_plant_stays_down<H: Harness>() {
    let h = H::open(true).await;
    let s = h.session();
    s.ensure_order_plant().await.unwrap();
    s.disconnect_order_plant().await.unwrap();

    let err = s.place_order(&order("after-stop")).await.unwrap_err();
    assert!(
        matches!(err, Error::NotSent(_)),
        "after an explicit disconnect: {err:?}"
    );
    assert_eq!(h.places_sent(), 0);

    s.ensure_order_plant().await.unwrap();
    s.place_order(&order("reopened")).await.unwrap();
}

macro_rules! contract {
    ($($scenario:ident),* $(,)?) => {
        mod fake_session {
            use super::*;
            $(#[tokio::test] async fn $scenario() { super::$scenario::<Fake>().await })*
        }
        mod direct_session {
            use super::*;
            $(#[tokio::test] async fn $scenario() { super::$scenario::<Direct>().await })*
        }
    };
}

contract!(
    trading_gate_denies_before_anything_is_sent,
    outcome_classes_survive_the_session,
    cancel_brings_up_the_order_plant,
    order_op_during_disconnect_is_not_sent,
    an_explicitly_disconnected_order_plant_stays_down,
);
