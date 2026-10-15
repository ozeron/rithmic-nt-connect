//! Market delivery after intent rejoin, snapshot-on-join and plant recovery.
//! Every case uses both poll surfaces, both attachments, and both venue layers.
use std::sync::Arc;
use std::time::Duration;

use rithmic_gateway::client::{GatewayConfig, RemoteSession};
use rithmic_gateway::idle_exit::{IdleExit, IdleExitPolicy};
use rithmic_gateway::server::{boot_gateway_metadata, Fingerprint, GatewayState};
use rithmic_host::{
    fake::FakeVenue, HistoryPolicy, OrderBookDto, ParentGates, PlantEvent, PlantSet, Session,
    SessionEvent, SessionHost, Stream, Timing, Venue, VenueHandle,
};
use rithmic_plants::fake_server::{frames, FakeRithmicServer, Plant};
use rithmic_plants::RithmicVenue;

const SYMBOL: &str = "NQZ6";
const EXCHANGE: &str = "CME";
const WAIT: Duration = Duration::from_secs(3);

enum Feed {
    Fake(Arc<FakeVenue>),
    Wire(FakeRithmicServer, Arc<RithmicVenue>),
}

struct Rig {
    feed: Feed,
    state: Arc<GatewayState>,
    remote: bool,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}
impl Rig {
    async fn new(wire: bool, remote: bool) -> Self {
        let (feed, venue): (_, Arc<dyn Venue>) = if wire {
            let server = FakeRithmicServer::start().await;
            server.front_month("ES", EXCHANGE, "ESZ6");
            let venue = Arc::new(RithmicVenue::new(
                server.session_config(),
                PlantSet::MARKET_DATA,
            ));
            (Feed::Wire(server, venue.clone()), venue)
        } else {
            let venue = Arc::new(FakeVenue::new());
            (Feed::Fake(venue.clone()), venue)
        };
        let host = SessionHost::with_handle(
            Some(VenueHandle::new(venue)),
            ParentGates::default(),
            Timing {
                error_backoff: Duration::from_millis(1),
                connect_retry: Duration::from_millis(1),
                restore_backoff: Duration::from_millis(1),
            },
            HistoryPolicy::Allow,
            false,
            rithmic_host::fanout::DEFAULT_QUEUE_CAP,
        );
        host.connect().await.unwrap();
        Self {
            feed,
            state: Arc::new(GatewayState {
                host,
                fingerprint: Fingerprint {
                    user: "rejoin-offline".into(),
                    system_name: "fake".into(),
                    url: "ws://unused.example".into(),
                    env: "Test".into(),
                    account_id: String::new(),
                    fcm_id: String::new(),
                    ib_id: String::new(),
                },
                expected_auth_token: "duplex-rejoin".into(),
                idle: IdleExit::new(IdleExitPolicy::Never),
                gateway_instance_id: boot_gateway_metadata().0,
            }),
            remote,
            tasks: Vec::new(),
        }
    }
    async fn attach(&mut self) -> Arc<dyn Session> {
        let session: Arc<dyn Session> = if self.remote {
            let (client, server) = tokio::io::duplex(64 * 1024);
            let state = self.state.clone();
            self.tasks.push(tokio::spawn(async move {
                rithmic_gateway::server::serve_connection(server, state)
                    .await
                    .unwrap();
            }));
            Arc::new(RemoteSession::over(
                GatewayConfig::new("rejoin-offline", "fake", "ws://unused.example", "Test")
                    .with_auth_token("duplex-rejoin"),
                client,
            ))
        } else {
            Arc::new(self.state.host.attach())
        };
        session.connect().await.unwrap();
        session.subscribe(SYMBOL, EXCHANGE).await.unwrap();
        session
            .subscribe_order_book_summary(SYMBOL, EXCHANGE)
            .await
            .unwrap();
        session
    }
    fn push(&self, price: f64) {
        match &self.feed {
            Feed::Fake(venue) => {
                venue.push_event(
                    Stream::Ticker,
                    PlantEvent::OrderBook(OrderBookDto {
                        symbol: Some(SYMBOL.into()),
                        exchange: Some(EXCHANGE.into()),
                        update_type: Some(1),
                        bid_price: vec![price],
                        bid_size: vec![3],
                        ask_price: vec![price + 0.25],
                        ask_size: vec![4],
                        ssboe: Some(1_790_000_000),
                        usecs: Some(0),
                        ts_event_ns: Some(1_790_000_000_000_000_000),
                    }),
                );
                for p in [price, price + 1.0] {
                    venue.push_event(
                        Stream::Ticker,
                        rithmic_gateway::testing::last_trade(SYMBOL, EXCHANGE, p, 3),
                    );
                }
            }
            Feed::Wire(server, _) => {
                server.push(Plant::Ticker, &frames::order_book(SYMBOL, EXCHANGE, price));
                for p in [price, price + 1.0] {
                    server.push(
                        Plant::Ticker,
                        &frames::last_trade(SYMBOL, EXCHANGE, p, 3, 1_790_000_000),
                    );
                }
            }
        }
    }
    async fn fail_ticker(&self) {
        match &self.feed {
            Feed::Fake(venue) => venue.fail_stream(Stream::Ticker, "offline ticker break"),
            Feed::Wire(_, venue) => venue.fault_inject_drop_plant(Stream::Ticker).await.unwrap(),
        }
    }
    async fn shutdown(self) {
        self.state.host.shutdown().await;
        for task in self.tasks {
            task.abort();
        }
    }
}

async fn expect_fresh(session: &dyn Session, price: f64) {
    assert!(matches!(session.poll_event(WAIT).await.unwrap(),
        Some(PlantEvent::OrderBook(row)) if row.bid_price == [price]));
    let batch = session.next_events(Stream::Ticker, 1, WAIT).await.unwrap();
    assert!(
        matches!(&batch[..], [PlantEvent::LastTrade(row)] if row.trade_price == Some(price)),
        "next_events lost a fresh trade: {batch:?}"
    );
    assert!(matches!(session.poll_event(WAIT).await.unwrap(),
        Some(PlantEvent::LastTrade(row)) if row.trade_price == Some(price + 1.0)));
}

enum Change {
    Rejoin,
    PeerJoin,
    Recovery,
}
async fn scenario(wire: bool, remote: bool, change: Change) {
    let mut rig = Rig::new(wire, remote).await;
    let first = rig.attach().await;
    rig.push(100.0);
    expect_fresh(&*first, 100.0).await;
    let second = match change {
        Change::Rejoin => {
            first.subscribe(SYMBOL, EXCHANGE).await.unwrap();
            first
                .subscribe_order_book_summary(SYMBOL, EXCHANGE)
                .await
                .unwrap();
            None
        }
        Change::PeerJoin => Some(rig.attach().await),
        Change::Recovery => {
            let mut lifecycle = first.session_events();
            rig.fail_ticker().await;
            assert!(matches!(
                tokio::time::timeout(WAIT, lifecycle.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                SessionEvent::Interrupted { .. }
            ));
            assert!(matches!(
                tokio::time::timeout(WAIT, lifecycle.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                SessionEvent::Resumed { .. }
            ));
            None
        }
    };
    rig.push(200.0);
    expect_fresh(&*first, 200.0).await;
    if let Some(second) = second {
        expect_fresh(&*second, 200.0).await;
        second.disconnect().await.unwrap();
    }
    first.disconnect().await.unwrap();
    rig.shutdown().await;
}

macro_rules! cases {
    ($module:ident, $wire:expr, $remote:expr) => {
        mod $module {
            use super::*;
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn same_session_rejoin_keeps_ticker_and_book_delivery() {
                scenario($wire, $remote, Change::Rejoin).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn a_peers_book_refresh_keeps_both_sessions_delivery() {
                scenario($wire, $remote, Change::PeerJoin).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn ticker_recovery_resumes_fresh_delivery() {
                scenario($wire, $remote, Change::Recovery).await;
            }
        }
    };
}
cases!(fake_local, false, false);
cases!(fake_remote, false, true);
cases!(wire_local, true, false);
cases!(wire_remote, true, true);
