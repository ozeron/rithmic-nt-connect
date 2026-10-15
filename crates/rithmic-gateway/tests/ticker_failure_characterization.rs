//! Today's parent recovery is session-wide even for a ticker-only failure.
//! This deliberately characterizes the baseline for unify-session-host D3/D8;
//! per-plant recovery will require changing these expectations.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{read_frame, short_dir, write_body, CME, NQ, SYSTEM, URL};
use rithmic_gateway::idle_exit::IdleExitPolicy;
use rithmic_gateway::pb::{self, event, frame::Body, Frame};
use rithmic_gateway::server::pump::PumpTiming;
use rithmic_gateway::server::{run_gateway, Fingerprint, GatewayParams, SessionHandle};
use rithmic_gateway::subscriptions::ParentGates;
use rithmic_gateway::testing::{account_pnl, last_trade, order_notification};
use rithmic_host::fake::FakeVenue;
use rithmic_host::{Session, Stream};

use tokio::net::{UnixListener, UnixStream};

const DEADLINE: Duration = Duration::from_secs(5);

async fn receive(client: &mut UnixStream) -> Frame {
    tokio::time::timeout(DEADLINE, read_frame(client))
        .await
        .expect("gateway frame deadline")
        .expect("client remains attached")
        .0
}

async fn request(client: &mut UnixStream, id: u64, body: Body) -> Frame {
    write_body(client, id, body).await;
    let reply = receive(client).await;
    assert_eq!(
        reply.request_id, id,
        "unexpected unsolicited frame: {reply:?}"
    );
    reply
}

#[tokio::test]
async fn ticker_only_failure_rebuilds_all_plants_and_restores_account_intents() {
    let dir = short_dir();
    let path = dir.path().join("ticker-failure.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let fake = Arc::new(FakeVenue::new());
    fake.connect().await.unwrap();
    let parent = tokio::spawn(run_gateway(GatewayParams {
        listener,
        path: path.clone(),
        fingerprint: Fingerprint {
            user: "ticker-characterization".into(),
            system_name: SYSTEM.into(),
            url: URL.into(),
            env: "Test".into(),
            account_id: String::new(),
            fcm_id: String::new(),
            ib_id: String::new(),
        },
        expected_auth_token: String::new(),
        idle_policy: IdleExitPolicy::After(Duration::ZERO),
        gates: ParentGates {
            trading_enabled: true,
            cancel_all_enabled: false,
        },
        session: SessionHandle::new(fake.clone()),
        timing: PumpTiming::default(),
    }));
    let mut client = UnixStream::connect(&path).await.unwrap();
    let ready = request(
        &mut client,
        0,
        Body::Handshake(pb::Handshake {
            user: "ticker-characterization".into(),
            system_name: SYSTEM.into(),
            url: URL.into(),
            env: "Test".into(),
            ..Default::default()
        }),
    )
    .await;
    assert!(matches!(
        ready.body,
        Some(Body::Ready(pb::Ready {
            transport_generation: 1,
            ..
        }))
    ));

    for (id, body) in [
        Body::Subscribe(pb::SubscribeRequest {
            symbol: NQ.into(),
            exchange: CME.into(),
        }),
        Body::SubscribeBook(pb::SubscribeBookRequest {
            symbol: NQ.into(),
            exchange: CME.into(),
        }),
        Body::SubscribePnl(pb::SubscribePnlRequest {}),
        Body::SubscribeOrderUpdates(pb::SubscribeOrderUpdatesRequest {}),
    ]
    .into_iter()
    .enumerate()
    {
        let reply = request(&mut client, id as u64 + 2, body).await;
        assert!(matches!(reply.body, Some(Body::Ack(_))), "{reply:?}");
    }
    fake.clear_calls();

    // This fails only the ticker lane, not drop_plants() or all poll methods.
    // No queued events or overflow: any gap here would be recovery-generated.
    fake.fail_stream(Stream::Ticker, "ticker-only connection lost");
    let resumed = receive(&mut client).await;
    assert_eq!(resumed.request_id, 0);
    assert_eq!(
        resumed.body,
        Some(Body::Event(pb::Event {
            body: Some(event::Body::TransportResumed(pb::TransportResumed {
                generation: 2,
            })),
        })),
        "the first recovery frame is Resumed, with no interruption or gap before it"
    );

    // connect() attaches ticker AND history. Full disconnect drops all four;
    // restore then brings PnL and order back through their account joins.
    let expected_calls = [
        "disconnect",
        "connect",
        "subscribe NQH6 CME",
        "subscribe_book NQH6 CME",
        "ensure_pnl_plant",
        "subscribe_pnl",
        "subscribe_order_updates",
    ];
    assert_eq!(fake.calls(), expected_calls);
    // The exact trace also rules out reset_ticker, per-plant disconnects,
    // client replay, and duplicate account subscriptions.

    // The SAME socket still owns all its fan-out queues after recovery.
    fake.push_event(Stream::Ticker, last_trade(NQ, CME, 100.25, 1));
    fake.push_event(
        Stream::Ticker,
        rithmic_host::dto::PlantEvent::OrderBook(rithmic_host::dto::OrderBookDto {
            symbol: Some(NQ.into()),
            exchange: Some(CME.into()),
            update_type: Some(1),
            bid_price: vec![100.0],
            bid_size: vec![2],
            ask_price: vec![100.5],
            ask_size: vec![3],
            ssboe: Some(1_700_000_000),
            usecs: Some(0),
            ts_event_ns: Some(1_700_000_000_000_000_000),
        }),
    );
    fake.push_event(Stream::Pnl, account_pnl("ACCT", "12.50"));
    fake.push_event(
        Stream::Order,
        order_notification("accepted", "basket-1", "tag-1", NQ),
    );
    let mut kinds = Vec::new();
    for _ in 0..4 {
        let frame = receive(&mut client).await;
        assert_eq!(frame.request_id, 0);
        let Some(Body::Event(event)) = frame.body else {
            panic!("expected restored stream event: {frame:?}");
        };
        kinds.push(match event.body {
            Some(event::Body::LastTrade(_)) => "ticker",
            Some(event::Body::OrderBook(_)) => "book",
            Some(event::Body::AccountPnl(_)) => "pnl",
            Some(event::Body::OrderNotification(_)) => "order",
            other => panic!("unexpected recovery event (including gaps): {other:?}"),
        });
    }
    kinds.sort_unstable();
    assert_eq!(kinds, ["book", "order", "pnl", "ticker"]);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), read_frame(&mut client))
            .await
            .is_err(),
        "no additional recovery notifications or automatic gaps"
    );
    assert_eq!(fake.calls(), expected_calls, "no client-side re-subscribe");

    let disconnected = request(&mut client, 6, Body::Disconnect(pb::DisconnectRequest {})).await;
    assert!(matches!(disconnected.body, Some(Body::Ack(_))));
    drop(client);
    tokio::time::timeout(DEADLINE, parent)
        .await
        .expect("parent exits after its last client detaches")
        .expect("parent task")
        .expect("parent shutdown");
}
