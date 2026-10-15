//! A ticker break recovers only ticker/book and signals interruption before resume.

mod common;

use std::time::Duration;

use common::{read_frame, write_body, FakeParent, Opts, AUTH_TOKEN, CME, NQ, SYSTEM, URL};
use rithmic_gateway::client::{GatewayConfig, RemoteSession};
use rithmic_gateway::pb::{self, event, frame::Body, Frame};
use rithmic_gateway::testing::{account_pnl, last_trade, order_notification};
use rithmic_host::{PlantEvent, Session, SessionEvent, Stream};

use tokio::io::DuplexStream;

const DEADLINE: Duration = Duration::from_secs(5);

async fn receive(client: &mut DuplexStream) -> Frame {
    tokio::time::timeout(DEADLINE, read_frame(client))
        .await
        .expect("gateway frame deadline")
        .expect("client remains attached")
        .0
}

async fn request(client: &mut DuplexStream, id: u64, body: Body) -> Frame {
    write_body(client, id, body).await;
    let reply = receive(client).await;
    assert_eq!(
        reply.request_id, id,
        "unexpected unsolicited frame: {reply:?}"
    );
    reply
}

#[tokio::test]
async fn remote_session_receives_host_interruption_resume_and_restored_ticker_over_duplex() {
    let parent = FakeParent::start(Opts::default()).await;
    let session = RemoteSession::over(
        GatewayConfig::new(&parent.user, SYSTEM, URL, "Live").with_auth_token(AUTH_TOKEN),
        parent.stream(),
    );
    session.connect().await.unwrap();
    session.subscribe(NQ, CME).await.unwrap();
    let mut events = session.session_events();
    parent
        .fake
        .push_event(Stream::Ticker, last_trade(NQ, CME, 100.25, 1));
    assert!(matches!(
        session.poll_event(DEADLINE).await.unwrap(),
        Some(PlantEvent::LastTrade(row)) if row.trade_price == Some(100.25)
    ));
    parent.fake.clear_calls();
    parent.fake.fail_stream(Stream::Ticker, "ticker-only break");
    assert!(matches!(
        tokio::time::timeout(DEADLINE, events.recv())
            .await
            .unwrap()
            .unwrap(),
        SessionEvent::Interrupted { generation: 1, .. }
    ));
    assert_eq!(
        tokio::time::timeout(DEADLINE, events.recv())
            .await
            .unwrap()
            .unwrap(),
        SessionEvent::Resumed { generation: 2 }
    );
    parent
        .fake
        .push_event(Stream::Ticker, last_trade(NQ, CME, 101.25, 2));
    assert!(matches!(
        session.poll_event(DEADLINE).await.unwrap(),
        Some(PlantEvent::LastTrade(row)) if row.trade_price == Some(101.25)
    ));
    assert_eq!(parent.fake.calls(), ["reset_ticker", "subscribe NQH6 CME"]);
    assert!(
        events.try_recv().is_err(),
        "one recovery pair on the existing transport"
    );
    session.disconnect().await.unwrap();
}

#[tokio::test]
async fn ticker_only_failure_preserves_sibling_plants_and_signals_interruption() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Default::default()
    })
    .await;
    let fake = &parent.fake;
    let mut client = parent.stream();
    let ready = request(
        &mut client,
        0,
        Body::Handshake(pb::Handshake {
            user: parent.user.clone(),
            auth_token: AUTH_TOKEN.into(),
            system_name: SYSTEM.into(),
            url: URL.into(),
            env: "Live".into(),
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
    let interrupted = receive(&mut client).await;
    assert_eq!(
        interrupted.body,
        Some(Body::Event(pb::Event {
            body: Some(event::Body::TransportInterrupted(
                pb::TransportInterrupted { generation: 1 }
            )),
        }))
    );
    let resumed = receive(&mut client).await;
    assert_eq!(resumed.request_id, 0);
    assert_eq!(
        resumed.body,
        Some(Body::Event(pb::Event {
            body: Some(event::Body::TransportResumed(pb::TransportResumed {
                generation: 2,
            })),
        })),
        "resume follows interruption on the same connection"
    );

    let expected_calls = [
        "reset_ticker",
        "subscribe NQH6 CME",
        "subscribe_book NQH6 CME",
    ];
    assert_eq!(fake.calls(), expected_calls);

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
}

async fn attached(parent: &FakeParent) -> DuplexStream {
    let mut client = parent.stream();
    let ready = request(
        &mut client,
        0,
        Body::Handshake(pb::Handshake {
            user: parent.user.clone(),
            auth_token: AUTH_TOKEN.into(),
            system_name: SYSTEM.into(),
            url: URL.into(),
            env: "Live".into(),
            ..Default::default()
        }),
    )
    .await;
    assert!(matches!(ready.body, Some(Body::Ready(_))));
    client
}

#[tokio::test]
async fn queued_order_precedes_interruption_and_every_connection_resumes() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Default::default()
    })
    .await;
    let mut a = attached(&parent).await;
    let mut b = attached(&parent).await;
    for client in [&mut a, &mut b] {
        let reply = request(
            client,
            1,
            Body::SubscribeOrderUpdates(pb::SubscribeOrderUpdatesRequest {}),
        )
        .await;
        assert!(matches!(reply.body, Some(Body::Ack(_))));
    }
    parent.fake.push_event(
        Stream::Order,
        order_notification("accepted", "B1", "tag", NQ),
    );
    parent
        .fake
        .fail_stream(Stream::Order, "lost after notification");
    for client in [&mut a, &mut b] {
        assert!(matches!(
            receive(client).await.body,
            Some(Body::Event(pb::Event {
                body: Some(event::Body::OrderNotification(_))
            }))
        ));
        assert!(matches!(
            receive(client).await.body,
            Some(Body::Event(pb::Event {
                body: Some(event::Body::TransportInterrupted(
                    pb::TransportInterrupted { generation: 1 }
                ))
            }))
        ));
        assert!(matches!(
            receive(client).await.body,
            Some(Body::Event(pb::Event {
                body: Some(event::Body::TransportResumed(pb::TransportResumed {
                    generation: 2
                }))
            }))
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn exhausted_recovery_flushes_interruption_then_closes_the_socket() {
    let parent = FakeParent::start(Opts::default()).await;
    let mut client = attached(&parent).await;
    for _ in 0..3 {
        parent.fake.fail_next(
            "reset_ticker",
            rithmic_host::fake::FakeFailure::Config("offline".into()),
        );
    }
    parent.fake.fail_stream(Stream::Ticker, "ticker lost");
    assert!(matches!(
        receive(&mut client).await.body,
        Some(Body::Event(pb::Event {
            body: Some(event::Body::TransportInterrupted(
                pb::TransportInterrupted { generation: 1 }
            ))
        }))
    ));
    // Recovery retries are 5s apart. No RecoveryFailed wire event is invented:
    // EOF is the existing client's transport-loss signal.
    let end = tokio::time::timeout(Duration::from_secs(20), read_frame(&mut client))
        .await
        .unwrap();
    assert!(end.is_err(), "expected EOF, got {end:?}");
}
