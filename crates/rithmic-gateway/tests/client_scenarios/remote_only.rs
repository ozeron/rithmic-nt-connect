//! Socket backpressure, fingerprint handshake, handler panic containment and shared-socket facade ownership. These have no local transport counterpart.
use super::{code, Instant};
use super::{common::*, drain, market, order, place, Duration, Error, Session};
use rithmic_gateway::client::{GatewayConfig, RemoteSession};
use rithmic_gateway::pb::{self, frame::Body};
use tokio::io::AsyncWriteExt;

async fn session_for(parent: &FakeParent) -> RemoteSession {
    let session = parent.session();
    session.connect().await.expect("connect");
    session
}

#[tokio::test]
async fn a_client_that_never_reads_is_dropped_without_hurting_its_peer() {
    let parent = FakeParent::start(Opts::default()).await;
    let fast = session_for(&parent).await;
    fast.subscribe(NQ, CME).await.unwrap();

    // A raw client that handshakes, subscribes, and then never reads again.
    let mut stuck = parent.stream();
    let hello = pb::Frame {
        request_id: 0,
        body: Some(Body::Handshake(pb::Handshake {
            user: parent.user.clone(),
            auth_token: AUTH_TOKEN.into(),
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
            Err(rithmic_host::Error::ChannelLagged { .. }) => {}
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

#[tokio::test]
async fn a_fingerprint_for_a_different_credential_set_is_refused_quickly() {
    let parent = FakeParent::start(Opts::default()).await;
    let cfg = GatewayConfig::new("someone-else", SYSTEM, URL, "Live").with_auth_token(AUTH_TOKEN);
    let started = Instant::now();
    let err = RemoteSession::over(cfg, parent.stream())
        .connect()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "fingerprint_mismatch");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "refused, not retried"
    );
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
async fn a_facade_dropped_without_disconnect_still_releases_what_only_it_held() {
    let parent = FakeParent::start(Opts::default()).await;
    let a = session_for(&parent).await;
    let b = session_for(&parent).await;
    a.subscribe("ESZ6", CME).await.unwrap();
    a.subscribe(NQ, CME).await.unwrap();
    b.subscribe(NQ, CME).await.unwrap();

    drop(a);
    assert!(
        wait_until_async(Duration::from_secs(5), || async {
            parent.count(&format!("unsubscribe ESZ6 {CME}")).await == 1
        })
        .await,
        "the dropped facade's sole intent is released: {:?}",
        parent.calls().await
    );
    assert_eq!(parent.count(&format!("unsubscribe {NQ} {CME}")).await, 0);

    // The dropped facade no longer counts as a holder, so b's release reaches
    // the parent.
    b.unsubscribe(NQ, CME).await.unwrap();
    assert!(
        wait_until_async(Duration::from_secs(5), || async {
            parent.count(&format!("unsubscribe {NQ} {CME}")).await == 1
        })
        .await,
        "{:?}",
        parent.calls().await
    );
}
