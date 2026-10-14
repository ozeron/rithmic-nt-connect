//! Connection lanes: a slow read RPC must never delay a command, an event write
//! or the reading of the next frame, and commands keep their arrival order.
//! These drive the real server over a fake plant with raw frames on ONE
//! connection, the case the shared-socket client produces.

mod common;

use std::time::{Duration, Instant};

use common::*;
use rithmic_gateway::pb::{self, frame::Body};
use rithmic_gateway::rpc_table::{lane_of, Lane, RPC_TABLE};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

async fn connect(parent: &FakeParent) -> UnixStream {
    let mut stream = UnixStream::connect(&parent.listen).await.unwrap();
    write_body(
        &mut stream,
        0,
        Body::Handshake(pb::Handshake {
            user: parent.user.clone(),
            system_name: SYSTEM.into(),
            url: URL.into(),
            env: "Live".into(),
            ..Default::default()
        }),
    )
    .await;
    let (ready, _) = read_frame(&mut stream).await.unwrap();
    assert!(matches!(ready.body, Some(Body::Ready(_))));
    stream
}

fn place(tag: &str) -> Body {
    Body::PlaceOrder(pb::PlaceOrderRequest {
        symbol: NQ.into(),
        exchange: CME.into(),
        side: "BUY".into(),
        price_type: "LIMIT".into(),
        quantity: 1,
        user_tag: tag.into(),
        price: Some(100.0),
        duration: "DAY".into(),
        ..Default::default()
    })
}

fn fill_history() -> Body {
    Body::LoadFillHistory(pb::LoadFillHistoryRequest {
        start_date: 0,
        finish_date: 0,
    })
}

/// Read replies until every id in `ids` has arrived; returns (id, arrival offset).
async fn replies(stream: &mut UnixStream, ids: &[u64], started: Instant) -> Vec<(u64, Duration)> {
    let mut out = Vec::new();
    while out.len() < ids.len() {
        let (frame, _) = tokio::time::timeout(Duration::from_secs(30), read_frame(stream))
            .await
            .expect("reply in time")
            .expect("frame");
        if ids.contains(&frame.request_id) {
            out.push((frame.request_id, started.elapsed()));
        }
    }
    out
}

#[tokio::test]
async fn a_place_overtakes_a_slow_read_on_the_same_connection() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    // The read is slow by a wide margin, so a loaded CI box cannot blur which
    // reply came first: ordering is the property, the bounds are generous.
    parent.command("delay load_fill_history 3000").await;
    let mut c = connect(&parent).await;
    let started = Instant::now();
    write_body(&mut c, 10, fill_history()).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    write_body(&mut c, 11, place("overtake")).await;

    let got = replies(&mut c, &[10, 11], started).await;
    assert_eq!(got[0].0, 11, "the place replies first: {got:?}");
    assert!(
        got[0].1 < Duration::from_millis(2000),
        "place waited for the slow read: {got:?}"
    );
    assert!(got[1].1 >= Duration::from_millis(2900), "{got:?}");
}

#[tokio::test]
async fn commands_keep_their_arrival_order() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    let mut c = connect(&parent).await;
    for i in 0..20u64 {
        write_body(&mut c, 100 + i, place(&format!("t{i}"))).await;
    }
    let ids: Vec<u64> = (100..120).collect();
    let got = replies(&mut c, &ids, Instant::now()).await;
    let order: Vec<u64> = got.iter().map(|(id, _)| *id).collect();
    assert_eq!(order, ids, "replies follow request order");
    let calls = parent.calls().await;
    let tags: Vec<&String> = calls
        .iter()
        .filter(|c| c.starts_with("place_order"))
        .collect();
    let want: Vec<String> = (0..20).map(|i| format!("place_order t{i}")).collect();
    assert_eq!(tags, want.iter().collect::<Vec<_>>());
}

#[tokio::test]
async fn slow_reads_run_concurrently_but_only_a_few_at_a_time() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    parent.command("delay load_fill_history 1000").await;
    let mut c = connect(&parent).await;
    let started = Instant::now();
    // Eight slow reads at concurrency four finish in two waves, not eight.
    let ids: Vec<u64> = (200..208).collect();
    for id in &ids {
        write_body(&mut c, *id, fill_history()).await;
    }
    let got = replies(&mut c, &ids, started).await;
    let last = got.last().unwrap().1;
    assert!(
        last >= Duration::from_millis(1900),
        "concurrency is bounded: {got:?}"
    );
    // One after another would take 8s; two waves take 2s.
    assert!(
        last < Duration::from_millis(6000),
        "reads ran concurrently, not one after another: {got:?}"
    );
}

#[tokio::test]
async fn a_frame_split_across_writes_while_events_flow_is_decoded_intact() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    let mut c = connect(&parent).await;
    write_body(
        &mut c,
        1,
        Body::Subscribe(pb::SubscribeRequest {
            symbol: NQ.into(),
            exchange: CME.into(),
        }),
    )
    .await;
    let (ack, _) = read_frame(&mut c).await.unwrap();
    assert!(matches!(ack.body, Some(Body::Ack(_))));

    // Encode a place request and send it a byte at a time while ticks stream out
    // of the same connection.
    use prost::Message;
    let frame = pb::Frame {
        request_id: 77,
        body: Some(place("split")),
    };
    let payload = frame.encode_to_vec();
    let mut wire = (payload.len() as u32).to_be_bytes().to_vec();
    wire.extend_from_slice(&payload);
    // Ticks stream out of this connection (pipelined through the control socket)
    // while the request arrives in 3-byte pieces.
    let send_split = async {
        for chunk in wire.chunks(3) {
            c.write_all(chunk).await.unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    tokio::join!(parent.push_last_trades(NQ, 2000), send_split);
    let got = replies(&mut c, &[77], Instant::now()).await;
    assert_eq!(got.len(), 1);
    assert_eq!(parent.count("place_order split").await, 1);
}

#[test]
fn every_table_row_lane_agrees_with_the_classifier() {
    for row in RPC_TABLE {
        if let Some(make) = row.make_body {
            assert_eq!(
                lane_of(&make()),
                row.lane,
                "{:?}: table lane and lane_of disagree",
                row.body
            );
        }
    }
    let slow: Vec<_> = RPC_TABLE
        .iter()
        .filter(|r| r.lane == Lane::Slow)
        .filter_map(|r| r.body)
        .collect();
    assert_eq!(
        slow,
        [
            // Reads that wait on the plant session.
            "get_front_month",
            "get_reference_data",
            "resolved_account",
            "load_ticks",
            "load_time_bars",
            "probe_time_bars",
            "load_orders",
            "load_product_rms_info",
            "load_account_rms_info",
            "load_fill_history",
            "load_order_history_dates",
            "load_order_history",
        ]
    );
    // Subscription intents mutate the connection's context, so they must stay on
    // the ordered command lane.
    for row in RPC_TABLE {
        if matches!(
            row.kind,
            rithmic_gateway::rpc_table::Kind::SubscriptionIntent
                | rithmic_gateway::rpc_table::Kind::IntentRelease
        ) {
            assert_eq!(row.lane, Lane::Command, "{:?}", row.body);
        }
    }
}

#[tokio::test]
async fn a_place_is_not_held_by_a_time_bar_load_that_holds_the_session_gate() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    parent.command("delay load_time_bars 3000").await;
    let mut c = connect(&parent).await;
    let started = Instant::now();
    write_body(
        &mut c,
        10,
        Body::LoadTimeBars(pb::LoadTimeBarsRequest {
            symbol: NQ.into(),
            exchange: CME.into(),
            bar_type: 2,
            period: 1,
            start_time_sec: 0,
            end_time_sec: 3600,
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    write_body(&mut c, 11, place("during-history")).await;
    let got = replies(&mut c, &[10, 11], started).await;
    assert_eq!(got[0].0, 11, "the place replies first: {got:?}");
    assert!(got[0].1 < Duration::from_millis(2000), "{got:?}");
}

#[tokio::test]
async fn the_session_gate_is_shared_for_rpcs_and_exclusive_for_a_rebuild() {
    use rithmic_gateway::server::SessionHandle;
    use rithmic_plants::fake::FakeSession;
    use std::sync::Arc;

    let handle = SessionHandle::new(Arc::new(FakeSession::new()));
    // Any number of RPCs hold the gate together.
    let first = handle.lock().await;
    let second = tokio::time::timeout(Duration::from_millis(100), handle.lock())
        .await
        .expect("a second shared holder does not wait");

    // A rebuild waits for them, and holds new RPCs out while it runs.
    let rebuild = {
        let handle = handle.clone();
        tokio::spawn(async move {
            let _exclusive = handle.lock_exclusive().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(!rebuild.is_finished(), "rebuild waits for in-flight RPCs");
    drop((first, second));
    rebuild.await.unwrap();
    // After the rebuild the gate is shared again.
    let _again = tokio::time::timeout(Duration::from_millis(100), handle.lock())
        .await
        .expect("shared access resumes");
}

#[tokio::test]
async fn a_subscribe_and_a_place_are_not_held_by_a_history_load() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    parent.command("delay load_ticks 3000").await;
    let mut c = connect(&parent).await;
    let started = Instant::now();
    write_body(
        &mut c,
        10,
        Body::LoadTicks(pb::LoadTicksRequest {
            symbol: NQ.into(),
            exchange: CME.into(),
            start_time_sec: 0,
            end_time_sec: 60,
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    // The load started with no live market data, so it runs; a subscribe that
    // arrives mid-load goes ahead, and the place queued behind it is not held.
    write_body(
        &mut c,
        11,
        Body::Subscribe(pb::SubscribeRequest {
            symbol: NQ.into(),
            exchange: CME.into(),
        }),
    )
    .await;
    write_body(&mut c, 12, place("behind-subscribe")).await;

    let got = replies(&mut c, &[10, 11, 12], started).await;
    let order: Vec<u64> = got.iter().map(|(id, _)| *id).collect();
    assert_eq!(order, vec![11, 12, 10], "{got:?}");
    assert!(got[1].1 < Duration::from_millis(2000), "{got:?}");
}

#[tokio::test]
async fn an_undecodable_request_gets_a_typed_reply_and_the_connection_lives() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    let mut c = connect(&parent).await;
    // request_id = 7, then a field with an invalid wire type: the frame cannot
    // be decoded, but its id can.
    let payload: &[u8] = &[0x08, 0x07, 0x17, 0x01];
    c.write_all(&(payload.len() as u32).to_be_bytes())
        .await
        .unwrap();
    c.write_all(payload).await.unwrap();
    let (reply, _) = read_frame(&mut c)
        .await
        .expect("a reply, not a closed socket");
    assert_eq!(reply.request_id, 7);
    match reply.body {
        Some(Body::Error(e)) => assert_eq!(e.code, "bad_request"),
        other => panic!("{other:?}"),
    }
    // The connection keeps serving.
    write_body(&mut c, 8, place("after-bad-frame")).await;
    let got = replies(&mut c, &[8], Instant::now()).await;
    assert_eq!(got[0].0, 8);
}

#[tokio::test]
async fn a_request_with_no_body_is_answered_not_dropped() {
    let parent = FakeParent::start(Opts::default()).await;
    let mut c = connect(&parent).await;
    use prost::Message;
    let frame = pb::Frame {
        request_id: 9,
        body: None,
    };
    let payload = frame.encode_to_vec();
    c.write_all(&(payload.len() as u32).to_be_bytes())
        .await
        .unwrap();
    c.write_all(&payload).await.unwrap();
    let (reply, _) = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut c))
        .await
        .expect("answered in time")
        .unwrap();
    assert_eq!(reply.request_id, 9);
    assert!(matches!(reply.body, Some(Body::Error(e)) if e.code == "bad_request"));
}

#[tokio::test]
async fn a_place_is_not_held_by_a_slow_reference_data_lookup() {
    let parent = FakeParent::start(Opts {
        trading: true,
        ..Opts::default()
    })
    .await;
    parent.command("delay get_reference_data 3000").await;
    let mut c = connect(&parent).await;
    let started = Instant::now();
    write_body(
        &mut c,
        20,
        Body::GetReferenceData(pb::GetReferenceDataRequest {
            symbol: NQ.into(),
            exchange: CME.into(),
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    write_body(&mut c, 21, place("during-lookup")).await;
    let got = replies(&mut c, &[20, 21], started).await;
    assert_eq!(got[0].0, 21, "the place replies first: {got:?}");
    assert!(got[0].1 < Duration::from_millis(2000), "{got:?}");
}

#[tokio::test]
async fn a_peer_that_never_handshakes_is_closed() {
    let parent = FakeParent::start(Opts::default()).await;
    let mut silent = UnixStream::connect(&parent.listen).await.unwrap();
    let mut buf = [0u8; 1];
    let read = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::io::AsyncReadExt::read(&mut silent, &mut buf),
    )
    .await
    .expect("the parent closes a silent peer");
    assert_eq!(read.unwrap_or(0), 0, "closed, not answered");
}

#[tokio::test]
async fn an_oversized_frame_before_the_handshake_is_refused() {
    let parent = FakeParent::start(Opts::default()).await;
    let mut peer = UnixStream::connect(&parent.listen).await.unwrap();
    // Claim a 1 MiB handshake: refused without allocating or waiting for it.
    peer.write_all(&(1u32 << 20).to_be_bytes()).await.unwrap();
    let mut buf = [0u8; 1];
    let read = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::io::AsyncReadExt::read(&mut peer, &mut buf),
    )
    .await
    .expect("refused promptly");
    assert_eq!(read.unwrap_or(0), 0);
}
