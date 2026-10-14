//! A client receives only the kinds of frame it asked for on a symbol: a
//! ticker-only client is not sent time bars (or book updates) for that symbol, and
//! a client that asked for both gets both.

mod common;

use std::time::Duration;

use common::*;
use rithmic_gateway::pb::{self, frame::Body};
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

async fn ack(stream: &mut UnixStream, id: u64, body: Body) {
    write_body(stream, id, body).await;
    loop {
        let (frame, _) = read_frame(stream).await.unwrap();
        if frame.request_id == id {
            assert!(matches!(frame.body, Some(Body::Ack(_))), "{frame:?}");
            return;
        }
    }
}

/// Event kinds received within `window`, in arrival order.
async fn kinds_within(stream: &mut UnixStream, window: Duration) -> Vec<&'static str> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + window;
    while let Ok(Ok((frame, _))) = tokio::time::timeout_at(deadline, read_frame(stream)).await {
        if let Some(Body::Event(ev)) = frame.body {
            out.push(match ev.body {
                Some(pb::event::Body::LastTrade(_)) => "last_trade",
                Some(pb::event::Body::Bbo(_)) => "bbo",
                Some(pb::event::Body::TimeBar(_)) => "time_bar",
                _ => "other",
            });
        }
    }
    out
}

fn subscribe() -> Body {
    Body::Subscribe(pb::SubscribeRequest {
        symbol: NQ.into(),
        exchange: CME.into(),
    })
}

fn subscribe_bars() -> Body {
    Body::SubscribeTimeBars(pb::SubscribeTimeBarsRequest {
        symbol: NQ.into(),
        exchange: CME.into(),
        bar_type: 2,
        period: 1,
    })
}

#[tokio::test]
async fn a_ticker_only_client_is_not_sent_time_bars_for_the_symbol() {
    let parent = FakeParent::start(Opts::default()).await;
    let mut ticker_only = connect(&parent).await;
    ack(&mut ticker_only, 1, subscribe()).await;
    let mut both = connect(&parent).await;
    ack(&mut both, 1, subscribe()).await;
    ack(&mut both, 2, subscribe_bars()).await;

    parent.command("push last_trade NQH6 CME 100.25 1").await;
    parent.command("push time_bar NQH6 CME 101.5").await;
    parent.command("push bbo NQH6 CME 100.0 5 100.25 7").await;

    let only = kinds_within(&mut ticker_only, Duration::from_millis(400)).await;
    assert_eq!(
        only,
        ["last_trade", "bbo"],
        "no time bar for a ticker-only client"
    );
    let all = kinds_within(&mut both, Duration::from_millis(400)).await;
    assert_eq!(all, ["last_trade", "time_bar", "bbo"]);
}

#[tokio::test]
async fn leaving_a_kind_stops_its_frames_but_keeps_the_others() {
    let parent = FakeParent::start(Opts::default()).await;
    let mut c = connect(&parent).await;
    ack(&mut c, 1, subscribe()).await;
    ack(&mut c, 2, subscribe_bars()).await;
    ack(
        &mut c,
        3,
        Body::UnsubscribeTimeBars(pb::UnsubscribeTimeBarsRequest {
            symbol: NQ.into(),
            exchange: CME.into(),
            bar_type: 2,
            period: 1,
        }),
    )
    .await;
    parent.command("push time_bar NQH6 CME 101.5").await;
    parent.command("push last_trade NQH6 CME 100.25 1").await;
    assert_eq!(
        kinds_within(&mut c, Duration::from_millis(400)).await,
        ["last_trade"]
    );
}
