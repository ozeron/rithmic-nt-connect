//! Compatibility only: the parent does not emit TransportInterrupted yet.

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use prost::Message;
use rithmic_gateway::framing::{decode_frame, encode_frame};
use rithmic_gateway::pb::{self, frame::Body};
use rithmic_host::{dto::PlantEvent, Session};

fn interrupted() -> pb::Event {
    pb::Event {
        body: Some(pb::event::Body::TransportInterrupted(
            pb::TransportInterrupted { generation: 1 },
        )),
    }
}

#[tokio::test]
async fn interruption_is_skipped_and_subscriptions_keep_serving() {
    let mock = MockParent::duplex(mock_ready(false, false), 0, |_, _| Action::Ack);
    let session = mock.session(duplex_config());
    session.connect().await.unwrap();
    let mut transport = session.subscribe_transport();
    session.subscribe(NQ, CME).await.unwrap();
    mock.push_last_trade(NQ, 1.0);
    assert!(matches!(
        session.poll_event(Duration::from_secs(2)).await.unwrap(),
        Some(PlantEvent::LastTrade(t)) if t.trade_price == Some(1.0)
    ));

    mock.events.send(Body::Event(interrupted())).unwrap();
    mock.push_last_trade(NQ, 2.0);
    assert!(matches!(
        session.poll_event(Duration::from_secs(2)).await.unwrap(),
        Some(PlantEvent::LastTrade(t)) if t.trade_price == Some(2.0)
    ));
    // The additive event must not trigger redial, replay, or adapter recovery.
    assert!(session.transport_connected());
    assert_eq!(session.transport_generation(), 1);
    assert!(matches!(
        transport.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    assert_eq!(mock.connections.load(Ordering::SeqCst), 1);
    assert_eq!(
        mock.request_bodies()
            .iter()
            .filter(|b| matches!(b, Body::Subscribe(_)))
            .count(),
        1
    );
    session.subscribe("ESU6", CME).await.unwrap();
    mock.push_last_trade("ESU6", 3.0);
    assert!(matches!(
        session.poll_event(Duration::from_secs(2)).await.unwrap(),
        Some(PlantEvent::LastTrade(t)) if t.symbol.as_deref() == Some("ESU6")
    ));
    session.disconnect().await.unwrap();
}

// A pre-field-12 reader's projection of Frame/Event. Existing message types
// are unchanged; only these known variants are needed to prove continued reads.
#[derive(Clone, PartialEq, prost::Message)]
struct OldFrame {
    #[prost(uint64, tag = "1")]
    request_id: u64,
    #[prost(message, optional, tag = "90")]
    event: Option<OldEvent>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct OldEvent {
    #[prost(oneof = "OldBody", tags = "1, 10")]
    body: Option<OldBody>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum OldBody {
    #[prost(message, tag = "1")]
    LastTrade(pb::LastTrade),
    #[prost(message, tag = "10")]
    TransportResumed(pb::TransportResumed),
}

#[test]
fn old_prost_reader_skips_field_12_and_decodes_the_next_frame() {
    assert_eq!(interrupted().encode_to_vec(), [0x62, 2, 8, 1]);
    let frames = [
        pb::Frame {
            request_id: 0,
            body: Some(Body::Event(interrupted())),
        },
        pb::Frame {
            request_id: 0,
            body: Some(Body::Event(pb::Event {
                body: Some(pb::event::Body::LastTrade(pb::LastTrade {
                    symbol: Some(NQ.into()),
                    ..Default::default()
                })),
            })),
        },
    ];
    let wire: Vec<u8> = frames
        .iter()
        .flat_map(|frame| encode_frame(&frame.encode_to_vec()).unwrap())
        .collect();
    let (payload, consumed) = decode_frame(&wire).unwrap();
    let decoded = OldFrame::decode(payload).unwrap();
    let event = decoded.event.unwrap();
    assert!(event.body.is_none(), "unknown oneof leaves the body unset");
    assert!(
        event.encode_to_vec().is_empty(),
        "prost drops unknown fields"
    );
    let (payload, _) = decode_frame(&wire[consumed..]).unwrap();
    let decoded = OldFrame::decode(payload).unwrap();
    assert!(matches!(
        decoded.event.unwrap().body,
        Some(OldBody::LastTrade(t)) if t.symbol.as_deref() == Some(NQ)
    ));
}
