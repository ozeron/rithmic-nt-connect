//! Recovery contract exercised through scripted parents over duplex streams.
use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use rithmic_host::events::SessionEvent;
use rithmic_host::{Error, Session};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::{mpsc, oneshot};

use super::{GatewayConfig, RemoteSession, TransportEvent};
use crate::pb::{self, frame::Body, Frame};

const WAIT: Duration = Duration::from_secs(2);
type Dial = oneshot::Sender<rithmic_host::Result<DuplexStream>>;

struct Rig {
    session: Arc<RemoteSession>,
    dials: mpsc::UnboundedReceiver<Dial>,
}

impl Rig {
    fn new() -> Self {
        let (tx, dials) = mpsc::unbounded_channel::<Dial>();
        let mut cfg = GatewayConfig::new("recovery", "test", "wss://fake", "Test");
        cfg.rpc_timeout = Duration::from_millis(200);
        cfg.attach_timeout = Duration::from_millis(300);
        cfg.recovery_attempts = 2;
        cfg.recovery_initial_backoff = Duration::from_millis(1);
        cfg.recovery_max_backoff = Duration::from_millis(1);
        let session = Arc::new(RemoteSession::with_connector(cfg, move || {
            let (reply, result) = oneshot::channel();
            let _ = tx.send(reply);
            async move {
                result
                    .await
                    .unwrap_or_else(|_| Err(Error::Transport("dial cancelled".into())))
            }
        }));
        Self { session, dials }
    }

    async fn parent(&mut self) -> Parent {
        let dial = tokio::time::timeout(WAIT, self.dials.recv())
            .await
            .unwrap()
            .unwrap();
        let (client, stream) = tokio::io::duplex(64 * 1024);
        dial.send(Ok(client)).unwrap();
        let mut parent = Parent(stream);
        assert!(matches!(parent.read().await.body, Some(Body::Handshake(_))));
        parent
            .write(Frame {
                request_id: 0,
                body: Some(Body::Ready(pb::Ready::default())),
            })
            .await;
        parent
    }

    async fn connected(&mut self) -> Parent {
        let session = self.session.clone();
        let connect = tokio::spawn(async move { session.connect().await });
        let parent = self.parent().await;
        connect.await.unwrap().unwrap();
        parent
    }
}

struct Parent(DuplexStream);
impl Parent {
    async fn read(&mut self) -> Frame {
        tokio::time::timeout(WAIT, async {
            let n = self.0.read_u32().await.unwrap();
            let mut bytes = vec![0; n as usize];
            self.0.read_exact(&mut bytes).await.unwrap();
            Frame::decode(bytes.as_slice()).unwrap()
        })
        .await
        .unwrap()
    }

    async fn write(&mut self, frame: Frame) {
        self.0
            .write_all(&crate::codec::encode(&frame).unwrap())
            .await
            .unwrap();
    }

    async fn ack(&mut self, request_id: u64) {
        self.write(Frame {
            request_id,
            body: Some(Body::Ack(pb::Ack {})),
        })
        .await;
    }

    async fn call(
        &mut self,
        call: impl std::future::Future<Output = rithmic_host::Result<()>>,
    ) -> Body {
        let (result, request) = tokio::join!(call, async {
            let frame = self.read().await;
            self.ack(frame.request_id).await;
            frame.body.unwrap()
        });
        result.unwrap();
        request
    }

    async fn event(&mut self, body: pb::event::Body) {
        self.write(Frame {
            request_id: 0,
            body: Some(Body::Event(pb::Event { body: Some(body) })),
        })
        .await;
    }
}

async fn session_event(rx: &mut tokio::sync::broadcast::Receiver<SessionEvent>) -> SessionEvent {
    tokio::time::timeout(WAIT, rx.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn parent_frames_report_the_remote_generation_and_legacy_events() {
    let mut rig = Rig::new();
    let mut events = rig.session.session_events();
    let mut legacy = rig.session.subscribe_transport();
    let mut parent = rig.connected().await;
    assert!(
        events.try_recv().is_err(),
        "initial connect is not recovery"
    );
    let generation = rig.session.transport_generation();
    parent
        .event(pb::event::Body::TransportInterrupted(
            pb::TransportInterrupted { generation: 700 },
        ))
        .await;
    assert_eq!(
        session_event(&mut events).await,
        SessionEvent::Interrupted {
            generation,
            cause: Some("parent plant interrupted".into()),
        }
    );
    assert!(
        legacy.try_recv().is_err(),
        "legacy channel retains its old mapping"
    );
    parent
        .event(pb::event::Body::TransportResumed(pb::TransportResumed {
            generation: 701,
        }))
        .await;
    assert_eq!(
        session_event(&mut events).await,
        SessionEvent::Resumed {
            generation: generation + 1
        }
    );
    assert_eq!(
        legacy.recv().await.unwrap(),
        TransportEvent::Down { generation }
    );
    assert_eq!(
        legacy.recv().await.unwrap(),
        TransportEvent::Up {
            generation: generation + 1
        }
    );
    assert!(legacy.try_recv().is_err(), "only the legacy Down/Up pair");
}

#[tokio::test]
async fn socket_recovery_rejoins_all_live_intents_before_resumed() {
    let mut rig = Rig::new();
    let mut events = rig.session.session_events();
    let mut parent = rig.connected().await;
    let session = rig.session.clone();
    let other = session.as_ref().clone();
    other.connect().await.unwrap();
    let mut expected = vec![
        parent.call(session.subscribe("NQU6", "CME")).await,
        parent
            .call(session.subscribe_order_book_summary("NQU6", "CME"))
            .await,
        parent
            .call(session.subscribe_time_bars("NQU6", "CME", 2, 1))
            .await,
        parent.call(session.subscribe_pnl()).await,
        parent.call(session.subscribe_order_updates()).await,
        parent.call(session.subscribe_bracket_updates()).await,
    ];
    // Two holders still mean one socket intent. A departing facade's private
    // ticker must not be restored, while its shared ticker remains live.
    parent.call(other.subscribe("NQU6", "CME")).await;
    parent.call(other.subscribe("ESU6", "CME")).await;
    parent.call(other.disconnect()).await;
    drop(parent);
    assert_eq!(
        session_event(&mut events).await,
        SessionEvent::Interrupted {
            generation: 1,
            cause: Some("socket lost".into()),
        }
    );
    // The connector is deliberately pending; polling must time out empty.
    for stream in [
        rithmic_host::Stream::Ticker,
        rithmic_host::Stream::History,
        rithmic_host::Stream::Pnl,
        rithmic_host::Stream::Order,
    ] {
        assert!(session
            .next_events(stream, 10, Duration::from_millis(2))
            .await
            .unwrap()
            .is_empty());
    }
    let mut waiters = Vec::new();
    for _ in 0..8 {
        let session = session.clone();
        waiters.push(tokio::spawn(async move {
            session.ensure_transport_live().await
        }));
    }
    let mut parent = rig.parent().await;
    let last = expected.pop().unwrap();
    for body in expected {
        let request = parent.read().await;
        assert_eq!(request.body, Some(body));
        assert!(
            events.try_recv().is_err(),
            "no Resumed before all join Acks"
        );
        parent.ack(request.request_id).await;
    }
    let request = parent.read().await;
    assert_eq!(request.body, Some(last));
    assert!(events.try_recv().is_err());
    parent.ack(request.request_id).await;
    assert_eq!(
        session_event(&mut events).await,
        SessionEvent::Resumed { generation: 2 }
    );
    for waiter in waiters {
        assert_eq!(waiter.await.unwrap().unwrap(), 2);
    }
    assert!(rig.dials.try_recv().is_err(), "one dial for every waiter");
    // Recovery restores delivery without the consumer subscribing again.
    parent
        .event(pb::event::Body::LastTrade(pb::LastTrade {
            symbol: Some("NQU6".into()),
            ..Default::default()
        }))
        .await;
    assert_eq!(
        session
            .next_events(rithmic_host::Stream::Ticker, 1, WAIT)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn a_written_command_stays_unknown_and_is_not_replayed() {
    let mut rig = Rig::new();
    let mut events = rig.session.session_events();
    let mut parent = rig.connected().await;
    let session = rig.session.clone();
    let command = tokio::spawn(async move { session.cancel_order("basket-1").await });
    assert!(matches!(
        parent.read().await.body,
        Some(Body::CancelOrder(_))
    ));
    drop(parent);
    assert!(matches!(
        command.await.unwrap(),
        Err(Error::OutcomeUnknown(_))
    ));
    assert!(matches!(
        session_event(&mut events).await,
        SessionEvent::Interrupted { generation: 1, .. }
    ));
    let mut parent = rig.parent().await;
    assert_eq!(
        session_event(&mut events).await,
        SessionEvent::Resumed { generation: 2 }
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), parent.read())
            .await
            .is_err(),
        "command was never resent"
    );
}

#[tokio::test]
async fn failed_recovery_is_terminal_for_pending_and_subsequent_rpcs() {
    let mut rig = Rig::new();
    let mut events = rig.session.session_events();
    let mut legacy = rig.session.subscribe_recovery_failed();
    let parent = rig.connected().await;
    drop(parent);
    assert!(matches!(
        session_event(&mut events).await,
        SessionEvent::Interrupted { .. }
    ));
    let session = rig.session.clone();
    let pending = tokio::spawn(async move { session.get_front_month("NQ", "CME").await });
    for _ in 0..2 {
        let dial = tokio::time::timeout(WAIT, rig.dials.recv())
            .await
            .unwrap()
            .unwrap();
        dial.send(Err(Error::Transport("scripted dial failure".into())))
            .unwrap();
    }
    assert_eq!(
        session_event(&mut events).await,
        SessionEvent::RecoveryFailed {
            generation: 1,
            attempts: 2,
            cause: Some("transport error: scripted dial failure".into()),
        }
    );
    let failed = legacy.recv().await.unwrap();
    assert_eq!(failed.attempts, 2);
    assert!(matches!(pending.await.unwrap(), Err(Error::Transport(_))));
    assert!(matches!(
        rig.session.get_front_month("NQ", "CME").await,
        Err(Error::Transport(_))
    ));
    assert!(matches!(
        rig.session
            .next_events(rithmic_host::Stream::Ticker, 1, WAIT)
            .await,
        Err(Error::ChannelClosed { .. })
    ));
    assert!(
        rig.dials.try_recv().is_err(),
        "failure must not start another flight"
    );
}

#[tokio::test]
async fn a_stalled_connector_exhausts_a_bounded_recovery_window() {
    let mut rig = Rig::new();
    let mut events = rig.session.session_events();
    let parent = rig.connected().await;
    drop(parent);
    assert!(matches!(
        session_event(&mut events).await,
        SessionEvent::Interrupted { .. }
    ));
    // Keep all connector replies pending instead of returning a failure.
    let _pending_dial = tokio::time::timeout(WAIT, rig.dials.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        session_event(&mut events).await,
        SessionEvent::RecoveryFailed { attempts: 1, .. }
    ));
    assert!(rig.dials.try_recv().is_err());
}

#[tokio::test]
async fn disconnect_drains_queued_events_then_closes_the_stream() {
    let mut rig = Rig::new();
    let mut parent = rig.connected().await;
    rig.session
        .next_events(rithmic_host::Stream::Ticker, 1, Duration::ZERO)
        .await
        .unwrap();
    parent
        .event(pb::event::Body::LastTrade(pb::LastTrade::default()))
        .await;
    // Reader processes frames in order; the interruption is our delivery barrier.
    let mut events = rig.session.session_events();
    parent
        .event(pb::event::Body::TransportInterrupted(
            pb::TransportInterrupted::default(),
        ))
        .await;
    session_event(&mut events).await;
    rig.session.disconnect().await.unwrap();
    assert_eq!(
        rig.session
            .next_events(rithmic_host::Stream::Ticker, 1, WAIT)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        rig.session
            .next_events(rithmic_host::Stream::Ticker, 1, WAIT)
            .await,
        Err(Error::ChannelClosed { .. })
    ));
}

#[tokio::test]
async fn a_failed_intent_rejoin_never_publishes_resumed() {
    let mut rig = Rig::new();
    let mut events = rig.session.session_events();
    let mut parent = rig.connected().await;
    parent.call(rig.session.subscribe("NQU6", "CME")).await;
    drop(parent);
    assert!(matches!(
        session_event(&mut events).await,
        SessionEvent::Interrupted { .. }
    ));
    let mut parents = Vec::new();
    for _ in 0..2 {
        let mut parent = rig.parent().await;
        let request = parent.read().await;
        assert!(matches!(request.body, Some(Body::Subscribe(_))));
        parent
            .write(Frame {
                request_id: request.request_id,
                body: Some(Body::Error(pb::ErrorResponse {
                    code: "join_failed".into(),
                    message: "scripted rejection".into(),
                })),
            })
            .await;
        parents.push(parent);
    }
    assert!(matches!(
        session_event(&mut events).await,
        SessionEvent::RecoveryFailed {
            generation: 1,
            attempts: 2,
            ..
        }
    ));
    assert_eq!(rig.session.transport_generation(), 1);
    assert!(!rig.session.transport_connected());
}

#[tokio::test]
async fn an_old_parent_keeps_the_legacy_synthetic_down_before_up() {
    let mut rig = Rig::new();
    let mut events = rig.session.session_events();
    let mut legacy = rig.session.subscribe_transport();
    let mut parent = rig.connected().await;
    parent
        .event(pb::event::Body::TransportResumed(pb::TransportResumed {
            generation: 999,
        }))
        .await;
    assert_eq!(
        session_event(&mut events).await,
        SessionEvent::Resumed { generation: 2 }
    );
    assert_eq!(
        legacy.recv().await.unwrap(),
        TransportEvent::Down { generation: 1 }
    );
    assert_eq!(
        legacy.recv().await.unwrap(),
        TransportEvent::Up { generation: 2 }
    );
}

#[tokio::test]
async fn disconnect_during_redial_closes_pollers_without_resuming() {
    let mut rig = Rig::new();
    let mut events = rig.session.session_events();
    let parent = rig.connected().await;
    drop(parent);
    assert!(matches!(
        session_event(&mut events).await,
        SessionEvent::Interrupted { .. }
    ));
    let _pending_dial = tokio::time::timeout(WAIT, rig.dials.recv())
        .await
        .unwrap()
        .unwrap();
    let session = rig.session.clone();
    let poller = tokio::spawn(async move {
        session
            .next_events(rithmic_host::Stream::Ticker, 1, WAIT)
            .await
    });
    tokio::time::timeout(WAIT, rig.session.disconnect())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        poller.await.unwrap(),
        Err(Error::ChannelClosed { .. })
    ));
    assert!(
        events.try_recv().is_err(),
        "deliberate close never resumes or fails recovery"
    );
    assert!(rig.dials.try_recv().is_err());
}

#[tokio::test]
async fn a_departing_facades_pending_subscribe_cannot_become_a_rejoin_intent() {
    let mut rig = Rig::new();
    let mut events = rig.session.session_events();
    let mut parent = rig.connected().await;
    let departing = Arc::new(rig.session.as_ref().clone());
    departing.connect().await.unwrap();
    let subscriber = departing.clone();
    let subscribe = tokio::spawn(async move { subscriber.subscribe("ESU6", "CME").await });
    let request = parent.read().await;
    assert!(matches!(request.body, Some(Body::Subscribe(_))));
    departing.disconnect().await.unwrap();
    parent.ack(request.request_id).await;
    // A late Ack must be balanced by a release, not recorded for facade zero.
    let release = parent.read().await;
    assert!(matches!(release.body, Some(Body::Unsubscribe(_))));
    parent.ack(release.request_id).await;
    assert!(matches!(
        subscribe.await.unwrap(),
        Err(Error::NotConnected { .. })
    ));
    drop(parent);
    assert!(matches!(
        session_event(&mut events).await,
        SessionEvent::Interrupted { .. }
    ));
    let _parent = rig.parent().await;
    assert_eq!(
        session_event(&mut events).await,
        SessionEvent::Resumed { generation: 2 }
    );
}
