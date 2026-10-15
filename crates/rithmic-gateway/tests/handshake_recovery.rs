//! Plant recovery while the initial Ready write is suspended.
mod common;

use common::*;
use rithmic_gateway::pb::{self, frame::Body, Frame};
use rithmic_host::Stream;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

/// Inject a plant failure at the first Ready write, after attachment. The
/// paused clock advances only once the pump's runnable lifecycle work drains.
struct RecoverDuringReady {
    stream: DuplexStream,
    venue: Arc<rithmic_host::fake::FakeVenue>,
    delay: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl AsyncRead for RecoverDuringReady {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl AsyncWrite for RecoverDuringReady {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.delay.is_none() {
            self.venue.fail_stream(Stream::Ticker, "during Ready");
            self.delay = Some(Box::pin(tokio::time::sleep(Duration::from_secs(1))));
        }
        if self.delay.as_mut().unwrap().as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[tokio::test(start_paused = true)]
async fn recovery_between_attach_and_ready_keeps_the_connection_alive() {
    let parent = FakeParent::start(Opts::default()).await;
    let (mut client, server) = tokio::io::duplex(64 * 1024);
    let state = parent.state.clone();
    let server = RecoverDuringReady {
        stream: server,
        venue: parent.fake.clone(),
        delay: None,
    };
    let connection = tokio::spawn(rithmic_gateway::server::serve_connection(server, state));
    client
        .write_all(
            &rithmic_gateway::codec::encode(&Frame {
                request_id: 0,
                body: Some(Body::Handshake(pb::Handshake {
                    user: parent.user.clone(),
                    auth_token: AUTH_TOKEN.into(),
                    system_name: SYSTEM.into(),
                    url: URL.into(),
                    env: "Live".into(),
                    ..Default::default()
                })),
            })
            .unwrap(),
        )
        .await
        .unwrap();
    let (ready, _) = read_frame(&mut client).await.unwrap();
    assert!(matches!(ready.body, Some(Body::Ready(_))));
    for interrupted in [true, false] {
        let frame = tokio::time::timeout(Duration::from_secs(4), read_frame(&mut client))
            .await
            .unwrap();
        let Ok((frame, _)) = frame else {
            panic!(
                "connection closed during handshake recovery: {:?}",
                connection.await
            );
        };
        let Some(Body::Event(event)) = frame.body else {
            panic!("{frame:?}")
        };
        if interrupted {
            assert!(matches!(
                event.body,
                Some(pb::event::Body::TransportInterrupted(_))
            ));
        } else {
            assert!(matches!(
                event.body,
                Some(pb::event::Body::TransportResumed(_))
            ));
        }
    }
    drop(client);
    assert!(connection.await.unwrap().is_ok());
}
