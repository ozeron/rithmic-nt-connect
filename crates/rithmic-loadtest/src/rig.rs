//! The system under test: the real gateway server over a fake plant, embedded
//! in this process and served on a real unix socket, plus two client kinds.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use rithmic_gateway::client::{GatewayConfig, GatewaySession};
use rithmic_gateway::idle_exit::IdleExitPolicy;
use rithmic_gateway::listen::bind_unix;
use rithmic_gateway::pb::{frame::Body, Frame, Handshake, SubscribeRequest};
use rithmic_gateway::server::{run_gateway, Fingerprint, GatewayParams, SessionHandle};
use rithmic_gateway::subscriptions::ParentGates;
use rithmic_host::dto::{LastTradeDto, PlantEvent};
use rithmic_host::fake::{FakeVenue, Stream};
use rithmic_host::Session;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::task::JoinHandle;

use crate::metrics::now_ns;

pub const SYSTEM: &str = "LucidTrading";
pub const URL: &str = "wss://fake.example";
pub const SYMBOL: &str = "NQH6";
pub const EXCHANGE: &str = "CME";

static N: AtomicU64 = AtomicU64::new(0);

pub struct Rig {
    pub fake: Arc<FakeVenue>,
    pub listen: PathBuf,
    pub user: String,
    _dir: tempfile::TempDir,
    server: JoinHandle<()>,
}

impl Rig {
    /// Start the production server path (handshake, gates, fan-out, pump,
    /// reconnect) over a connected fake plant.
    pub async fn start(trading: bool) -> Rig {
        let dir = tempfile::Builder::new()
            .prefix("rgl-")
            .tempdir_in("/tmp")
            .expect("tempdir");
        let path = dir.path().join("gw.sock");
        let user = format!(
            "load-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        );
        let listener = bind_unix(&path).await.expect("bind");
        let fake = Arc::new(FakeVenue::new());
        fake.connect().await.expect("fake connect");
        let params = GatewayParams {
            listener,
            path: path.clone(),
            fingerprint: Fingerprint {
                user: user.clone(),
                system_name: SYSTEM.into(),
                url: URL.into(),
                env: "Live".into(),
                account_id: String::new(),
                fcm_id: String::new(),
                ib_id: String::new(),
            },
            expected_auth_token: String::new(),
            idle_policy: IdleExitPolicy::Never,
            gates: ParentGates {
                trading_enabled: trading,
                cancel_all_enabled: false,
            },
            session: SessionHandle::new(fake.clone()),
            timing: rithmic_gateway::server::pump::PumpTiming::default(),
        };
        let server = tokio::spawn(async move {
            let _ = run_gateway(params).await;
        });
        Rig {
            fake,
            listen: path,
            user,
            _dir: dir,
            server,
        }
    }

    pub fn config(&self) -> GatewayConfig {
        let mut cfg = GatewayConfig::new(&self.user, SYSTEM, URL, "Live")
            .with_listen(&format!("unix://{}", self.listen.display()))
            .expect("listen");
        // The in-process server holds no credential flock.
        cfg.attest_flock = false;
        cfg
    }

    /// A real library client (one shared socket per process, per fingerprint).
    pub async fn session(&self) -> GatewaySession {
        let s = GatewaySession::new(self.config());
        s.connect().await.expect("session connect");
        s
    }

    /// Push one tick carrying its sequence number and creation time.
    pub fn push_tick(&self, seq: u64) {
        self.fake.push_event(Stream::Ticker, tick(seq));
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.server.abort();
    }
}

pub fn tick(seq: u64) -> PlantEvent {
    PlantEvent::LastTrade(LastTradeDto {
        symbol: Some(SYMBOL.into()),
        exchange: Some(EXCHANGE.into()),
        trade_price: Some(21_000.0),
        trade_size: Some(seq as i32),
        aggressor: Some(1),
        ssboe: Some(1_700_000_000),
        usecs: Some(0),
        ts_event_ns: Some(now_ns()),
        is_snapshot: Some(false),
    })
}

/// What a raw consumer reads from the stream.
pub enum Item {
    Tick {
        seq: i32,
        created_ns: u64,
    },
    Gap {
        skipped: u64,
    },
    /// The parent rebuilt its plants without dropping this connection.
    Resumed,
}

/// A minimal protocol client: handshake, subscribe, read frames. It lets the
/// harness open many independent connections to the parent (the library client
/// shares one socket per process).
pub struct RawClient {
    stream: UnixStream,
}

impl RawClient {
    pub async fn connect(rig: &Rig) -> std::io::Result<RawClient> {
        let mut stream = UnixStream::connect(&rig.listen).await?;
        write_frame(
            &mut stream,
            &Frame {
                request_id: 0,
                body: Some(Body::Handshake(Handshake {
                    user: rig.user.clone(),
                    system_name: SYSTEM.into(),
                    url: URL.into(),
                    env: "Live".into(),
                    ..Default::default()
                })),
            },
        )
        .await?;
        match read_frame(&mut stream).await? {
            Some(Frame {
                body: Some(Body::Ready(_)),
                ..
            }) => Ok(RawClient { stream }),
            other => Err(std::io::Error::other(format!("handshake: {other:?}"))),
        }
    }

    pub async fn subscribe(&mut self) -> std::io::Result<()> {
        write_frame(
            &mut self.stream,
            &Frame {
                request_id: 1,
                body: Some(Body::Subscribe(SubscribeRequest {
                    symbol: SYMBOL.into(),
                    exchange: EXCHANGE.into(),
                })),
            },
        )
        .await?;
        loop {
            match read_frame(&mut self.stream).await? {
                Some(Frame {
                    body: Some(Body::Ack(_)),
                    ..
                }) => return Ok(()),
                Some(Frame {
                    body: Some(Body::Error(e)),
                    ..
                }) => return Err(std::io::Error::other(format!("subscribe: {}", e.message))),
                Some(_) => {}
                None => return Err(std::io::Error::other("eof during subscribe")),
            }
        }
    }

    /// Send one request frame without waiting for its reply.
    pub async fn send(&mut self, request_id: u64, body: Body) -> std::io::Result<()> {
        write_frame(
            &mut self.stream,
            &Frame {
                request_id,
                body: Some(body),
            },
        )
        .await
    }

    /// Read frames until every id in `ids` has replied; returns each reply with
    /// the instant it arrived, in arrival order (events are skipped).
    pub async fn replies(
        &mut self,
        ids: &[u64],
    ) -> std::io::Result<Vec<(u64, Frame, std::time::Instant)>> {
        let mut out = Vec::new();
        while out.len() < ids.len() {
            match read_frame(&mut self.stream).await? {
                Some(f) if ids.contains(&f.request_id) => {
                    out.push((f.request_id, f, std::time::Instant::now()))
                }
                Some(_) => {}
                None => return Err(std::io::Error::other("eof waiting for replies")),
            }
        }
        Ok(out)
    }

    /// Next tick or gap; `Ok(None)` on EOF; other frames are skipped.
    pub async fn next_item(&mut self) -> std::io::Result<Option<Item>> {
        loop {
            let Some(frame) = read_frame(&mut self.stream).await? else {
                return Ok(None);
            };
            if let Some(Body::Event(ev)) = frame.body {
                match ev.body {
                    Some(rithmic_gateway::pb::event::Body::LastTrade(lt)) => {
                        return Ok(Some(Item::Tick {
                            seq: lt.trade_size.unwrap_or(0),
                            created_ns: lt.ts_event_ns.unwrap_or(0),
                        }))
                    }
                    Some(rithmic_gateway::pb::event::Body::Gap(g)) => {
                        return Ok(Some(Item::Gap { skipped: g.skipped }))
                    }
                    Some(rithmic_gateway::pb::event::Body::TransportResumed(_)) => {
                        return Ok(Some(Item::Resumed))
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn write_frame(stream: &mut UnixStream, frame: &Frame) -> std::io::Result<()> {
    let payload = frame.encode_to_vec();
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    stream.write_all(&out).await
}

async fn read_frame(stream: &mut UnixStream) -> std::io::Result<Option<Frame>> {
    let mut len = [0u8; 4];
    match stream.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let mut payload = vec![0u8; u32::from_be_bytes(len) as usize];
    stream.read_exact(&mut payload).await?;
    Frame::decode(payload.as_slice())
        .map(Some)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Wait up to `total` for `cond` to hold, polling every 10 ms.
pub async fn wait_until(total: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + total;
    while tokio::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cond()
}
