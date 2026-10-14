//! Shared harness for the gateway client tests: a real fake-plant parent
//! (`FakeParent`) and a scriptable mock parent (`MockParent`).
#![allow(dead_code, clippy::large_enum_variant)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prost::Message;
use rithmic_gateway::client::GatewayConfig;
use rithmic_gateway::pb::{self, frame::Body, Frame};
use rithmic_gateway::singleton::flock_held;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

pub const SYSTEM: &str = "LucidTrading";
pub const URL: &str = "wss://fake.example";
pub const NQ: &str = "NQH6";
pub const CME: &str = "CME";

pub fn unique_user(prefix: &str) -> String {
    use std::sync::atomic::AtomicU64;
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    )
}

/// A short, private runtime dir (macOS `sockaddr_un` is ~104 bytes).
pub fn short_dir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("rgf-")
        .tempdir_in("/tmp")
        .expect("tempdir")
}

pub use rithmic_gateway::testing::wait_until;

// --- the real server over a fake plant ---------------------------------------

#[derive(Default, Clone, Copy)]
pub struct Opts {
    pub trading: bool,
    pub cancel_all: bool,
}

pub struct FakeParent {
    child: Option<Child>,
    pub dir: tempfile::TempDir,
    pub user: String,
    pub listen: PathBuf,
    pub control: PathBuf,
    opts: Opts,
}

impl FakeParent {
    pub async fn start(opts: Opts) -> Self {
        let mut parent = Self {
            child: None,
            dir: short_dir(),
            user: unique_user("fake"),
            listen: PathBuf::new(),
            control: PathBuf::new(),
            opts,
        };
        parent.listen = parent.dir.path().join("gw.sock");
        parent.control = parent.dir.path().join("ctl.sock");
        parent.spawn().await;
        parent
    }

    pub fn config(&self) -> GatewayConfig {
        GatewayConfig::new(&self.user, SYSTEM, URL, "Live")
            .with_listen(&format!("unix://{}", self.listen.display()))
            .expect("listen")
    }

    async fn spawn(&mut self) {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_rithmic-gateway-fake"));
        cmd.env_clear()
            .env("RITHMIC_USER", &self.user)
            .env("RITHMIC_SYSTEM_NAME", SYSTEM)
            .env("RITHMIC_URL", URL)
            .env("RITHMIC_ENV", "Live")
            .env(
                "RITHMIC_GATEWAY_LISTEN",
                format!("unix://{}", self.listen.display()),
            )
            .env("RITHMIC_FAKE_CONTROL", &self.control)
            .env("RITHMIC_GATEWAY_RESTORE_BACKOFF_MS", "50")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // The credential flock lives under the runtime dir both sides derive.
        for key in ["PATH", "HOME", "TMPDIR", "XDG_RUNTIME_DIR"] {
            if let Ok(v) = std::env::var(key) {
                cmd.env(key, v);
            }
        }
        if self.opts.trading {
            cmd.env("RITHMIC_ENABLE_TRADING", "1");
        }
        if self.opts.cancel_all {
            cmd.env("RITHMIC_GATEWAY_CANCEL_ALL", "1");
        }
        self.child = Some(cmd.spawn().expect("spawn fake gateway"));
        let up = wait_until(Duration::from_secs(20), || {
            std::os::unix::net::UnixStream::connect(&self.listen).is_ok()
                && flock_held(&self.user, SYSTEM, URL, "Live")
                && self.control.exists()
        })
        .await;
        assert!(up, "fake gateway did not come up");
    }

    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Kill the parent and start a fresh one on the same paths / fingerprint.
    pub async fn restart(&mut self) {
        self.stop();
        assert!(
            wait_until(Duration::from_secs(10), || !flock_held(
                &self.user, SYSTEM, URL, "Live"
            ))
            .await,
            "flock not released"
        );
        let _ = std::fs::remove_file(&self.listen);
        self.spawn().await;
    }

    pub async fn control(&self, commands: &[String]) -> Vec<Vec<String>> {
        let mut stream = UnixStream::connect(&self.control)
            .await
            .expect("control connect");
        let mut replies = Vec::new();
        for chunk in commands.chunks(200) {
            let mut text = String::new();
            for c in chunk {
                text.push_str(c);
                text.push('\n');
            }
            stream
                .write_all(text.as_bytes())
                .await
                .expect("control write");
            let mut lines = BufReader::new(&mut stream).lines();
            let mut current = Vec::new();
            let mut got = 0;
            while got < chunk.len() {
                let line = lines
                    .next_line()
                    .await
                    .expect("control read")
                    .expect("control closed");
                if line == "END" {
                    replies.push(std::mem::take(&mut current));
                    got += 1;
                } else {
                    current.push(line);
                }
            }
        }
        replies
    }

    pub async fn command(&self, cmd: &str) -> Vec<String> {
        let reply = self.control(&[cmd.to_string()]).await.pop().expect("reply");
        assert!(
            !reply.first().is_some_and(|l| l.starts_with("err")),
            "{cmd}: {reply:?}"
        );
        reply
    }

    pub async fn calls(&self) -> Vec<String> {
        self.command("calls").await
    }

    pub async fn count(&self, prefix: &str) -> usize {
        self.calls()
            .await
            .iter()
            .filter(|c| c.starts_with(prefix))
            .count()
    }

    pub async fn clear_calls(&self) {
        self.command("clear_calls").await;
    }

    pub async fn push_last_trades(&self, symbol: &str, n: usize) {
        let cmds: Vec<String> = (0..n)
            .map(|i| format!("push last_trade {symbol} {CME} {}.25 1", 100 + i))
            .collect();
        for reply in self.control(&cmds).await {
            assert_eq!(reply, ["ok"]);
        }
    }

    pub async fn wait_for_call(&self, prefix: &str, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.count(prefix).await > 0 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }
}

impl Drop for FakeParent {
    fn drop(&mut self) {
        self.stop();
    }
}

// --- a scriptable mock parent --------------------------------------------------

/// What the mock does with one request.
pub enum Action {
    /// Reply with this body.
    Reply(Body),
    /// Reply `Ack`.
    Ack,
    /// Close the connection without replying.
    Close,
    /// Read the request and never answer.
    Hang,
}

type Handler = dyn Fn(&Frame, usize) -> Action + Send + Sync;

/// A tiny parent: Handshake → Ready, then every request goes to `handler`
/// (request, connection index). Records every request it receives.
pub struct MockParent {
    pub path: PathBuf,
    pub requests: Arc<Mutex<Vec<Frame>>>,
    pub connections: Arc<AtomicUsize>,
    pub handshakes: Arc<Mutex<Vec<Vec<u8>>>>,
    /// Push an event (request_id 0) to every connected client.
    pub events: tokio::sync::broadcast::Sender<Body>,
}

pub async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> std::io::Result<(Frame, Vec<u8>)> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let mut buf = vec![0u8; u32::from_be_bytes(len) as usize];
    r.read_exact(&mut buf).await?;
    let frame = Frame::decode(buf.as_slice()).expect("decode");
    Ok((frame, buf))
}

pub async fn write_body<W: AsyncWriteExt + Unpin>(w: &mut W, request_id: u64, body: Body) {
    let bytes = rithmic_gateway::codec::encode(&Frame {
        request_id,
        body: Some(body),
    })
    .expect("encode");
    let _ = w.write_all(&bytes).await;
}

impl MockParent {
    /// Bind `path` now and serve until dropped. `ready` is the `Ready` reply;
    /// `not_ready_first` handshakes are answered `not_ready`.
    pub async fn spawn(
        path: &Path,
        ready: pb::Ready,
        not_ready_first: usize,
        handler: impl Fn(&Frame, usize) -> Action + Send + Sync + 'static,
    ) -> Self {
        let listener = UnixListener::bind(path).expect("bind mock");
        Self::serve(path, listener, ready, not_ready_first, handler)
    }

    pub fn serve(
        path: &Path,
        listener: UnixListener,
        ready: pb::Ready,
        not_ready_first: usize,
        handler: impl Fn(&Frame, usize) -> Action + Send + Sync + 'static,
    ) -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(AtomicUsize::new(0));
        let handshakes = Arc::new(Mutex::new(Vec::new()));
        let (events_tx, _) = tokio::sync::broadcast::channel::<Body>(65_536);
        let handler: Arc<Handler> = Arc::new(handler);
        let (reqs, conns, hs, events) = (
            requests.clone(),
            connections.clone(),
            handshakes.clone(),
            events_tx.clone(),
        );
        tokio::spawn(async move {
            let not_ready = Arc::new(AtomicUsize::new(not_ready_first));
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let index = conns.fetch_add(1, Ordering::SeqCst);
                let (reqs, hs, handler, ready, not_ready) = (
                    reqs.clone(),
                    hs.clone(),
                    handler.clone(),
                    ready.clone(),
                    not_ready.clone(),
                );
                let mut events = events.subscribe();
                tokio::spawn(async move {
                    let Ok((hello, raw)) = read_frame(&mut stream).await else {
                        return;
                    };
                    hs.lock().unwrap().push(raw);
                    assert!(matches!(hello.body, Some(Body::Handshake(_))));
                    if not_ready
                        .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok()
                    {
                        write_body(
                            &mut stream,
                            0,
                            Body::Error(pb::ErrorResponse {
                                code: "not_ready".into(),
                                message: "plants connecting".into(),
                            }),
                        )
                        .await;
                        return;
                    }
                    write_body(&mut stream, 0, Body::Ready(ready)).await;

                    // Frames are read on their own task so the select below only
                    // waits on cancel-safe channel receives.
                    let (mut read, mut write) = stream.into_split();
                    let (frames_tx, mut frames) = tokio::sync::mpsc::channel::<Frame>(64);
                    tokio::spawn(async move {
                        while let Ok((frame, _)) = read_frame(&mut read).await {
                            if frames_tx.send(frame).await.is_err() {
                                return;
                            }
                        }
                    });
                    loop {
                        tokio::select! {
                            frame = frames.recv() => {
                                let Some(frame) = frame else { return };
                                reqs.lock().unwrap().push(frame.clone());
                                match handler(&frame, index) {
                                    Action::Reply(body) => {
                                        write_body(&mut write, frame.request_id, body).await
                                    }
                                    Action::Ack => {
                                        write_body(&mut write, frame.request_id, Body::Ack(pb::Ack {})).await
                                    }
                                    Action::Close => return,
                                    Action::Hang => {}
                                }
                            }
                            event = events.recv() => {
                                if let Ok(body) = event {
                                    write_body(&mut write, 0, Body::Event(match body {
                                        Body::Event(e) => e,
                                        _ => continue,
                                    })).await;
                                }
                            }
                        }
                    }
                });
            }
        });
        Self {
            path: path.to_path_buf(),
            requests,
            connections,
            handshakes,
            events: events_tx,
        }
    }

    /// Push a last-trade event to every connected client.
    pub fn push_last_trade(&self, symbol: &str, price: f64) {
        let _ = self.events.send(Body::Event(pb::Event {
            body: Some(pb::event::Body::LastTrade(pb::LastTrade {
                symbol: Some(symbol.into()),
                exchange: Some("CME".into()),
                trade_price: Some(price),
                trade_size: Some(1),
                ..Default::default()
            })),
        }));
    }

    pub fn request_bodies(&self) -> Vec<Body> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter_map(|f| f.body.clone())
            .collect()
    }
}

pub fn mock_ready(trading: bool, cancel_all: bool) -> pb::Ready {
    pb::Ready {
        scopes: vec!["md".into(), "history".into()],
        trading_enabled: trading,
        cancel_all_enabled: cancel_all,
        gateway_instance_id: 7,
        transport_generation: 0,
    }
}

/// A client config for a mock parent (no flock attestation).
pub fn mock_config(path: &Path) -> GatewayConfig {
    let mut cfg = GatewayConfig::new(&unique_user("mock"), SYSTEM, URL, "Live")
        .with_listen(&format!("unix://{}", path.display()))
        .expect("listen");
    cfg.attest_flock = false;
    cfg.rpc_timeout = Duration::from_secs(5);
    cfg.attach_timeout = Duration::from_secs(5);
    cfg
}

/// Wait until the async `cond` holds, bounded by `timeout` (the async twin of
/// `rithmic_gateway::testing::wait_until`). Tests wait on an observable
/// condition instead of a fixed sleep.
pub async fn wait_until_async<F, Fut>(timeout: Duration, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if cond().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
