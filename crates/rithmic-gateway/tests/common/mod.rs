//! Shared harness for the gateway client tests: a real fake-plant parent
//! (`FakeParent`) and a scriptable mock parent (`MockParent`).
#![allow(dead_code, clippy::large_enum_variant)]

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prost::Message;
use rithmic_gateway::client::{GatewayConfig, RemoteSession};
use rithmic_gateway::pb::{self, frame::Body, Frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::net::UnixListener;

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

#[allow(unused_imports)]
pub use rithmic_gateway::testing::wait_until;

pub mod unix;

// --- the real server over a fake plant ---------------------------------------

#[derive(Default, Clone, Copy)]
pub struct Opts {
    pub trading: bool,
    pub cancel_all: bool,
}

pub const AUTH_TOKEN: &str = "duplex-test-token";

/// Real server and pump, with in-memory transports and scripted fake plants.
pub struct FakeParent {
    pub user: String,
    pub fake: Arc<rithmic_host::fake::FakeVenue>,
    pub state: Arc<rithmic_gateway::server::GatewayState>,
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    pump: tokio::task::JoinHandle<()>,
    shared: RemoteSession,
}

pub fn gateway_state(
    user: &str,
    fake: Arc<rithmic_host::fake::FakeVenue>,
    opts: Opts,
) -> Arc<rithmic_gateway::server::GatewayState> {
    use rithmic_gateway::{
        idle_exit::{IdleExit, IdleExitPolicy},
        reconnect::ReconnectController,
        server::{boot_gateway_metadata, Fingerprint, GatewayState, SessionHandle},
        subscriptions::{FanoutHub, ParentGates},
    };
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    let hub = Arc::new(FanoutHub::new(
        rithmic_gateway::subscriptions::DEFAULT_QUEUE_CAP,
    ));
    let (gateway_instance_id, transport_generation, transport_epoch) = boot_gateway_metadata();
    Arc::new(GatewayState {
        gates: ParentGates {
            trading_enabled: opts.trading,
            cancel_all_enabled: opts.cancel_all,
        },
        reconnect: Arc::new(ReconnectController::new(hub.clone())),
        hub,
        fingerprint: Fingerprint {
            user: user.into(),
            system_name: SYSTEM.into(),
            url: URL.into(),
            env: "Live".into(),
            account_id: String::new(),
            fcm_id: String::new(),
            ib_id: String::new(),
        },
        ready: AtomicBool::new(true),
        force_reconnect: AtomicBool::new(false),
        reconnect_wake: tokio::sync::Notify::new(),
        expected_auth_token: AUTH_TOKEN.into(),
        session: Some(SessionHandle::new(fake)),
        topic_locks: tokio::sync::Mutex::new(HashMap::new()),
        recon_lock: Arc::new(tokio::sync::Mutex::new(())),
        md_history_gate: Arc::new(tokio::sync::Mutex::new(())),
        history_load_lock: Arc::new(tokio::sync::Mutex::new(())),
        idle: IdleExit::new(IdleExitPolicy::Never),
        gateway_instance_id,
        transport_generation,
        transport_epoch,
        timing: rithmic_gateway::server::pump::PumpTiming {
            restore_backoff: Duration::from_millis(50),
            ..Default::default()
        },
    })
}

fn connection(
    state: Arc<rithmic_gateway::server::GatewayState>,
    tasks: &Mutex<Vec<tokio::task::JoinHandle<()>>>,
) -> DuplexStream {
    // A relay gives stop/restart a transport-level disconnect without aborting
    // serve_connection before its ordinary detach/teardown path runs.
    let (client, mut relay_client) = tokio::io::duplex(64 * 1024);
    let (mut relay_server, server) = tokio::io::duplex(64 * 1024);
    let relay = tokio::spawn(async move {
        let _ = tokio::io::copy_bidirectional(&mut relay_client, &mut relay_server).await;
    });
    tasks.lock().unwrap().push(relay);
    tokio::spawn(async move {
        let _ = rithmic_gateway::server::serve_connection(server, state).await;
    });
    client
}

impl FakeParent {
    pub async fn start(opts: Opts) -> Self {
        use rithmic_host::Session;
        let user = unique_user("fake");
        let fake = Arc::new(rithmic_host::fake::FakeVenue::new());
        fake.connect().await.unwrap();
        let state = gateway_state(&user, fake.clone(), opts);
        let pump = tokio::spawn(rithmic_gateway::server::pump::run_event_pump(
            state.session.clone().unwrap(),
            state.hub.clone(),
            state.reconnect.clone(),
            state.clone(),
        ));
        let tasks = Arc::new(Mutex::new(Vec::new()));
        let cfg = GatewayConfig::new(&user, SYSTEM, URL, "Live").with_auth_token(AUTH_TOKEN);
        let shared = RemoteSession::with_connector(cfg, {
            let state = state.clone();
            let tasks = tasks.clone();
            move || {
                let client = connection(state.clone(), &tasks);
                async { Ok(client) }
            }
        });
        Self {
            user,
            fake,
            state,
            tasks,
            pump,
            shared,
        }
    }

    pub fn session(&self) -> RemoteSession {
        self.shared.clone()
    }

    pub fn stream(&self) -> DuplexStream {
        connection(self.state.clone(), &self.tasks)
    }

    pub fn stop(&mut self) {
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
        self.pump.abort();
    }

    pub async fn control(&self, commands: &[String]) -> Vec<Vec<String>> {
        let mut replies = Vec::new();
        for batch in commands.chunks(200) {
            for cmd in batch {
                replies.push(
                    rithmic_gateway::testing::control_command(&self.fake, cmd)
                        .lines()
                        .map(str::to_owned)
                        .collect(),
                );
            }
            // Preserve bounded producer batches without relying on socket I/O
            // to let the pump and client readers run.
            tokio::task::yield_now().await;
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
    streams: Option<tokio::sync::mpsc::UnboundedSender<DuplexStream>>,
    task: tokio::task::JoinHandle<()>,
    accept: Option<tokio::task::JoinHandle<()>>,
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
        _path: &Path,
        listener: UnixListener,
        ready: pb::Ready,
        not_ready_first: usize,
        handler: impl Fn(&Frame, usize) -> Action + Send + Sync + 'static,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let accept = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                if tx.send(stream).is_err() {
                    break;
                }
            }
        });
        let mut mock = Self::serve_streams(rx, ready, not_ready_first, handler, None);
        mock.accept = Some(accept);
        mock
    }

    pub fn duplex(
        ready: pb::Ready,
        not_ready_first: usize,
        handler: impl Fn(&Frame, usize) -> Action + Send + Sync + 'static,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self::serve_streams(rx, ready, not_ready_first, handler, Some(tx))
    }

    pub fn session(&self, cfg: GatewayConfig) -> RemoteSession {
        let tx = self.streams.as_ref().expect("duplex mock").clone();
        RemoteSession::with_connector(cfg, move || {
            let (client, server) = tokio::io::duplex(64 * 1024);
            let sent = tx
                .send(server)
                .map_err(|_| rithmic_host::Error::Transport("mock parent stopped".into()));
            async move {
                sent?;
                Ok(client)
            }
        })
    }

    fn serve_streams<S>(
        mut streams_rx: tokio::sync::mpsc::UnboundedReceiver<S>,
        ready: pb::Ready,
        not_ready_first: usize,
        handler: impl Fn(&Frame, usize) -> Action + Send + Sync + 'static,
        streams: Option<tokio::sync::mpsc::UnboundedSender<DuplexStream>>,
    ) -> Self
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
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
        let task = tokio::spawn(async move {
            let not_ready = Arc::new(AtomicUsize::new(not_ready_first));
            let mut clients = tokio::task::JoinSet::new();
            loop {
                let Some(mut stream) = streams_rx.recv().await else {
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
                clients.spawn(async move {
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
                    let (mut read, mut write) = tokio::io::split(stream);
                    let (frames_tx, mut frames) = tokio::sync::mpsc::channel::<Frame>(64);
                    let reader = AbortOnDrop(tokio::spawn(async move {
                        while let Ok((frame, _)) = read_frame(&mut read).await {
                            if frames_tx.send(frame).await.is_err() {
                                return;
                            }
                        }
                    }));
                    loop {
                        tokio::select! {
                            frame = frames.recv() => {
                                let Some(frame) = frame else { break };
                                reqs.lock().unwrap().push(frame.clone());
                                match handler(&frame, index) {
                                    Action::Reply(body) => {
                                        write_body(&mut write, frame.request_id, body).await
                                    }
                                    Action::Ack => {
                                        write_body(&mut write, frame.request_id, Body::Ack(pb::Ack {})).await
                                    }
                                    Action::Close => break,
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
                    drop(reader);
                    let _ = write.shutdown().await;
                });
                while clients.try_join_next().is_some() {}
            }
        });
        Self {
            accept: None,
            streams,
            task,
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

impl Drop for MockParent {
    fn drop(&mut self) {
        self.task.abort();
        if let Some(accept) = &self.accept {
            accept.abort();
        }
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub fn duplex_config() -> GatewayConfig {
    let mut cfg = GatewayConfig::new(&unique_user("duplex"), SYSTEM, URL, "Live");
    cfg.rpc_timeout = Duration::from_secs(5);
    cfg.attach_timeout = Duration::from_secs(5);
    cfg
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
