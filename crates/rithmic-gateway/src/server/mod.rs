//! Accept loop: Handshake + Ready, gated RPC dispatch backed by
//! [`rithmic_host::Session`], and per-client fan-out of pushed
//! events. Client disconnect is detach-only (refcount decrement) while peers
//! remain; optional idle-exit shuts down the parent after the last Ready
//! peer leaves (see [`crate::idle_exit`]).

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::FutureExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::{mpsc, watch};

use rithmic_host::Session;

use crate::codec;
use crate::idle_exit::{IdleExit, IdleExitPolicy};
use crate::pb::{self, frame::Body, Frame, Handshake, Ready};

mod dispatch;
pub mod pump;
mod transport;
pub use dispatch::{
    gate_rpc_for_test, history_rpc_with_live_ticker_intent_for_test, rpc_sequence_with_gates,
};
pub use transport::listen_unix;
pub use transport::listen_unix as serve;

/// Metadata advertised in every ``Ready`` frame for this parent process.
///
/// Returns `(gateway_instance_id, transport_generation, transport_epoch)`.
/// `transport_epoch` notifies live clients when generation bumps so they
/// re-handshake instead of keeping a stale Ready generation.
pub fn boot_gateway_metadata() -> (u64, AtomicU64, watch::Sender<u64>) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        ^ (std::process::id() as u64);
    let (epoch_tx, _) = watch::channel(1u64);
    (id, AtomicU64::new(1), epoch_tx)
}

/// Compatibility name for the venue rebuild gate. Host owns the gate.
pub type SessionHandle = rithmic_host::VenueHandle;

/// Transport-owned state. All plant policy is behind `host`.
pub struct GatewayState {
    pub host: Arc<rithmic_host::SessionHost>,
    pub fingerprint: Fingerprint,
    pub expected_auth_token: String,
    pub idle: IdleExit,
    pub gateway_instance_id: u64,
}
impl std::ops::Deref for GatewayState {
    type Target = rithmic_host::host::HostState;
    fn deref(&self) -> &Self::Target {
        &self.host.state
    }
}

/// Credential fingerprint advertised/checked at Handshake (consistency
/// check only — never authentication; see [`crate::pb::Handshake::auth_token`]).
#[derive(Debug, Clone)]
pub struct Fingerprint {
    pub user: String,
    pub system_name: String,
    pub url: String,
    pub env: String,
    pub account_id: String,
    pub fcm_id: String,
    pub ib_id: String,
}

impl Fingerprint {
    pub fn matches(&self, hs: &Handshake) -> bool {
        hs.user == self.user
            && hs.system_name == self.system_name
            && hs.url == self.url
            && (hs.env.is_empty()
                || hs.env.eq_ignore_ascii_case(&self.env)
                || env_aliases_match(&hs.env, &self.env))
            && account_field_ok(&self.account_id, &hs.account_id)
            && account_field_ok(&self.fcm_id, &hs.fcm_id)
            && account_field_ok(&self.ib_id, &hs.ib_id)
    }
}

/// When the parent has a non-empty account field, the client must match it.
fn account_field_ok(parent: &str, client: &str) -> bool {
    parent.is_empty() || parent == client
}

/// Constant-time equality for auth tokens (length mismatch → false).
fn auth_token_eq(a: &str, b: &str) -> bool {
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    if ab.len() != bb.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in ab.iter().zip(bb.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Handshake gates shared by accept path and unit tests.
fn authorize_handshake(
    state: &GatewayState,
    hs: &Handshake,
    same_uid: bool,
) -> Result<(), (&'static str, &'static str)> {
    if !state.fingerprint.matches(hs) {
        return Err((
            "fingerprint_mismatch",
            "handshake does not match this gateway's credential set",
        ));
    }
    // v1 local policy (KTD16 / R15): empty auth_token is only trusted from a
    // same-UID unix peer. Non-empty tokens must match the parent secret.
    if hs.auth_token.is_empty() {
        if !same_uid {
            return Err((
                "auth_required",
                "empty auth_token is only accepted from a same-UID unix peer",
            ));
        }
    } else if state.expected_auth_token.is_empty()
        || !auth_token_eq(&hs.auth_token, &state.expected_auth_token)
    {
        return Err(("auth_failed", "auth_token rejected"));
    }
    if !state.ready.load(Ordering::SeqCst) {
        return Err(("not_ready", "gateway not Ready (plants not connected)"));
    }
    Ok(())
}

fn env_aliases_match(a: &str, b: &str) -> bool {
    fn canon(s: &str) -> &'static str {
        match s.to_ascii_lowercase().as_str() {
            "live" | "production" => "live",
            "demo" | "development" => "demo",
            "test" => "test",
            _ => "",
        }
    }
    let ca = canon(a);
    let cb = canon(b);
    !ca.is_empty() && ca == cb
}

/// Everything `run_gateway` needs besides the already-connected session.
pub struct GatewayParams {
    pub listener: UnixListener,
    /// Socket path to remove on shutdown.
    pub path: std::path::PathBuf,
    pub fingerprint: Fingerprint,
    pub expected_auth_token: String,
    pub idle_policy: IdleExitPolicy,
    pub host: Arc<rithmic_host::SessionHost>,
}

/// Serve the gateway over an already-connected session until the listener
/// ends: build state, run the event pump, serve clients, then disconnect the
/// session and remove the socket. The production binary and the test-only
/// fake-plant binary both run exactly this.
pub async fn run_gateway(params: GatewayParams) -> Result<(), String> {
    let GatewayParams {
        listener,
        path,
        fingerprint,
        expected_auth_token,
        idle_policy,
        host,
    } = params;
    host.state.hub.set_encoder(encode_plant_event);
    let (gateway_instance_id, _, _) = boot_gateway_metadata();
    let state = Arc::new(GatewayState {
        host: host.clone(),
        fingerprint,
        expected_auth_token,
        idle: IdleExit::new(idle_policy),
        gateway_instance_id,
    });
    let serve_result = serve(listener, state).await.map_err(|e| e.to_string());
    host.shutdown().await;
    let _ = std::fs::remove_file(&path);
    serve_result
}

/// How long a new connection may take to send its handshake.
const HANDSHAKE_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Largest handshake frame accepted from a peer that has not authenticated yet.
const HANDSHAKE_MAX_LEN: usize = 64 * 1024;

async fn read_frame_bytes<R: tokio::io::AsyncRead + Unpin>(
    stream: &mut R,
) -> std::io::Result<Option<Vec<u8>>> {
    read_frame_bytes_max(stream, crate::framing::MAX_FRAME_LEN as usize).await
}

async fn read_frame_bytes_max<R: tokio::io::AsyncRead + Unpin>(
    stream: &mut R,
    max_len: usize,
) -> std::io::Result<Option<Vec<u8>>> {
    // Idle wait for the next frame length has no short timeout (clients may
    // pause between RPCs). Once a length is known, bound the payload read so a
    // peer cannot allocate MAX_FRAME_LEN and stall forever.
    const PAYLOAD_READ_TIMEOUT: Duration = Duration::from_secs(5);
    let mut len_bytes = [0u8; 4];
    match stream.read_exact(&mut len_bytes).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > max_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut payload = vec![0u8; len];
    match tokio::time::timeout(PAYLOAD_READ_TIMEOUT, stream.read_exact(&mut payload)).await {
        Ok(Ok(_)) => Ok(Some(payload)),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "payload read timeout",
        )),
    }
}

async fn write_frame<W: AsyncWrite + Unpin>(stream: &mut W, frame: &Frame) -> Result<(), String> {
    let wire = codec::encode(frame).map_err(|e| e.to_string())?;
    tokio::time::timeout(Duration::from_secs(5), stream.write_all(&wire))
        .await
        .map_err(|_| "write timeout".to_string())?
        .map_err(|e| format!("write: {e}"))
}

/// Encode one event as a wire frame (a single allocation) ready to fan out.
fn encode_event(event: pb::Event) -> Option<Bytes> {
    let frame = Frame {
        request_id: 0,
        body: Some(Body::Event(event)),
    };
    let wire = codec::encode(&frame).ok()?;
    Some(Bytes::from(wire))
}

/// The one protobuf encoding boundary, also used as the host's optional
/// projection for hosts with exclusively remote consumers.
pub fn encode_plant_event(event: rithmic_host::PlantEvent) -> Bytes {
    encode_event(crate::convert::plant_event_to_pb(event)).unwrap_or_default()
}
pub use rithmic_host::fanout::publish_plant_event;

/// Serve an authenticated session over an already-connected transport.
/// Generic transports require a token; only `listen_unix` attests same-UID peers.
pub async fn serve_connection<S>(stream: S, state: Arc<GatewayState>) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_connection_with_peer(stream, state, false).await
}

async fn serve_connection_with_peer<S>(
    mut stream: S,
    state: Arc<GatewayState>,
    same_uid: bool,
) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Before it authenticates a peer gets a short deadline and a small frame
    // budget: an idle or hostile connection cannot hold a task or 16 MiB.
    let payload = tokio::time::timeout(
        HANDSHAKE_READ_TIMEOUT,
        read_frame_bytes_max(&mut stream, HANDSHAKE_MAX_LEN),
    )
    .await
    .map_err(|_| "no handshake in time".to_string())?
    .map_err(|e| format!("read handshake: {e}"))?
    .ok_or_else(|| "client closed before handshake".to_string())?;
    // Use codec path (framed buffer) so production shares decode with tests.
    let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
    framed.extend_from_slice(&payload);
    let (req, _) = codec::decode::<Frame>(&framed).map_err(|e| format!("proto: {e}"))?;
    let hs = match req.body {
        Some(Body::Handshake(hs)) => hs,
        _ => return Err("first frame must be Handshake".into()),
    };
    if state.idle.is_shutting_down() {
        let _ = write_frame(
            &mut stream,
            &dispatch::error_frame(0, "shutting_down", "gateway idle-exit in progress"),
        )
        .await;
        return Err("shutting_down".into());
    }
    if let Err((code, msg)) = authorize_handshake(&state, &hs, same_uid) {
        let _ = write_frame(&mut stream, &dispatch::error_frame(0, code, msg)).await;
        return Err(format!("{code}: {msg}"));
    }

    // Attach and subscribe before Ready: a recovery during the handshake must
    // still deliver its interruption and completed generation to this connection.
    state.host.state.hub.set_encoder(encode_plant_event);
    let client = state.host.attach();
    let session_events = client.session_events();

    write_frame(
        &mut stream,
        &Frame {
            request_id: 0,
            body: Some(Body::Ready(Ready {
                scopes: state.gates.scopes(),
                trading_enabled: state.gates.snapshot().trading,
                cancel_all_enabled: state.gates.snapshot().cancel_all,
                gateway_instance_id: state.gateway_instance_id,
                transport_generation: state.transport_generation.load(Ordering::Relaxed),
            })),
        },
    )
    .await?;

    if !state.idle.peer_attached().await {
        let _ = write_frame(
            &mut stream,
            &dispatch::error_frame(0, "shutting_down", "gateway idle-exit in progress"),
        )
        .await;
        return Err("shutting_down".into());
    }

    let (read_half, write_half) = tokio::io::split(stream);
    serve_connection_halves(read_half, write_half, state, client, session_events).await
}

async fn serve_connection_halves<R, W>(
    read_half: R,
    write_half: W,
    state: Arc<GatewayState>,
    client: rithmic_host::LocalSession,
    mut session_events: tokio::sync::broadcast::Receiver<rithmic_host::SessionEvent>,
) -> Result<(), String>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let out_rx = client.take_events().expect("new attachment event queue");
    // One connection is four tasks so no request can hold up another:
    //   reader   owns the read half, decodes frames and hands each to a lane;
    //   commands runs state-changing / subscription requests in arrival order;
    //   slow     runs read RPCs (history loads) a few at a time, off that lane;
    //   writer   owns the write half; replies go out ahead of fan-out events.
    let (reply_tx, reply_rx) = mpsc::channel::<Frame>(COMMAND_QUEUE + SLOW_QUEUE);
    let (cmd_tx, cmd_rx) = mpsc::channel::<Request>(COMMAND_QUEUE);
    let (slow_tx, slow_rx) = mpsc::channel::<Request>(SLOW_QUEUE);
    let (stop_tx, stop_rx) = watch::channel(false);

    let mut reader = tokio::spawn(read_requests(read_half, cmd_tx, slow_tx, reply_tx.clone()));
    // Detached: when the connection ends the slow lane drains on its own (each
    // read is bounded by its own timeout). Aborting it mid-drain could leave an
    // order replay half-read on the plant, to be collected by the next drain.
    tokio::spawn(run_slow_lane(
        Arc::clone(&state),
        client.clone(),
        slow_rx,
        reply_tx.clone(),
    ));
    // Kept to tell this client when the plants were rebuilt; dropped before the
    // writer is awaited so the reply channel can close.
    let resume_tx = reply_tx.clone();
    let mut commands = tokio::spawn(run_commands(
        Arc::clone(&state),
        client,
        cmd_rx,
        reply_tx,
        stop_rx,
    ));
    let mut writer = tokio::spawn(write_replies(write_half, reply_rx, out_rx));

    let mut result: Result<(), String> = Ok(());
    let mut reader_done = false;
    let mut writer_done = false;
    let client = loop {
        tokio::select! {
            // The peer closed or the stream broke: let the command lane finish
            // what was already queued, then it ends on its own.
            r = &mut reader, if !reader_done => {
                reader_done = true;
                if let Ok(Err(e)) = r {
                    result = Err(e);
                }
            }
            // The command lane ended: a Disconnect, or the connection is done.
            done = &mut commands => break done.ok(),
            // The client stopped draining its socket (or it died).
            _ = &mut writer => {
                writer_done = true;
                let _ = stop_tx.send(true);
                break (&mut commands).await.ok();
            }
            event = session_events.recv() => {
                use rithmic_host::SessionEvent;
                let body = match event {
                    Ok(SessionEvent::Interrupted { generation, .. }) => {
                        pb::event::Body::TransportInterrupted(pb::TransportInterrupted { generation })
                    }
                    Ok(SessionEvent::Resumed { generation }) => {
                        pb::event::Body::TransportResumed(pb::TransportResumed { generation })
                    }
                    // No failure frame was added to the protocol. Flush the
                    // interruption and queued replies, then close this transport.
                    Ok(SessionEvent::RecoveryFailed { .. }) | Err(_) => {
                        reader.abort();
                        let _ = stop_tx.send(true);
                        break (&mut commands).await.ok();
                    }
                };
                let frame = Frame {
                    request_id: 0,
                    body: Some(Body::Event(pb::Event { body: Some(body) })),
                };
                if resume_tx.send(frame).await.is_err() {
                    let _ = stop_tx.send(true);
                    break (&mut commands).await.ok();
                }
            }
        }
    };

    drop(resume_tx);
    reader.abort();
    if let Some(client) = client {
        let _ = client.disconnect().await;
    }
    // With every sender gone the writer flushes what is queued (a Disconnect
    // ack) and ends; bound the wait so a dead peer cannot hold the teardown.
    if !writer_done {
        let _ = tokio::time::timeout(Duration::from_secs(2), &mut writer).await;
        writer.abort();
    }
    state.idle.peer_detached().await;
    result
}

/// Requests a connection may have queued for each lane before the reader waits
/// (command) or answers `busy` (slow).
const COMMAND_QUEUE: usize = 256;
const SLOW_QUEUE: usize = 64;
/// Slow read RPCs one connection runs at once.
const SLOW_CONCURRENCY: usize = 4;
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

struct Request {
    id: u64,
    body: Body,
}

/// Read and decode frames on their own task: nothing selects over this read, so
/// a partly received frame is never abandoned, and a frame is decoded straight
/// from its payload with no intermediate copy.
/// Error code for a request whose handler failed part-way; its effect is unknown.
pub const INTERNAL: &str = "internal";

/// Error code for a request the parent could not read; nothing was dispatched.
pub const BAD_REQUEST: &str = "bad_request";

/// The `request_id` (field 1) of a frame that failed to decode, read from its
/// first field when the encoder put it there (prost writes fields in order).
fn request_id_of(payload: &[u8]) -> Option<u64> {
    use prost::encoding::{decode_key, decode_varint, WireType};
    let mut buf = payload;
    let (tag, wire) = decode_key(&mut buf).ok()?;
    if tag != 1 || wire != WireType::Varint {
        return None;
    }
    decode_varint(&mut buf).ok().filter(|id| *id != 0)
}

async fn read_requests<R: tokio::io::AsyncRead + Unpin>(
    mut read: R,
    commands: mpsc::Sender<Request>,
    slow: mpsc::Sender<Request>,
    replies: mpsc::Sender<Frame>,
) -> Result<(), String> {
    use prost::Message;
    loop {
        match read_frame_bytes(&mut read).await {
            Ok(Some(payload)) => {
                let frame = match Frame::decode(payload.as_slice()) {
                    Ok(frame) => frame,
                    // An undecodable request whose id can still be read is
                    // answered (nothing was dispatched, so the client reports it
                    // as not sent) and the connection keeps serving. Without an
                    // id the reply cannot be matched: the connection ends.
                    Err(e) => match request_id_of(&payload) {
                        Some(id) => {
                            let reply = dispatch::error_frame(
                                id,
                                BAD_REQUEST,
                                &format!("undecodable request: {e}"),
                            );
                            if replies.send(reply).await.is_err() {
                                return Ok(());
                            }
                            continue;
                        }
                        None => return Err(format!("proto: {e}")),
                    },
                };
                let Some(body) = frame.body else {
                    if frame.request_id != 0 {
                        let reply = dispatch::error_frame(
                            frame.request_id,
                            BAD_REQUEST,
                            "request carries no body",
                        );
                        if replies.send(reply).await.is_err() {
                            return Ok(());
                        }
                    }
                    continue;
                };
                let req = Request {
                    id: frame.request_id,
                    body,
                };
                match crate::rpc_table::lane_of(&req.body) {
                    crate::rpc_table::Lane::Command => {
                        if commands.send(req).await.is_err() {
                            return Ok(());
                        }
                    }
                    crate::rpc_table::Lane::Slow => match slow.try_send(req) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(req)) => {
                            let busy = dispatch::error_frame(
                                req.id,
                                "busy",
                                "too many slow requests in flight on this connection",
                            );
                            if replies.send(busy).await.is_err() {
                                return Ok(());
                            }
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                    },
                }
            }
            Ok(None) => return Ok(()),
            Err(e) => return Err(format!("read: {e}")),
        }
    }
}

/// Serve state-changing and subscription requests one at a time, in the order
/// they arrived. A dispatch in progress is never cancelled (a half-applied
/// subscription would leak intent); a stop is noticed between requests. Returns
/// the client context for teardown.
async fn run_commands(
    state: Arc<GatewayState>,
    client: rithmic_host::LocalSession,
    mut requests: mpsc::Receiver<Request>,
    replies: mpsc::Sender<Frame>,
    mut stop: watch::Receiver<bool>,
) -> rithmic_host::LocalSession {
    loop {
        let req = tokio::select! {
            biased;
            _ = stop.changed() => break,
            r = requests.recv() => match r {
                Some(r) => r,
                None => break,
            },
        };
        let close_after = matches!(req.body, Body::Disconnect(_));
        // During plant reconnect, refuse new RPCs (except detach) so callers do
        // not observe Ack against a half-rebuilt venue session.
        let resp = if !close_after && !state.ready.load(Ordering::SeqCst) {
            not_ready_frame(req.id)
        } else {
            // A panic in one handler answers that request and keeps the lane:
            // ending it would skip the teardown that releases this connection's
            // intents, leaking venue subscriptions for the life of the parent.
            let id = req.id;
            match AssertUnwindSafe(dispatch::dispatch(&state, &client, id, req.body))
                .catch_unwind()
                .await
            {
                Ok(frame) => frame,
                Err(_) => {
                    eprintln!("rithmic-gateway: request {id} panicked; answered as internal");
                    dispatch::error_frame(
                        id,
                        INTERNAL,
                        "the parent failed while serving this request",
                    )
                }
            }
        };
        if replies.send(resp).await.is_err() || close_after {
            break;
        }
    }
    client
}

/// A request refused while the plants rebuild never reached the venue, so it is
/// answered with the `not_sent` class: the caller may deny it (and a command is
/// not an unknown outcome that would latch the order plant).
fn not_ready_frame(request_id: u64) -> Frame {
    dispatch::error_frame(
        request_id,
        "not_sent",
        "gateway not ready (plants reconnecting); request not sent",
    )
}

/// Serve slow read RPCs concurrently (bounded), replying as each finishes.
async fn run_slow_lane(
    state: Arc<GatewayState>,
    client: rithmic_host::LocalSession,
    mut requests: mpsc::Receiver<Request>,
    replies: mpsc::Sender<Frame>,
) {
    let permits = Arc::new(tokio::sync::Semaphore::new(SLOW_CONCURRENCY));
    let mut running = tokio::task::JoinSet::new();
    while let Some(req) = requests.recv().await {
        let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
            return;
        };
        let (state, replies, client) = (Arc::clone(&state), replies.clone(), client.clone());
        running.spawn(async move {
            let _permit = permit;
            let resp = if !state.ready.load(Ordering::SeqCst) {
                not_ready_frame(req.id)
            } else {
                dispatch::dispatch(&state, &client, req.id, req.body).await
            };
            let _ = replies.send(resp).await;
        });
        while running.try_join_next().is_some() {}
    }
    while running.join_next().await.is_some() {}
}

/// Own the write half. Replies are written ahead of fan-out events; a write that
/// takes longer than [`WRITE_TIMEOUT`] means the client stopped draining, and
/// the connection ends. The task ends once every reply sender is gone and the
/// queue is flushed.
async fn write_replies<W: tokio::io::AsyncWrite + Unpin>(
    mut write: W,
    mut replies: mpsc::Receiver<Frame>,
    mut events: rithmic_host::EventReceiver,
) {
    let mut events_open = true;
    // Ends when the reply channel has closed and drained (that flushes a final
    // ack); fan-out events are best effort and never keep the socket open.
    let mut replies_open = true;
    while replies_open {
        let wire: Option<Bytes> = tokio::select! {
            biased;
            frame = replies.recv(), if replies_open => match frame {
                Some(frame) => match codec::encode(&frame) {
                    Ok(wire) => Some(Bytes::from(wire)),
                    // A reply that cannot be framed (a history window over the frame
                    // limit) must still answer its request, or the caller waits for
                    // its own timeout.
                    Err(e) => codec::encode(&dispatch::error_frame(
                        frame.request_id,
                        "response_too_large",
                        &e.to_string(),
                    ))
                    .ok()
                    .map(Bytes::from),
                },
                None => {
                    replies_open = false;
                    None
                }
            },
            msg = events.recv(), if events_open => match msg {
                Some(fan) if fan.acknowledge_flush() => None,
                Some(fan) => Some(fan.encoded_with(|event| {
                    encode_event(crate::convert::plant_event_to_pb(event.clone())).unwrap_or_default()
                })),
                None => {
                    events_open = false;
                    None
                }
            },
        };
        if let Some(wire) = wire {
            match tokio::time::timeout(WRITE_TIMEOUT, write.write_all(&wire)).await {
                Ok(Ok(())) => {}
                _ => return, // slow / dead client
            }
        }
    }
}

#[cfg(test)]
mod auth_tests {
    use super::*;
    use crate::pb::Handshake;
    use crate::subscriptions::{FanoutHub, ParentGates};

    // Test fixture builder: maps a `Handshake`'s fields 1:1, so the arity is
    // inherent to the wire type. Allowed rather than boxing the helper.
    #[allow(clippy::too_many_arguments)]
    fn hs(
        user: &str,
        system: &str,
        url: &str,
        env: &str,
        account: &str,
        fcm: &str,
        ib: &str,
        token: &str,
    ) -> Handshake {
        Handshake {
            user: user.into(),
            system_name: system.into(),
            url: url.into(),
            env: env.into(),
            account_id: account.into(),
            fcm_id: fcm.into(),
            ib_id: ib.into(),
            auth_token: token.into(),
        }
    }

    #[test]
    fn fingerprint_matches_env_aliases() {
        let fp = Fingerprint {
            user: "alice".into(),
            system_name: "LucidTrading".into(),
            url: "wss://example".into(),
            env: "Live".into(),
            account_id: "A1".into(),
            fcm_id: "F1".into(),
            ib_id: "I1".into(),
        };
        assert!(fp.matches(&hs(
            "alice",
            "LucidTrading",
            "wss://example",
            "production",
            "A1",
            "F1",
            "I1",
            ""
        )));
        assert!(!fp.matches(&hs(
            "bob",
            "LucidTrading",
            "wss://example",
            "Live",
            "A1",
            "F1",
            "I1",
            ""
        )));
        assert!(!fp.matches(&hs(
            "alice",
            "LucidTrading",
            "wss://example",
            "Live",
            "OTHER",
            "F1",
            "I1",
            ""
        )));
    }

    #[test]
    fn auth_token_eq_constant_time_length_mismatch() {
        assert!(auth_token_eq("abc", "abc"));
        assert!(!auth_token_eq("abc", "abcd"));
        assert!(!auth_token_eq("abc", "abx"));
    }

    #[test]
    fn authorize_handshake_matrix() {
        let hub = Arc::new(FanoutHub::new(8));
        let mut state = dispatch::test_state(ParentGates::default(), hub);
        state.expected_auth_token = "secret".into();
        state.ready.store(true, Ordering::SeqCst);

        let good = hs(
            "alice",
            "LucidTrading",
            "wss://example",
            "Live",
            "A1",
            "F1",
            "I1",
            "secret",
        );
        assert!(authorize_handshake(&state, &good, false).is_ok());

        let empty = hs(
            "alice",
            "LucidTrading",
            "wss://example",
            "Live",
            "A1",
            "F1",
            "I1",
            "",
        );
        assert_eq!(
            authorize_handshake(&state, &empty, false).unwrap_err().0,
            "auth_required"
        );
        assert!(authorize_handshake(&state, &empty, true).is_ok());

        let bad = hs(
            "alice",
            "LucidTrading",
            "wss://example",
            "Live",
            "A1",
            "F1",
            "I1",
            "nope",
        );
        assert_eq!(
            authorize_handshake(&state, &bad, true).unwrap_err().0,
            "auth_failed"
        );

        state.expected_auth_token.clear();
        assert_eq!(
            authorize_handshake(&state, &good, true).unwrap_err().0,
            "auth_failed"
        );

        state.ready.store(false, Ordering::SeqCst);
        assert_eq!(
            authorize_handshake(&state, &empty, true).unwrap_err().0,
            "not_ready"
        );
    }

    #[test]
    fn a_request_refused_while_plants_rebuild_is_not_sent() {
        let frame = not_ready_frame(7);
        assert_eq!(frame.request_id, 7);
        let Some(Body::Error(e)) = frame.body else {
            panic!("expected an error frame");
        };
        assert_eq!(e.code, "not_sent");
    }
}
