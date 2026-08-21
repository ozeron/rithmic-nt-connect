//! Accept loop: Handshake + Ready, gated RPC dispatch backed by
//! [`rithmic_plants::RithmicSession`], and per-client fan-out of pushed
//! events. Client disconnect is detach-only (refcount decrement) while peers
//! remain; optional idle-exit shuts down the parent after the last Ready
//! peer leaves (see [`crate::idle_exit`]).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{broadcast, mpsc, Mutex as TokioMutex};
use tokio::task::JoinHandle;

use rithmic_plants::RithmicSession;

use crate::codec;
use crate::idle_exit::{IdleExit, IdleExitPolicy};
use crate::pb::{self, frame::Body, Frame, Handshake, Ready};
use crate::reconnect::{ReconnectController, TimeBarIntent};
use crate::subscriptions::{ClientId, FanoutHub, ParentGates, SharedFanout, SubKey};

mod dispatch;
pub mod pump;
use dispatch::TopicIntent;
pub use dispatch::{gate_rpc_for_test, rpc_sequence_with_gates};

/// Shared gateway runtime state, held for the process lifetime.
pub struct GatewayState {
    pub gates: ParentGates,
    pub hub: SharedFanout,
    pub fingerprint: Fingerprint,
    /// Cleared while plants reconnect so new Handshakes get `not_ready`.
    pub ready: AtomicBool,
    /// Set when a plant must be rebuilt outside the poll path (e.g. failed
    /// order-plant reseat). Event pump treats this like a connection issue.
    pub force_reconnect: AtomicBool,
    /// Parent secret for non-empty Handshake `auth_token` (from
    /// `RITHMIC_GATEWAY_AUTH_TOKEN`). Empty means only same-UID empty-token
    /// peers are accepted.
    pub expected_auth_token: String,
    /// `None` only in gate / RPC-sequence unit tests without plants.
    /// Plant RPCs return `Error(no_session)` — never Ack / empty success.
    pub session: Option<Arc<TokioMutex<RithmicSession>>>,
    pub reconnect: Arc<ReconnectController>,
    /// Serialize first venue-subscribe per topic so a failing peer cannot
    /// leave concurrent Ack'd clients without a live venue join.
    pub topic_locks: TokioMutex<HashMap<SubKey, Arc<TokioMutex<()>>>>,
    /// Serialize the complete `show_orders` + bounded-drain reconciliation so
    /// concurrent `LoadOrders` requests cannot interleave their replays on the
    /// shared order-updates subscription channel (each drain would otherwise
    /// observe the other's working-order replay and return a mixed snapshot).
    /// Kept separate from the session lock so the drain can run without
    /// blocking the event pump.
    pub recon_lock: Arc<TokioMutex<()>>,
    /// Ready peer count + optional idle-exit after last client.
    pub idle: IdleExit,
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

/// Accept connections until idle-exit fires (or forever when policy is Never).
pub async fn serve(listener: UnixListener, state: Arc<GatewayState>) -> std::io::Result<()> {
    loop {
        tokio::select! {
            biased;
            _ = state.idle.wait_until_should_exit() => {
                match state.idle.policy() {
                    IdleExitPolicy::After(grace) => {
                        eprintln!(
                            "rithmic-gateway: idle-exit after {}s with 0 peers",
                            grace.as_secs()
                        );
                    }
                    IdleExitPolicy::Never => {
                        eprintln!("rithmic-gateway: idle-exit");
                    }
                }
                return Ok(());
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, state).await {
                        eprintln!("gateway client error: {e}");
                    }
                });
            }
        }
    }
}

async fn read_frame_bytes(stream: &mut UnixStream) -> std::io::Result<Option<Vec<u8>>> {
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
    if len > crate::framing::MAX_FRAME_LEN as usize {
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

async fn write_frame(stream: &mut UnixStream, frame: &Frame) -> Result<(), String> {
    let wire = codec::encode(frame).map_err(|e| e.to_string())?;
    tokio::time::timeout(Duration::from_secs(5), stream.write_all(&wire))
        .await
        .map_err(|_| "write timeout".to_string())?
        .map_err(|e| format!("write: {e}"))
}

/// Same-UID check for the v1 local-unix empty-token trust policy (R15 /
/// KTD16): unix `SO_PEERCRED` is a kernel fact, not client-supplied data.
fn same_uid_peer(stream: &UnixStream) -> bool {
    match stream.peer_cred() {
        Ok(cred) => {
            // SAFETY: getuid() has no preconditions and never fails.
            let our_uid = unsafe { libc::getuid() };
            cred.uid() == our_uid
        }
        Err(_) => false,
    }
}

pub(super) enum OutMsg {
    Frame(Bytes),
    Overflow,
}

/// Per-connection fan-out bookkeeping: which topics this client forwards,
/// typed venue joins this client asked for, and forwarder tasks.
pub(super) struct ClientCtx {
    #[allow(dead_code)]
    id: ClientId,
    subscribed: HashSet<SubKey>,
    ticker: HashSet<SubKey>,
    book: HashSet<SubKey>,
    time_bars: HashSet<TimeBarIntent>,
    pnl: bool,
    order: bool,
    brackets: bool,
    forwarders: HashMap<SubKey, JoinHandle<()>>,
    out_tx: mpsc::Sender<OutMsg>,
}

impl ClientCtx {
    fn new(out_tx: mpsc::Sender<OutMsg>) -> Self {
        Self {
            id: ClientId::new(),
            subscribed: HashSet::new(),
            ticker: HashSet::new(),
            book: HashSet::new(),
            time_bars: HashSet::new(),
            pnl: false,
            order: false,
            brackets: false,
            forwarders: HashMap::new(),
            out_tx,
        }
    }

    fn wants_md(&self, key: &SubKey) -> bool {
        self.ticker.contains(key)
            || self.book.contains(key)
            || self
                .time_bars
                .iter()
                .any(|tb| tb.symbol == key.symbol && tb.exchange == key.exchange)
    }

    /// Attach this client to `key`'s fan-out topic (idempotent). The topic
    /// must already exist in the hub (caller subscribes hub interest first).
    async fn ensure_forwarder(&mut self, hub: &FanoutHub, key: &SubKey) {
        if self.subscribed.contains(key) {
            return;
        }
        if let Some(rx) = hub.subscribe_receiver(key).await {
            let handle = spawn_forwarder(rx, self.out_tx.clone());
            self.forwarders.insert(key.clone(), handle);
            self.subscribed.insert(key.clone());
        }
    }

    fn drop_forwarder(&mut self, key: &SubKey) {
        if let Some(handle) = self.forwarders.remove(key) {
            handle.abort();
        }
        self.subscribed.remove(key);
    }

    /// Forget one typed intent and leave the venue topic when this was the
    /// last peer. Shared by all MD teardown drains via [`TopicIntent`].
    async fn teardown_topic(state: &GatewayState, intent: dispatch::TopicIntent) {
        let key = intent.key();
        let lock = dispatch::topic_lock(state, &key).await;
        let _guard = lock.lock().await;
        if intent.forget(&state.reconnect).await {
            if let Some(session) = &state.session {
                let guard = session.lock().await;
                let _ = intent.venue_leave(&guard).await;
            }
        }
    }

    /// Detach-only disconnect: clear typed intents + hub interest; venue
    /// unsubscribe only when the last peer of that typed join leaves.
    async fn teardown(&mut self, state: &GatewayState) {
        for handle in self.forwarders.values() {
            handle.abort();
        }
        self.forwarders.clear();

        // Fire-and-forget per topic: the client is dying, so unlike
        // `unsubscribe_topic` there is no re-note on failure. The topic_lock
        // is still held across forget + venue leave so a concurrent
        // first-peer subscribe cannot have its join undone.
        for key in self.ticker.drain() {
            Self::teardown_topic(state, TopicIntent::Ticker(key)).await;
        }
        for key in self.book.drain() {
            Self::teardown_topic(state, TopicIntent::Book(key)).await;
        }
        for intent in self.time_bars.drain() {
            Self::teardown_topic(state, TopicIntent::TimeBars(intent)).await;
        }
        self.teardown_pnl(state).await;
        self.teardown_order_plants(state).await;

        for key in self.subscribed.drain() {
            let _ = state.reconnect.remove_hub_interest(&key).await;
        }
    }

    async fn teardown_pnl(&mut self, state: &GatewayState) {
        if !self.pnl {
            return;
        }
        self.pnl = false;
        if state.reconnect.forget_pnl().await {
            if let Some(session) = &state.session {
                let mut guard = session.lock().await;
                let _ = guard.disconnect_pnl_plant().await;
            }
        }
    }

    /// Order-plant teardown: brackets and order share one plant, so the last
    /// peer of either drives the disconnect; when only brackets peers leave,
    /// re-seat order-only to drop venue brackets (no brackets unsubscribe API).
    async fn teardown_order_plants(&mut self, state: &GatewayState) {
        if !self.brackets && !self.order {
            return;
        }
        if self.brackets {
            let key = crate::convert::order_key();
            let lock = dispatch::topic_lock(state, &key).await;
            let _guard = lock.lock().await;
            let last_brackets = dispatch::clear_client_brackets(state, self).await;
            let last_order = if self.order {
                self.order = false;
                state.reconnect.forget_order().await
            } else {
                false
            };
            if last_order {
                if let Some(session) = &state.session {
                    let mut guard = session.lock().await;
                    let _ = guard.disconnect_order_plant().await;
                }
            } else if last_brackets && state.reconnect.restore_plan().await.order {
                if let Err(e) = dispatch::reseat_order_plant_without_brackets(state).await {
                    eprintln!("rithmic-gateway: teardown reseat failed: {e}");
                    state.ready.store(false, Ordering::SeqCst);
                    state.force_reconnect.store(true, Ordering::SeqCst);
                }
            }
        } else {
            self.order = false;
            if state.reconnect.forget_order().await {
                if let Some(session) = &state.session {
                    let mut guard = session.lock().await;
                    let _ = guard.disconnect_order_plant().await;
                }
            }
        }
    }
}

fn spawn_forwarder(rx: broadcast::Receiver<Bytes>, out_tx: mpsc::Sender<OutMsg>) -> JoinHandle<()> {
    let mut queue = crate::subscriptions::ClientQueue::from_receiver(ClientId::new(), rx);
    tokio::spawn(async move {
        loop {
            match queue.recv().await {
                Ok(bytes) => {
                    if out_tx.send(OutMsg::Frame(bytes)).await.is_err() {
                        return;
                    }
                }
                Err(crate::subscriptions::ClientQueueError::Overflow) => {
                    let _ = out_tx.send(OutMsg::Overflow).await;
                    return;
                }
                Err(crate::subscriptions::ClientQueueError::Closed) => return,
            }
        }
    })
}

/// Encode a plant event as a wire frame and publish it to `key`'s topic.
pub async fn publish_event(hub: &FanoutHub, key: &SubKey, event: pb::Event) -> usize {
    let frame = Frame {
        request_id: 0,
        body: Some(Body::Event(event)),
    };
    let wire = match codec::encode(&frame) {
        Ok(w) => w,
        Err(_) => return 0,
    };
    hub.publish(key, Bytes::from(wire)).await
}

async fn handle_client(mut stream: UnixStream, state: Arc<GatewayState>) -> Result<(), String> {
    let same_uid = same_uid_peer(&stream);

    let payload = read_frame_bytes(&mut stream)
        .await
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

    write_frame(
        &mut stream,
        &Frame {
            request_id: 0,
            body: Some(Body::Ready(Ready {
                scopes: state.gates.scopes(),
                trading_enabled: state.gates.trading_enabled,
                cancel_all_enabled: state.gates.cancel_all_enabled,
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

    let (out_tx, mut out_rx) = mpsc::channel::<OutMsg>(crate::subscriptions::DEFAULT_QUEUE_CAP);
    let mut client = ClientCtx::new(out_tx);

    let loop_result: Result<(), String> = loop {
        tokio::select! {
            biased;
            frame = read_frame_bytes(&mut stream) => {
                match frame {
                    Ok(Some(payload)) => {
                        let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
                        framed.extend_from_slice(&payload);
                        let req = match codec::decode::<Frame>(&framed) {
                            Ok((f, _)) => f,
                            Err(e) => break Err(format!("proto: {e}")),
                        };
                        let request_id = req.request_id;
                        let Some(body) = req.body else { continue };
                        let close_after = matches!(body, Body::Disconnect(_));
                        // During plant reconnect, refuse new RPCs (except
                        // detach) so callers do not observe Ack against a
                        // half-rebuilt venue session.
                        let resp = if !close_after && !state.ready.load(Ordering::SeqCst) {
                            dispatch::error_frame(
                                request_id,
                                "not_ready",
                                "gateway not Ready (plants reconnecting)",
                            )
                        } else {
                            dispatch::dispatch(&state, &mut client, request_id, body).await
                        };
                        if write_frame(&mut stream, &resp).await.is_err() {
                            break Ok(());
                        }
                        if close_after {
                            break Ok(());
                        }
                    }
                    Ok(None) => break Ok(()),
                    Err(e) => break Err(format!("read: {e}")),
                }
            }
            msg = out_rx.recv() => {
                match msg {
                    Some(OutMsg::Frame(bytes)) => {
                        match tokio::time::timeout(Duration::from_secs(5), stream.write_all(&bytes))
                            .await
                        {
                            Ok(Ok(())) => {}
                            _ => break Ok(()), // slow / dead client
                        }
                    }
                    Some(OutMsg::Overflow) => {
                        eprintln!("gateway: client overflowed its outbound queue; disconnecting");
                        break Ok(());
                    }
                    None => {}
                }
            }
        }
    };

    client.teardown(&state).await;
    state.idle.peer_detached().await;
    loop_result
}

/// Rebroadcast a plant event to all interested fan-out subscribers, keyed by
/// symbol/exchange (or the internal pnl/order sentinel keys). Also feeds the
/// [`ReconnectController`]-visible hub so late subscribers still see the
/// venue-declared topic.
pub async fn publish_plant_event(hub: &FanoutHub, event: rithmic_plants::dto::PlantEvent) {
    let (key, wire_event) = crate::convert::plant_event_to_routed(event);
    publish_event(hub, &key, wire_event).await;
}

#[cfg(test)]
mod auth_tests {
    use super::*;
    use crate::pb::Handshake;

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
        let hub: SharedFanout = Arc::new(FanoutHub::new(8));
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
}
