//! Minimal accept loop for unix listeners (handshake + Ready + gated RPCs).

use std::sync::Arc;

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::framing::encode_frame;
use crate::pb::{frame::Body, Frame, Handshake, Ready};
use crate::subscriptions::{FanoutHub, ParentGates, SharedFanout};

/// Shared gateway runtime state.
pub struct GatewayState {
    pub gates: ParentGates,
    pub hub: SharedFanout,
    pub fingerprint: Fingerprint,
    pub ready: bool,
}

/// Credential fingerprint advertised/checked at Handshake.
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
            && (hs.env.is_empty() || hs.env == self.env)
    }
}

/// Serve forever on `listener`.
pub async fn serve(listener: UnixListener, state: Arc<GatewayState>) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, state).await {
                eprintln!("gateway client error: {e}");
            }
        });
    }
}

async fn handle_client(mut stream: UnixStream, state: Arc<GatewayState>) -> Result<(), String> {
    // Read one framed Handshake.
    let mut buf = vec![0u8; 4];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| format!("read len: {e}"))?;
    let mut len_buf = [0u8; 4];
    len_buf.copy_from_slice(&buf);
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > crate::framing::MAX_FRAME_LEN as usize {
        return Err("frame too large".into());
    }
    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|e| format!("read payload: {e}"))?;

    let frame = Frame::decode(payload.as_slice()).map_err(|e| format!("proto: {e}"))?;
    let hs = match frame.body {
        Some(Body::Handshake(hs)) => hs,
        _ => return Err("first frame must be Handshake".into()),
    };
    if !state.fingerprint.matches(&hs) {
        return Err("fingerprint mismatch".into());
    }
    // v1 local: empty auth_token accepted on unix.
    if !state.ready {
        return Err("gateway not Ready (plants not connected)".into());
    }

    let ready = Ready {
        scopes: state.gates.scopes(),
        trading_enabled: state.gates.trading_enabled,
        cancel_all_enabled: state.gates.cancel_all_enabled,
    };
    write_frame(
        &mut stream,
        &Frame {
            request_id: 0,
            body: Some(Body::Ready(ready)),
        },
    )
    .await?;

    // Request loop — gates only for place / cancel_all in this MVP.
    let mut len_bytes = [0u8; 4];
    loop {
        match stream.read_exact(&mut len_bytes).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(format!("read: {e}")),
        }
        let len = u32::from_be_bytes(len_bytes) as usize;
        let mut payload = vec![0u8; len];
        stream
            .read_exact(&mut payload)
            .await
            .map_err(|e| format!("read body: {e}"))?;
        let req = Frame::decode(payload.as_slice()).map_err(|e| format!("proto: {e}"))?;
        let resp = handle_rpc(&state, &req);
        write_frame(&mut stream, &resp).await?;
    }
}

fn handle_rpc(state: &GatewayState, req: &Frame) -> Frame {
    let request_id = req.request_id;
    match &req.body {
        Some(Body::PlaceOrder(_)) if !state.gates.allow_place() => Frame {
            request_id,
            body: Some(Body::Error(crate::pb::ErrorResponse {
                code: "trading_disabled".into(),
                message: "place_order denied: parent trading disabled".into(),
            })),
        },
        Some(Body::CancelAllOrders(_)) if !state.gates.allow_cancel_all() => Frame {
            request_id,
            body: Some(Body::Error(crate::pb::ErrorResponse {
                code: "cancel_all_denied".into(),
                message: "cancel_all_orders denied: parent cancel_all disabled".into(),
            })),
        },
        Some(Body::PlaceOrder(_)) => Frame {
            request_id,
            body: Some(Body::Ack(crate::pb::Ack {})),
        },
        Some(Body::CancelAllOrders(_)) => Frame {
            request_id,
            body: Some(Body::Ack(crate::pb::Ack {})),
        },
        Some(Body::Disconnect(_)) => Frame {
            request_id,
            body: Some(Body::Ack(crate::pb::Ack {})),
        },
        _ => Frame {
            request_id,
            body: Some(Body::Ack(crate::pb::Ack {})),
        },
    }
}

async fn write_frame(stream: &mut UnixStream, frame: &Frame) -> Result<(), String> {
    let payload = frame.encode_to_vec();
    let wire = encode_frame(&payload).map_err(|e| e.to_string())?;
    stream
        .write_all(&wire)
        .await
        .map_err(|e| format!("write: {e}"))?;
    Ok(())
}

/// Helper for tests: process one RPC against gates without a socket.
pub fn gate_rpc_for_test(gates: &ParentGates, body: Body) -> Body {
    let state = GatewayState {
        gates: *gates,
        hub: Arc::new(FanoutHub::new(8)),
        fingerprint: Fingerprint {
            user: String::new(),
            system_name: String::new(),
            url: String::new(),
            env: String::new(),
            account_id: String::new(),
            fcm_id: String::new(),
            ib_id: String::new(),
        },
        ready: true,
    };
    let frame = Frame {
        request_id: 1,
        body: Some(body),
    };
    handle_rpc(&state, &frame).body.expect("body")
}
