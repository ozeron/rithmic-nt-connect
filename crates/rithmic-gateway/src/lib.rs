//! Transport-agnostic length-delimited protobuf framing for the Rithmic gateway.
//!
//! Wire format: big-endian u32 length prefix + payload bytes.
//! Payloads are typically prost-encoded [`pb::Frame`] messages (see proto).

#![allow(missing_docs)]

pub mod codec;
pub mod convert;
pub mod framing;
pub mod idle_exit;
pub mod listen;
pub mod reconnect;
pub mod runtime_dir;
pub mod server;
pub mod singleton;
pub mod subscriptions;

/// Generated protobuf types from `proto/rithmic_gateway/v1/session.proto`.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/rithmic_gateway.v1.rs"));
}

pub use framing::{decode_frame, encode_frame, FrameError, Result as FrameResult, MAX_FRAME_LEN};

/// Re-export plant façade for gateway consumers.
pub use rithmic_plants;
