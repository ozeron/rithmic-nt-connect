//! Protobuf + framing codec: combines [`prost::Message`] encoding with
//! [`crate::framing`]'s length-delimited byte layer.

use prost::Message;
use thiserror::Error;

use crate::framing::{self, FrameError};

/// Errors from encoding or decoding a framed protobuf message.
#[derive(Debug, Error)]
pub enum CodecError {
    #[error(transparent)]
    Framing(#[from] FrameError),
    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),
}

/// Encode `msg` as protobuf, then wrap in a length-delimited frame.
pub fn encode<M: Message>(msg: &M) -> Result<Vec<u8>, CodecError> {
    let payload = msg.encode_to_vec();
    Ok(framing::encode_frame(&payload)?)
}

/// Decode one length-delimited protobuf frame from the front of `buf`.
///
/// Returns `(message, consumed)`; see [`framing::decode_frame`] for the
/// meaning of `consumed` and truncation semantics.
pub fn decode<M: Message + Default>(buf: &[u8]) -> Result<(M, usize), CodecError> {
    let (payload, consumed) = framing::decode_frame(buf)?;
    let msg = M::decode(payload)?;
    Ok((msg, consumed))
}
