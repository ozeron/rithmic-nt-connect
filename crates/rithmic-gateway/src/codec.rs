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

/// Encode `msg` as protobuf inside a length-delimited frame, with one
/// allocation: the buffer is sized for prefix plus payload and the message is
/// written straight after the length.
pub fn encode<M: Message>(msg: &M) -> Result<Vec<u8>, CodecError> {
    let len = msg.encoded_len();
    let framed_len = u32::try_from(len).map_err(|_| FrameError::FrameTooLarge(u32::MAX))?;
    if framed_len > framing::MAX_FRAME_LEN {
        return Err(FrameError::FrameTooLarge(framed_len).into());
    }
    let mut out = Vec::with_capacity(4 + len);
    out.extend_from_slice(&framed_len.to_be_bytes());
    // The buffer was sized from `encoded_len`, so this cannot run out of room.
    msg.encode(&mut out)
        .map_err(|_| FrameError::FrameTooLarge(framed_len))?;
    Ok(out)
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
