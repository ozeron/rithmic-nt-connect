//! Length-delimited frame codec (u32 BE length + payload).

use thiserror::Error;

/// Maximum accepted payload length (16 MiB). Larger frames are rejected.
pub const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

/// Framing result.
pub type Result<T> = std::result::Result<T, FrameError>;

/// Errors from encode/decode.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum FrameError {
    /// Buffer shorter than 4-byte length prefix.
    #[error("truncated length prefix")]
    TruncatedLengthPrefix,

    /// Declared length exceeds [`MAX_FRAME_LEN`].
    #[error("frame too large: {0} bytes (max {MAX_FRAME_LEN})")]
    FrameTooLarge(u32),

    /// Buffer shorter than declared payload.
    #[error("truncated payload: need {need} bytes, have {have}")]
    TruncatedPayload { need: usize, have: usize },
}

/// Encode `payload` as length-delimited bytes (u32 BE + payload).
pub fn encode_frame(payload: &[u8]) -> Result<Vec<u8>> {
    let len = u32::try_from(payload.len()).map_err(|_| FrameError::FrameTooLarge(u32::MAX))?;
    if len > MAX_FRAME_LEN {
        return Err(FrameError::FrameTooLarge(len));
    }
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

/// Decode one length-delimited frame from the start of `buf`.
///
/// Returns `(payload, bytes_consumed)` where `bytes_consumed` includes the
/// 4-byte length prefix.
pub fn decode_frame(buf: &[u8]) -> Result<(&[u8], usize)> {
    if buf.len() < 4 {
        return Err(FrameError::TruncatedLengthPrefix);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if len > MAX_FRAME_LEN {
        return Err(FrameError::FrameTooLarge(len));
    }
    let need = len as usize;
    let rest = &buf[4..];
    if rest.len() < need {
        return Err(FrameError::TruncatedPayload {
            need,
            have: rest.len(),
        });
    }
    Ok((&rest[..need], 4 + need))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_empty() {
        let enc = encode_frame(&[]).unwrap();
        assert_eq!(enc, [0, 0, 0, 0]);
        let (payload, n) = decode_frame(&enc).unwrap();
        assert!(payload.is_empty());
        assert_eq!(n, 4);
    }

    #[test]
    fn round_trip_bytes() {
        let body = b"hello-gateway";
        let enc = encode_frame(body).unwrap();
        let (payload, n) = decode_frame(&enc).unwrap();
        assert_eq!(payload, body);
        assert_eq!(n, 4 + body.len());
    }
}
