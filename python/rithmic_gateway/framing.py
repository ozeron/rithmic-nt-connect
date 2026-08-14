"""u32 BE length-delimited framing (mirrors Rust ``rithmic_gateway::framing``)."""

from __future__ import annotations

MAX_FRAME_LEN = 16 * 1024 * 1024


class FrameError(ValueError):
    """Framing / length-prefix error."""


def encode_frame(payload: bytes) -> bytes:
    length = len(payload)
    if length > MAX_FRAME_LEN:
        raise FrameError(f"frame too large: {length} bytes (max {MAX_FRAME_LEN})")
    return length.to_bytes(4, "big") + payload


def decode_frame(buf: bytes) -> tuple[bytes, int]:
    """Return ``(payload, bytes_consumed)`` including the 4-byte length prefix."""
    if len(buf) < 4:
        raise FrameError("truncated length prefix")
    length = int.from_bytes(buf[:4], "big")
    if length > MAX_FRAME_LEN:
        raise FrameError(f"frame too large: {length} bytes (max {MAX_FRAME_LEN})")
    need = length
    rest = buf[4:]
    if len(rest) < need:
        raise FrameError(f"truncated payload: need {need} bytes, have {len(rest)}")
    return rest[:need], 4 + need
