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
