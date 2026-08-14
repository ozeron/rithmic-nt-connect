"""Calendar window slicing for gateway history RPCs.

Ports ``bar_slice_secs`` / ``window_slices`` from
``crates/rithmic-plants/src/history.rs`` so Python clients can issue bounded
unary ``load_time_bars`` RPCs (unix timeout + frame limits) without depending
on Nautilus or PyO3.
"""

from __future__ import annotations

from typing import Any

# Wire TimeBarType ints (same as rithmic_nt_connect.data / plants parse_time_bar_type).
BAR_TYPE_SECOND = 1
BAR_TYPE_MINUTE = 2
BAR_TYPE_DAILY = 3
BAR_TYPE_WEEKLY = 4

DEFAULT_TICK_SLICE_SECS = 15 * 60
DEFAULT_BAR_SLICE_SECS = 4 * 60 * 60


def bar_slice_secs(bar_type: int, period: int) -> int:
    """Slice length (seconds) for a time-bar replay window."""
    if bar_type in (BAR_TYPE_DAILY, BAR_TYPE_WEEKLY):
        return 2**30  # one wide window (matches Rust i32::MAX/4 order of magnitude)
    if bar_type == BAR_TYPE_MINUTE:
        if period >= 60:
            return 24 * 60 * 60
        if period >= 15:
            return 12 * 60 * 60
        return DEFAULT_BAR_SLICE_SECS
    if bar_type == BAR_TYPE_SECOND:
        return DEFAULT_TICK_SLICE_SECS
    return DEFAULT_BAR_SLICE_SECS


def window_slices(start: int, end: int, step_secs: int) -> list[tuple[int, int]]:
    """Inclusive ``[start, end]`` slices of at most ``step_secs``.

    Adjacent slices share the boundary second; callers must dedupe.
    """
    if start > end or step_secs < 1:
        return []
    out: list[tuple[int, int]] = []
    cur = start
    while cur <= end:
        nxt = cur + step_secs
        if nxt >= end or nxt < cur:  # overflow / past end
            nxt = end
        out.append((cur, nxt))
        if nxt >= end:
            break
        cur = nxt
    return out


def dedupe_bars_by_marker(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep first bar per ``marker`` (boundary overlap from adjacent slices)."""
    seen: set[Any] = set()
    out: list[dict[str, Any]] = []
    for bar in bars:
        key = bar.get("marker")
        if key is None:
            # Fall back to ts_event_ns when marker absent.
            key = ("ts", bar.get("ts_event_ns"))
        if key in seen:
            continue
        seen.add(key)
        out.append(bar)
    return out
