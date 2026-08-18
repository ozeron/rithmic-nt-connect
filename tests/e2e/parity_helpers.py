"""Shared live helpers for the INTERNAL/EXTERNAL parity e2e tests (TC-D53/D54)."""

from __future__ import annotations

import time

from nautilus_trader.model.data import Bar, BarType
from rithmic_nt_connect.historical import load_time_bars

NS_PER_MIN = 60_000_000_000


def open_minute(ts_ns: int) -> int:
    """Floor a tick/bar timestamp to its open-minute grid (ns)."""
    return int(ts_ns) - (int(ts_ns) % NS_PER_MIN)


def wait_for_external_bar(
    session,
    instrument,
    bar_type: BarType,
    minute_ns: int,
    *,
    window_secs: int = 600,
    deadline_secs: float = 300.0,
) -> Bar | None:
    """Poll the EXTERNAL replay until the bar for ``minute_ns`` is published.

    The test-plant replay emits bars in delayed, irregular batches (observed
    8s..250s+ after close) and omits minutes with no trades, so poll patiently
    and return ``None`` when the deadline passes.
    """
    minute_sec = minute_ns // 1_000_000_000
    deadline = time.monotonic() + deadline_secs
    while time.monotonic() < deadline:
        bars = load_time_bars(
            session,
            instrument,
            minute_sec - window_secs,
            minute_sec + 60,
            bar_type,
        )
        bar = next((b for b in bars if int(b.ts_event) == minute_ns), None)
        if bar is not None:
            return bar
        time.sleep(15)
    return None
