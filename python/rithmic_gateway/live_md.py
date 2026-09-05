"""Wait helpers for RC2.3 live-MD barrier (deploy / ops)."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any


def wait_until_live_md_clear(
    get_state: Callable[[], dict[str, Any]],
    *,
    timeout_sec: float = 60.0,
    poll_sec: float = 1.0,
) -> dict[str, Any]:
    """Poll ``get_live_md_state`` until ``live_md`` is false or timeout.

    Raises ``TimeoutError`` with the last state snapshot when the deadline
    elapses while intents remain.
    """
    deadline = time.monotonic() + max(0.0, float(timeout_sec))
    last: dict[str, Any] = {}
    while True:
        last = dict(get_state() or {})
        if not bool(last.get("live_md")):
            return last
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"gateway still has live MD intents after {timeout_sec:.0f}s: {last}"
            )
        time.sleep(max(0.1, float(poll_sec)))
