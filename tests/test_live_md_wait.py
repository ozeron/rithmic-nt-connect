"""Unit tests for RC2.3 live-MD wait helpers."""

from __future__ import annotations

import pytest
from rithmic_gateway.live_md import wait_until_live_md_clear


def test_wait_until_live_md_clear_returns_when_quiet() -> None:
    calls = {"n": 0}

    def get_state() -> dict:
        calls["n"] += 1
        if calls["n"] < 3:
            return {"live_md": True, "ticker_intents": 1}
        return {"live_md": False, "ticker_intents": 0}

    out = wait_until_live_md_clear(get_state, timeout_sec=2.0, poll_sec=0.01)
    assert out["live_md"] is False
    assert calls["n"] >= 3


def test_wait_until_live_md_clear_timeout() -> None:
    with pytest.raises(TimeoutError, match="live MD intents"):
        wait_until_live_md_clear(
            lambda: {"live_md": True, "ticker_intents": 2},
            timeout_sec=0.05,
            poll_sec=0.01,
        )
