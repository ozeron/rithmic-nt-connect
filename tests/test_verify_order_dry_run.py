"""Unit tests for the order dry-run script's gating + trigger logic.

The live-place path itself is exercised only by the documented runbook
commands against Rithmic Test; these tests pin the decisions that must hold
before anything can connect: refusal gates and resting-side trigger math.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "verify_order_dry_run.py"
_spec = importlib.util.spec_from_file_location("verify_order_dry_run", _SCRIPT)
assert _spec is not None and _spec.loader is not None
dryrun = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("verify_order_dry_run", dryrun)
_spec.loader.exec_module(dryrun)


class _TickerSession:
    """Stub session: one last trade, then silence; records subscriptions."""

    def __init__(self, price: float) -> None:
        self.price = price
        self.subscribed: list[tuple[str, str]] = []
        self._polled = False

    def subscribe(self, symbol: str, exchange: str) -> None:
        self.subscribed.append((symbol, exchange))

    def poll_event(self):
        if self._polled:
            return None
        self._polled = True
        return {
            "type": "last_trade",
            "symbol": "NQU6",
            "trade_price": self.price,
            "trade_size": 1,
        }


def test_poll_market_px_returns_first_trade():
    session = _TickerSession(29678.25)

    px = dryrun._poll_market_px(session, "NQU6", "CME", 1.0)

    assert px == 29678.25
    assert session.subscribed == [("NQU6", "CME")]


def test_poll_market_px_empty_window_is_none_not_guess():
    # window=0 → the poll loop never runs; the caller must refuse, never
    # derive a stop trigger from a missing market.
    assert dryrun._poll_market_px(_TickerSession(29678.25), "NQU6", "CME", 0.0) is None


def test_derive_trigger_resting_side_both_sides():
    # CME rules (live-proven): SELL stop must sit below the last trade,
    # BUY stop above — otherwise the exchange rejects or it fires at once.
    assert dryrun._derive_trigger("SELL", 29678.25, 500.0) == pytest.approx(29178.25)
    assert dryrun._derive_trigger("BUY", 29678.25, 500.0) == pytest.approx(30178.25)


def test_live_place_refused_without_env_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RITHMIC_ENABLE_TRADING", raising=False)

    rc = dryrun.main(["--live-place"])

    assert rc == 3


def test_stop_market_refused_without_explicit_trigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RITHMIC_ENABLE_TRADING", "1")

    rc = dryrun.main(["--live-place", "--order-type", "STOP_MARKET"])

    assert rc == 3


def test_limit_still_requires_explicit_price(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RITHMIC_ENABLE_TRADING", "1")

    rc = dryrun.main(["--live-place"])

    assert rc == 3


def test_no_live_place_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RITHMIC_ENABLE_TRADING", "1")

    # --live-place + --no-live-place: the explicit refusal must win.
    rc = dryrun.main(["--live-place", "--no-live-place"])

    assert rc == 3
