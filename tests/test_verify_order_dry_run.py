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


def test_stop_market_refused_on_negative_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A non-positive offset derives a marketable stop (fires immediately).
    monkeypatch.setenv("RITHMIC_ENABLE_TRADING", "1")

    rc = dryrun.main(
        [
            "--live-place",
            "--order-type",
            "STOP_MARKET",
            "--auto-trigger-offset",
            "-1",
        ]
    )

    assert rc == 3


def test_stop_market_refused_on_nan_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RITHMIC_ENABLE_TRADING", "1")

    rc = dryrun.main(
        [
            "--live-place",
            "--order-type",
            "STOP_MARKET",
            "--auto-trigger-offset",
            "nan",
        ]
    )

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


# --------------------------------------------------------------------------- #
# Root-cause pins (Macroscope Highs on PR #31)
# --------------------------------------------------------------------------- #


def test_derive_trigger_rejects_market_side_offsets() -> None:
    """High 1: the invariant lives in _derive_trigger itself — zero,
    negative, and non-finite offsets must raise, not derive a marketable
    stop. Argparse gating alone would leave every other caller unsafe."""
    for bad in (0.0, -1.0, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            dryrun._derive_trigger("SELL", 29678.25, bad)
        with pytest.raises(ValueError):
            dryrun._derive_trigger("BUY", 29678.25, bad)


def test_resting_side_rule_has_one_source_of_truth() -> None:
    """High 2: help texts must state the CME resting-stop rule exactly as
    _derive_trigger implements it (SELL below / BUY above), and no doc may
    carry the inverted LIMIT rule onto stops."""
    help_text = dryrun._build_parser().format_help()

    # Both stop-related help strings are composed from the same constants
    # the trigger derivation implements.
    assert dryrun._RESTING_SELL_RULE in help_text
    assert dryrun._RESTING_BUY_RULE in help_text
    assert "SELL far above / BUY far below" not in help_text
    # And the implementation agrees with the stated rule.
    assert dryrun._derive_trigger("SELL", 100.0, 10.0) == pytest.approx(90.0)
    assert dryrun._derive_trigger("BUY", 100.0, 10.0) == pytest.approx(110.0)


def test_module_docs_never_invert_the_stop_rule() -> None:
    """The docstring example previously said 'SELL trigger above / BUY below'
    — the inverted LIMIT rule that makes an explicit stop marketable."""
    doc = dryrun.__doc__ or ""
    assert "SELL trigger above" not in doc
    assert "BUY trigger below" not in doc


def test_our_baskets_attributed_by_identity_not_price() -> None:
    """High 3: basket attribution is identity-only — user_tag hits claim a
    basket; later tag-less rows of the same basket inherit it; unknown
    baskets are never claimed (price matching is gone entirely)."""
    events = [
        {"basket_id": "B1", "user_tag": "smoke"},  # identity row
        {"basket_id": "B1", "user_tag": None},  # anonymous same-basket row
        {"basket_id": "B2", "user_tag": None},  # someone else's order
        {"basket_id": "B3", "user_tag": "other"},
    ]
    assert dryrun._our_baskets(events, "smoke") == {"B1"}
    # No identifying row at all: attribute nothing (fail-safe), regardless
    # of any price fields present.
    price_only = [
        {"basket_id": "B9", "user_tag": None, "price": 21000.0},
    ]
    assert dryrun._our_baskets(price_only, "smoke") == set()
