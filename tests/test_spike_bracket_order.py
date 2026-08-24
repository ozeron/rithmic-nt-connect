"""Unit pins for spike_bracket_order's drain-row / event classification.

The review High that motivated this file: substring matching classified a
``cancel rejected`` drain row as closed, so cleanup reported success while
the bracket was still live at the venue.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "spike_bracket_order.py"
_spec = importlib.util.spec_from_file_location("spike_bracket_order", _SCRIPT)
assert _spec is not None and _spec.loader is not None
spike = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("spike_bracket_order", spike)
_spec.loader.exec_module(spike)


class _DrainSession:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def load_orders(self, start: int, end: int) -> list[dict]:
        return self._rows


def test_reject_text_is_not_terminal() -> None:
    rows = [
        {"basket_id": "B1", "status": "OPEN", "text": "cancel rejected"},
    ]
    assert spike._drain_basket_working(_DrainSession(rows), "B1") is True


def test_explicit_terminal_status_closes() -> None:
    rows = [
        {"basket_id": "B1", "status": "CANCELED", "text": ""},
    ]
    assert spike._drain_basket_working(_DrainSession(rows), "B1") is False


def test_latest_row_wins_over_stale_open() -> None:
    rows = [
        {"basket_id": "B1", "status": "OPEN", "ssboe": 100},
        {"basket_id": "B1", "status": "COMPLETE", "ssboe": 200},
    ]
    assert spike._drain_basket_working(_DrainSession(rows), "B1") is False


def test_latest_row_by_ts_event_ns() -> None:
    rows = [
        {"basket_id": "B1", "status": "COMPLETE", "ts_event_ns": 5},
        {"basket_id": "B1", "status": "OPEN", "ts_event_ns": 9},
    ]
    assert spike._drain_basket_working(_DrainSession(rows), "B1") is True


def test_no_rows_is_not_working() -> None:
    assert spike._drain_basket_working(_DrainSession([]), "B1") is False


def test_cleanup_requires_explicit_terminal_row() -> None:
    assert spike._drain_basket_terminal(_DrainSession([]), "B1") is False
    assert (
        spike._drain_basket_terminal(
            _DrainSession([{"basket_id": "B1", "status": "OPEN"}]), "B1"
        )
        is False
    )
    assert (
        spike._drain_basket_terminal(
            _DrainSession([{"basket_id": "B1", "status": "COMPLETE"}]), "B1"
        )
        is True
    )


def test_other_baskets_ignored() -> None:
    rows = [
        {"basket_id": "OTHER", "status": "OPEN"},
    ]
    assert spike._drain_basket_working(_DrainSession(rows), "B1") is False


def test_event_rejection_detection() -> None:
    assert spike._event_is_rejection({"status": "", "text": "order rejected"}) is True
    assert (
        spike._event_is_rejection({"status": "Open", "text": "working order"}) is False
    )


def test_event_is_bracket_path() -> None:
    assert (
        spike._event_is_bracket_path(
            {"notify_type_name": "MODIFY_RCVD_FROM_CLNT", "status": ""}
        )
        is True
    )
    assert (
        spike._event_is_bracket_path({"notify_type_name": "OPEN", "status": "open"})
        is False
    )
    assert (
        spike._event_is_bracket_path(
            {"notify_type_name": "", "status": "Modification Failed"}
        )
        is True
    )


def test_derive_far_limit() -> None:
    # bid=101 ask=102 tick=0.25 far=20 → BUY=96.0 SELL=107.0
    assert spike._derive_far_limit("Buy", 101.0, 102.0, 0.25, 20) == 96.0
    assert spike._derive_far_limit("Sell", 101.0, 102.0, 0.25, 20) == 107.0


def test_limit_is_far_enough() -> None:
    bid, ask, tick, n = 101.0, 102.0, 0.25, 20
    assert spike._limit_is_far_enough("Buy", 96.0, bid, ask, tick, n) is True
    assert spike._limit_is_far_enough("Buy", 96.25, bid, ask, tick, n) is False
    assert spike._limit_is_far_enough("Sell", 107.0, bid, ask, tick, n) is True
    assert spike._limit_is_far_enough("Sell", 106.75, bid, ask, tick, n) is False


def test_limit_not_marketable_defense() -> None:
    assert spike._limit_not_marketable("Buy", 100.0, bid=101.0, ask=102.0) is True
    assert spike._limit_not_marketable("Buy", 102.0, bid=101.0, ask=102.0) is False
    assert spike._limit_not_marketable("Sell", 103.0, bid=101.0, ask=102.0) is True
    assert spike._limit_not_marketable("Sell", 101.0, bid=101.0, ask=102.0) is False


def test_resolve_tick_size() -> None:
    assert spike._resolve_tick_size("NQ", tick_size=None, front_raw=None) == 0.25
    assert spike._resolve_tick_size("CL", tick_size=None, front_raw=None) is None
    assert spike._resolve_tick_size("CL", tick_size=0.01, front_raw=None) == 0.01
    assert (
        spike._resolve_tick_size("CL", tick_size=None, front_raw={"tick_size": 0.01})
        == 0.01
    )


def test_size_ok() -> None:
    assert spike._size_ok(1) is True
    assert spike._size_ok(0) is False
    assert spike._size_ok(None) is False
    assert spike._size_ok("x") is False


def test_wait_bbo_requires_size() -> None:
    class _Sess:
        def __init__(self) -> None:
            self._events = [
                {
                    "type": "bbo",
                    "bid_price": 101.0,
                    "ask_price": 102.0,
                    "bid_size": 0,
                    "ask_size": 5,
                },
                {
                    "type": "bbo",
                    "bid_price": 101.0,
                    "ask_price": 102.0,
                    "bid_size": 3,
                    "ask_size": 0,
                },
                {
                    "type": "bbo",
                    "bid_price": 101.0,
                    "ask_price": 102.0,
                    "bid_size": 2,
                    "ask_size": 4,
                },
            ]
            self._i = 0

        def subscribe(self, *_a, **_k) -> None:
            return None

        def unsubscribe(self, *_a, **_k) -> None:
            return None

        def poll_event(self):
            if self._i >= len(self._events):
                return None
            ev = self._events[self._i]
            self._i += 1
            return ev

    assert spike._wait_bbo(_Sess(), "NQU6", "CME", seconds=1.0) == (101.0, 102.0)
