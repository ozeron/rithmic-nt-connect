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
    """High-fix pin: 'cancel rejected' keeps the basket WORKING."""
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
    """An OPEN row arriving before its terminal row must not mask closure."""
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
    """Empty drain must not print CLEANUP OK (propagation lag ≠ canceled)."""
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
