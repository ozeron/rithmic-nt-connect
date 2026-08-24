"""Unit pins for spike_bracket_order proof classification and FarLimit domain."""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import pytest

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
    assert (
        spike._drain_basket_terminal(
            _DrainSession([{"basket_id": "B1", "status": "REJECTED"}]), "B1"
        )
        is True
    )
    # Operation-rejection tokens are not place-terminal.
    assert (
        spike._drain_basket_terminal(
            _DrainSession([{"basket_id": "B1", "status": "cancel_rejected"}]),
            "B1",
        )
        is False
    )


def test_unknown_status_is_not_working() -> None:
    assert (
        spike._drain_basket_working(
            _DrainSession([{"basket_id": "B1", "status": ""}]), "B1"
        )
        is False
    )
    assert (
        spike._drain_basket_working(
            _DrainSession([{"basket_id": "B1", "status": "mystery"}]), "B1"
        )
        is False
    )


def test_reject_inf_tick_and_prices() -> None:
    assert spike.resolve_tick_size("NQ", tick_size=float("inf"), front_raw=None) is None
    assert (
        spike.SizedBbo.from_event(
            {
                "type": "bbo",
                "bid_price": float("nan"),
                "ask_price": 102.0,
                "bid_size": 1,
                "ask_size": 1,
            }
        )
        is None
    )
    bbo = spike.SizedBbo(bid=101.0, ask=102.0)
    with pytest.raises(spike.ProofError) as ei:
        spike.FarLimit.override("Buy", float("inf"), bbo, tick=0.25, far_ticks=20)
    assert ei.value.outcome is spike.Outcome.REFUSED


def test_far_limit_refuses_non_positive_price() -> None:
    bbo = spike.SizedBbo(bid=101.0, ask=102.0)
    with pytest.raises(spike.ProofError) as ei:
        spike.FarLimit.derive("Buy", bbo, tick=0.25, far_ticks=500)
    assert ei.value.outcome is spike.Outcome.REFUSED
    with pytest.raises(spike.ProofError) as ei2:
        spike.FarLimit.override("Buy", 0.0, bbo, tick=0.25, far_ticks=20)
    assert ei2.value.outcome is spike.Outcome.REFUSED


def test_far_limit_refuses_off_tick() -> None:
    bbo = spike.SizedBbo(bid=101.0, ask=102.0)
    with pytest.raises(spike.ProofError) as ei:
        # 95.9 is far enough for N=20 but not on a 0.25 grid from 101.
        spike.FarLimit.override("Buy", 95.9, bbo, tick=0.25, far_ticks=20)
    assert ei.value.outcome is spike.Outcome.REFUSED


def test_survival_ack_rejects_modify_failed() -> None:
    assert (
        spike.is_survival_ack(
            {"notify_type_name": "MODIFY_RCVD_FROM_CLNT", "status": "", "text": ""}
        )
        is True
    )
    assert (
        spike.is_survival_ack(
            {"notify_type_name": "", "status": "Modification Failed", "text": ""}
        )
        is False
    )


def test_refuse_cli_domain() -> None:
    ns = argparse.Namespace(
        market_entry=False,
        limit_price=None,
        far_ticks=20,
        qty=1,
        stop_ticks=40,
        target_ticks=None,
        seconds=8.0,
        tick_size=None,
    )
    assert spike._refuse_cli(ns) is None
    ns.qty = 0
    assert spike._refuse_cli(ns) is not None
    ns.qty = 1
    ns.far_ticks = 0
    assert spike._refuse_cli(ns) is not None


def test_other_baskets_ignored() -> None:
    rows = [
        {"basket_id": "OTHER", "status": "OPEN"},
    ]
    assert spike._drain_basket_working(_DrainSession(rows), "B1") is False


def test_event_rejection_detection() -> None:
    assert spike.is_rejection({"status": "", "text": "order rejected"}) is True
    assert spike.is_rejection({"status": "Open", "text": "working order"}) is False


def test_event_is_bracket_path() -> None:
    assert (
        spike.is_bracket_path(
            {"notify_type_name": "MODIFY_RCVD_FROM_CLNT", "status": ""}
        )
        is True
    )
    assert (
        spike.is_bracket_path({"notify_type_name": "OPEN", "status": "open"}) is False
    )
    assert (
        spike.is_bracket_path({"notify_type_name": "", "status": "Modification Failed"})
        is True
    )


def test_far_limit_derive() -> None:
    bbo = spike.SizedBbo(bid=101.0, ask=102.0)
    far = spike.FarLimit.derive("Buy", bbo, tick=0.25, far_ticks=20)
    assert far.price == 96.0
    assert far.source == "derived"
    far_s = spike.FarLimit.derive("Sell", bbo, tick=0.25, far_ticks=20)
    assert far_s.price == 107.0


def test_far_limit_override_refuses_not_far() -> None:
    bbo = spike.SizedBbo(bid=101.0, ask=102.0)
    with pytest.raises(spike.ProofError) as ei:
        spike.FarLimit.override("Buy", 96.25, bbo, tick=0.25, far_ticks=20)
    assert ei.value.outcome is spike.Outcome.REFUSED


def test_derived_far_limit_is_not_marketable() -> None:
    bbo = spike.SizedBbo(bid=101.0, ask=102.0)
    buy = spike.FarLimit.derive("Buy", bbo, tick=0.25, far_ticks=20)
    sell = spike.FarLimit.derive("Sell", bbo, tick=0.25, far_ticks=20)
    assert spike.FarLimit._not_marketable(buy.side, buy.price, buy.bid, buy.ask)
    assert spike.FarLimit._not_marketable(sell.side, sell.price, sell.bid, sell.ask)
    assert spike.FarLimit._not_marketable("Buy", 102.0, 101.0, 102.0) is False


def test_sized_bbo_from_event_requires_size() -> None:
    assert (
        spike.SizedBbo.from_event(
            {
                "type": "bbo",
                "bid_price": 101.0,
                "ask_price": 102.0,
                "bid_size": 0,
                "ask_size": 5,
            }
        )
        is None
    )
    assert spike.SizedBbo.from_event(
        {
            "type": "bbo",
            "bid_price": 101.0,
            "ask_price": 102.0,
            "bid_size": 2,
            "ask_size": 4,
        }
    ) == spike.SizedBbo(bid=101.0, ask=102.0)


def test_resolve_tick_size() -> None:
    assert spike.resolve_tick_size("NQ", tick_size=None, front_raw=None) == 0.25
    assert spike.resolve_tick_size("CL", tick_size=None, front_raw=None) is None
    assert spike.resolve_tick_size("CL", tick_size=0.01, front_raw=None) == 0.01
    assert (
        spike.resolve_tick_size("CL", tick_size=None, front_raw={"tick_size": 0.01})
        == 0.01
    )


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


def test_cleaned_requires_terminal_via_proof_io() -> None:
    io = spike.ProofIO(_DrainSession([]), seconds=0.0, localid="")
    with pytest.raises(spike.ProofError) as ei:
        io.require_terminal("B1")
    assert ei.value.outcome is spike.Outcome.CLEANUP
