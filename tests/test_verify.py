"""Unit tests for front-month resolve + live/history verify compare."""

from __future__ import annotations

import json
from pathlib import Path

from _stubs import WireSessionStub
from rithmic_nt_connect.front_month import FrontMonthError, resolve_front_month
from rithmic_nt_connect.verify import (
    RecordedTick,
    compare_ticks,
    run_front_month_verify,
)


class FakeSession(WireSessionStub):
    def __init__(self) -> None:
        self._events = [
            {
                "type": "last_trade",
                "trade_price": 21000.25,
                "trade_size": 1,
                "ts_event_ns": 1_700_000_010_000_000_000,
            },
            {
                "type": "last_trade",
                "trade_price": 21000.5,
                "trade_size": 2,
                "ts_event_ns": 1_700_000_011_000_000_000,
            },
            None,
            None,
        ]
        self.subscribed: list[tuple[str, str]] = []

    def get_front_month(self, symbol: str, exchange: str):
        return {
            "trading_symbol": "NQU6",
            "trading_exchange": exchange,
            "symbol": "NQU6",
            "exchange": exchange,
            "is_front_month_symbol": True,
            "symbol_name": "E-mini NASDAQ 100",
        }

    def subscribe(self, symbol: str, exchange: str) -> None:
        self.subscribed.append((symbol, exchange))

    def unsubscribe(self, symbol: str, exchange: str) -> None:
        return

    def poll_event(self, timeout_ms: int = 0):
        _ = timeout_ms
        if self._events:
            return self._events.pop(0)
        return None

    def load_ticks(self, symbol, exchange, start_ssboe, end_ssboe):
        return [
            {
                "type": "history_tick",
                "symbol": symbol,
                "exchange": exchange,
                "trade_price": 21000.25,
                "trade_size": 1,
                "ts_event_ns": 1_700_000_010_000_000_000,
            },
            {
                "type": "history_tick",
                "symbol": symbol,
                "exchange": exchange,
                "trade_price": 21000.5,
                "trade_size": 2,
                "ts_event_ns": 1_700_000_011_000_000_000,
            },
        ]


def test_resolve_front_month():
    front = resolve_front_month(FakeSession(), "NQ", "CME")
    assert front["trading_symbol"] == "NQU6"
    assert front["root"] == "NQ"


def test_resolve_front_month_missing_symbol():
    class Bad(WireSessionStub):
        def get_front_month(self, symbol, exchange):
            return {"exchange": exchange}

    try:
        resolve_front_month(Bad(), "NQ", "CME")
        raise AssertionError(
            "resolve_front_month should reject a session without get_front_month"
        )
    except FrontMonthError:
        pass


def test_compare_ticks_overlap():
    live = [
        RecordedTick(1_700_000_010_000_000_000, 21000.25, 1.0, "live"),
        RecordedTick(1_700_000_011_000_000_000, 21000.5, 2.0, "live"),
    ]
    hist = [
        RecordedTick(1_700_000_010_000_000_000, 21000.25, 1.0, "history"),
        RecordedTick(1_700_000_011_000_000_000, 21000.5, 2.0, "history"),
        RecordedTick(1_700_000_012_000_000_000, 21001.0, 1.0, "history"),
    ]
    cmp = compare_ticks(live, hist)
    assert cmp["matched"] == 2
    assert cmp["overlap_ratio"] == 1.0
    assert cmp["max_price_diff"] == 0.0


def test_compare_ticks_window_uses_min_max_not_list_ends():
    # Deliberately unsorted live list: ends are not the time window.
    live = [
        RecordedTick(1_700_000_020_000_000_000, 21001.0, 1.0, "live"),
        RecordedTick(1_700_000_010_000_000_000, 21000.25, 1.0, "live"),
    ]
    hist = [
        RecordedTick(1_700_000_010_000_000_000, 21000.25, 1.0, "history"),
        RecordedTick(1_700_000_015_000_000_000, 21000.5, 1.0, "history"),
        RecordedTick(1_700_000_020_000_000_000, 21001.0, 1.0, "history"),
    ]
    cmp = compare_ticks(live, hist)
    assert cmp["live_window_ns"]["start"] == 1_700_000_010_000_000_000
    assert cmp["live_window_ns"]["end"] == 1_700_000_020_000_000_000
    assert cmp["history_in_window"] == 3
    assert cmp["matched"] == 2


def test_compare_detects_usec_truncation_as_fuzzy():
    live = [
        RecordedTick(1_700_000_010_000_000_000, 21000.25, 1.0, "live"),
        RecordedTick(1_700_000_020_000_000_000, 21001.0, 1.0, "live"),
    ]
    hist = [
        RecordedTick(1_700_000_010_132_738_000, 21000.25, 1.0, "history"),
        RecordedTick(1_700_000_020_000_000_000, 21001.0, 1.0, "history"),
    ]
    cmp = compare_ticks(live, hist)
    assert cmp["matched"] == 1
    assert cmp["live_only"] == 1
    assert cmp["history_only"] == 1
    assert cmp["fuzzy_second_matches"] == 1
    assert cmp["live_only_samples"][0]["price"] == 21000.25
    assert any("usecs truncated" in n for n in cmp["notes"])


def test_run_front_month_verify_writes_report(tmp_path: Path):
    session = FakeSession()
    report = run_front_month_verify(
        session,
        root="NQ",
        exchange="CME",
        record_sec=0.5,
        record_dir=tmp_path / "ticks",
    )
    assert report.ok
    assert report.front["trading_symbol"] == "NQU6"
    assert "VERIFY OK" in report.summary
    out = report.write_json(tmp_path / "verify.json")
    payload = json.loads(out.read_text())
    assert payload["ok"] is True
    assert payload["compare"]["matched"] >= 1
    assert session.subscribed == [("NQU6", "CME")]
