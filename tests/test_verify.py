"""Unit tests for front-month resolve + live/history verify compare."""

from __future__ import annotations

import json
from pathlib import Path

from rithmic_connect.front_month import FrontMonthError
from rithmic_connect.front_month import resolve_front_month
from rithmic_connect.verify import RecordedTick
from rithmic_connect.verify import compare_ticks
from rithmic_connect.verify import run_front_month_verify


class FakeSession:
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

    def get_front_month(self, root: str, exchange: str):
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

    def poll_event(self):
        if self._events:
            return self._events.pop(0)
        return None

    def load_ticks(self, symbol, exchange, start, end):
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
                "close_price": 21000.5,
                "num_trades": 2,
                "ts_event_ns": 1_700_000_011_000_000_000,
            },
        ]


def test_resolve_front_month():
    front = resolve_front_month(FakeSession(), "NQ", "CME")
    assert front["trading_symbol"] == "NQU6"
    assert front["root"] == "NQ"


def test_resolve_front_month_missing_symbol():
    class Bad:
        def get_front_month(self, root, exchange):
            return {"exchange": exchange}

    try:
        resolve_front_month(Bad(), "NQ", "CME")  # type: ignore[arg-type]
        assert False
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
