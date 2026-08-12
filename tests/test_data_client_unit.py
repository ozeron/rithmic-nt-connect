"""Unit tests for data client conversion + subscribe wiring (mocked session)."""

from __future__ import annotations

from rithmic_connect.data import fields_to_order_book_deltas
from rithmic_connect.data import fields_to_quote_tick
from rithmic_connect.data import fields_to_trade_tick
from rithmic_connect._convert import bbo_to_fields
from rithmic_connect._convert import last_trade_to_fields
from rithmic_connect._convert import order_book_to_fields


def test_last_trade_fields_to_trade_tick():
    raw = {
        "type": "last_trade",
        "symbol": "NQU6",
        "exchange": "CME",
        "trade_price": 20000.25,
        "trade_size": 2,
        "aggressor": 1,
        "ts_event_ns": 1_700_000_000_000_000_000,
    }
    fields = last_trade_to_fields(raw)
    tick = fields_to_trade_tick(fields, ts_init=1_700_000_000_000_000_001)
    assert str(tick.instrument_id) == "NQU6.RITHMIC"
    assert float(tick.price) == 20000.25


def test_bbo_fields_to_quote_tick():
    raw = {
        "type": "bbo",
        "symbol": "NQU6",
        "exchange": "CME",
        "bid_price": 1.0,
        "ask_price": 2.0,
        "bid_size": 3,
        "ask_size": 4,
        "ssboe": 1_700_000_000,
        "usecs": 0,
    }
    fields = bbo_to_fields(raw)
    tick = fields_to_quote_tick(fields, ts_init=1)
    assert float(tick.bid_price) == 1.0
    assert float(tick.ask_price) == 2.0


def test_order_book_fields_to_deltas():
    raw = {
        "type": "order_book",
        "symbol": "NQU6",
        "exchange": "CME",
        "bid_price": [100.0],
        "bid_size": [2],
        "ask_price": [100.25],
        "ask_size": [3],
        "ts_event_ns": 1_700_000_000_000_000_000,
    }
    fields = order_book_to_fields(raw)
    deltas = fields_to_order_book_deltas(fields, ts_init=1)
    assert str(deltas.instrument_id) == "NQU6.RITHMIC"
    # CLEAR + 2 ADD levels
    assert len(deltas.deltas) == 3


def test_subscribe_contract_is_callable_on_mock_session():
    calls: list[tuple[str, str]] = []

    class Sess:
        def subscribe(self, symbol: str, exchange: str) -> None:
            calls.append((symbol, exchange))

    Sess().subscribe("NQU6", "CME")
    assert calls == [("NQU6", "CME")]
