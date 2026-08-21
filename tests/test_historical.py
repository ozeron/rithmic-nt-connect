"""Standalone historical helper (no live session)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from _stubs import WireSessionStub
from rithmic_nt_connect.data import payloads_to_trade_ticks
from rithmic_nt_connect.historical import load_front_month_instrument, load_trade_ticks
from rithmic_nt_connect.providers import future_from_reference

COMPLETE_REF = {
    "trading_symbol": "NQU6",
    "trading_exchange": "CME",
    "underlying": "NQ",
    "product_code": "NQ",
    "currency": "USD",
    "tick_size": 0.25,
    "point_value": 20.0,
    "price_precision": 2,
    "expiration_date": "20260918",
    "is_tradable": True,
}


class _FakeSession(WireSessionStub):
    def __init__(self) -> None:
        self.ticks: list[dict] = []

    def get_front_month(self, symbol: str, exchange: str) -> dict:
        return {
            "trading_symbol": "NQU6",
            "trading_exchange": exchange,
            "symbol": symbol,
        }

    def get_reference_data(self, symbol: str, exchange: str) -> dict:
        return dict(COMPLETE_REF)

    def load_ticks(
        self, symbol: str, exchange: str, start_ssboe: int, end_ssboe: int
    ) -> list[dict]:
        _ = (symbol, exchange, start_ssboe, end_ssboe)
        return list(self.ticks)


def test_payloads_to_trade_ticks_sorts_and_fills_route() -> None:
    raw = [
        {
            "trade_price": 21000.5,
            "trade_size": 1,
            "ts_event_ns": 1_700_000_002_000_000_000,
        },
        {
            "trade_price": 21000.25,
            "trade_size": 2,
            "ts_event_ns": 1_700_000_001_000_000_000,
        },
    ]
    ticks = payloads_to_trade_ticks(
        raw,
        symbol="NQU6",
        exchange="CME",
        price_precision=2,
        ts_init=None,
    )
    assert len(ticks) == 2
    assert ticks[0].ts_event < ticks[1].ts_event
    assert str(ticks[0].instrument_id) == "NQU6.RITHMIC"
    assert ticks[0].ts_init == ticks[0].ts_event


def test_load_front_month_instrument() -> None:
    instrument = load_front_month_instrument(_FakeSession(), "NQ", "CME")
    assert str(instrument.id) == "NQU6.RITHMIC"
    assert instrument.info["rithmic_symbol"] == "NQU6"


def test_load_trade_ticks_uses_session_window() -> None:
    session = _FakeSession()
    session.ticks = [
        {
            "symbol": "NQU6",
            "exchange": "CME",
            "trade_price": 21000.25,
            "trade_size": 1,
            "ts_event_ns": 1_700_000_000_000_000_000,
        }
    ]
    instrument = future_from_reference(COMPLETE_REF)
    start = datetime.fromtimestamp(1_700_000_000, tz=UTC)
    end = datetime.fromtimestamp(1_700_000_010, tz=UTC)
    ticks = load_trade_ticks(session, instrument, start, end)
    assert len(ticks) == 1
    assert float(ticks[0].price) == pytest.approx(21000.25)


def test_load_front_month_rejects_bad_ref() -> None:
    class Bad(WireSessionStub):
        def get_front_month(self, symbol, exchange):
            return {"trading_symbol": "NQU6", "trading_exchange": "CME"}

        def get_reference_data(self, symbol, exchange):
            return "nope"

    with pytest.raises(TypeError):
        load_front_month_instrument(Bad(), "NQ", "CME")
