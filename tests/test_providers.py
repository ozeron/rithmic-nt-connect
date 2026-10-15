"""Instrument provider helpers (reference data → FuturesContract)."""

from __future__ import annotations

import pytest
from rithmic_nt_connect.providers import (
    InstrumentBuildError,
    _parse_expiration_ns,
    future_from_reference,
)

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


def test_future_from_reference_uses_tick_size_and_underlying():
    instrument = future_from_reference(COMPLETE_REF)
    assert str(instrument.id) == "NQU6-CME.RITHMIC"
    assert instrument.underlying == "NQ"
    assert instrument.price_precision == 2
    assert float(instrument.price_increment) == pytest.approx(0.25)
    assert float(instrument.multiplier) == pytest.approx(20.0)
    assert instrument.info["rithmic_symbol"] == "NQU6"
    assert instrument.activation_ns == 0


def test_future_from_reference_requires_fields():
    with pytest.raises(InstrumentBuildError):
        future_from_reference({"trading_symbol": "NQU6"})


def test_parse_expiration_ymd():
    ns = _parse_expiration_ns("20260918")
    assert ns > 0


def test_parse_expiration_rejects_unknown():
    with pytest.raises(InstrumentBuildError):
        _parse_expiration_ns("not-a-date")


# Exact Rithmic Test reference-data reply observed 2026-10-15. In particular,
# trading_symbol is None and price_precision/trading_exchange are absent.
LISTED_REF = {
    "symbol": "NQZ6",
    "exchange": "CME",
    "trading_symbol": None,
    "product_code": "NQ",
    "instrument_type": "Future",
    "tick_size": 0.25,
    "min_qty": None,
    "point_value": 20.0,
    "expiration_date": "20261218",
    "currency": "USD",
    "is_tradable": False,
    "description": None,
    "price_display_format": None,
}


def test_future_from_listed_reference_without_trading_symbol_or_precision():
    instrument = future_from_reference(LISTED_REF)
    assert str(instrument.id) == "NQZ6-CME.RITHMIC"
    assert instrument.raw_symbol.value == "NQZ6"
    assert instrument.price_precision == 2
    assert float(instrument.price_increment) == 0.25
    assert float(instrument.multiplier) == 20.0
    assert instrument.underlying == "NQ"
    assert instrument.info["rithmic_symbol"] == "NQZ6"
    assert instrument.info["rithmic_exchange"] == "CME"
    assert instrument.info["rithmic_is_tradable"] is False
    assert instrument.expiration_ns == 1_797_552_000_000_000_000
    assert LISTED_REF["trading_symbol"] is None
    assert "price_precision" not in LISTED_REF


def test_provider_loads_listed_contract_without_front_month():
    import asyncio

    from _stubs import WireSessionStub
    from rithmic_nt_connect.providers import RithmicInstrumentProvider

    class ListedSession(WireSessionStub):
        def get_reference_data(self, symbol, exchange):
            assert (symbol, exchange) == ("NQZ6", "CME")
            return dict(LISTED_REF)

    provider = RithmicInstrumentProvider(ListedSession(), [("NQZ6", "CME")])
    asyncio.run(provider.load_all_async())
    assert [str(inst.id) for inst in provider.list_all()] == ["NQZ6-CME.RITHMIC"]


def test_explicit_contract_loader_and_front_month_remains_strict():
    from _stubs import WireSessionStub
    from rithmic_nt_connect.front_month import FrontMonthError
    from rithmic_nt_connect.historical import (
        load_contract_instrument,
        load_front_month_instrument,
    )

    class ListedSession(WireSessionStub):
        def get_front_month(self, symbol, exchange):
            return {"type": "front_month"}

        def get_reference_data(self, symbol, exchange):
            assert (symbol, exchange) == ("NQZ6", "CME")
            return dict(LISTED_REF)

    session = ListedSession()
    instrument = load_contract_instrument(session, "NQZ6", "CME")
    assert str(instrument.id) == "NQZ6-CME.RITHMIC"
    with pytest.raises(FrontMonthError, match="missing trading_symbol"):
        load_front_month_instrument(session, "NQ", "CME")
