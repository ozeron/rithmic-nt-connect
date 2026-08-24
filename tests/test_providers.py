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
