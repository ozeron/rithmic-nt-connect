"""Instrument provider helpers (reference data → FuturesContract)."""

from __future__ import annotations

from rithmic_connect.providers import future_from_reference
from rithmic_connect.providers import _parse_expiration_ns


def test_future_from_reference_uses_tick_size_and_underlying():
    instrument = future_from_reference(
        {
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
        },
        fallback_symbol="NQ",
        fallback_exchange="CME",
    )
    assert str(instrument.id) == "NQU6.RITHMIC"
    assert instrument.underlying == "NQ"
    assert instrument.price_precision == 2
    assert float(instrument.price_increment) == 0.25
    assert float(instrument.multiplier) == 20.0
    assert instrument.info["rithmic_symbol"] == "NQU6"


def test_parse_expiration_ymd():
    ns = _parse_expiration_ns("20260918", fallback_ns=0)
    assert ns > 0
