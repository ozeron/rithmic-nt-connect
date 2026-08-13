"""History/depth conversion and request path tests."""

from __future__ import annotations

import pytest
from nautilus_trader.model.data import BarType

from rithmic_nt_connect._convert import ConvertError
from rithmic_nt_connect._convert import last_trade_to_fields
from rithmic_nt_connect._convert import time_bar_to_fields
from rithmic_nt_connect.data import bar_type_to_rithmic
from rithmic_nt_connect.data import fields_to_bar


def test_history_tick_requires_trade_price_and_size():
    payload = {
        "type": "history_tick",
        "symbol": "NQU6",
        "exchange": "CME",
        "trade_price": 100.5,
        "trade_size": 1,
        "ssboe": 1700000000,
        "usecs": 0,
    }
    fields = last_trade_to_fields(payload)
    assert fields["price"] == 100.5
    assert fields["size"] == 1.0


def test_history_tick_does_not_invent_from_ohlc_or_volume():
    with pytest.raises(ConvertError):
        last_trade_to_fields(
            {
                "type": "history_tick",
                "symbol": "NQU6",
                "exchange": "CME",
                "close_price": 101.25,
                "num_trades": 3,
                "volume": 999_999,
                "ssboe": 1700000000,
                "usecs": 0,
            }
        )


def test_time_bar_to_fields_and_bar():
    raw = {
        "type": "history_bar",
        "symbol": "NQU6",
        "exchange": "CME",
        "open_price": 100.0,
        "high_price": 101.0,
        "low_price": 99.5,
        "close_price": 100.5,
        "volume": 42,
        "marker": 1_700_000_000,
        "bar_type": 2,
        "period": "1",
    }
    fields = time_bar_to_fields(raw)
    assert fields["open"] == 100.0
    assert fields["ts_event"] == 1_700_000_000 * 1_000_000_000
    bar_type = BarType.from_str("NQU6.RITHMIC-1-MINUTE-LAST-EXTERNAL")
    bar = fields_to_bar(fields, bar_type, ts_init=1)
    assert float(bar.close) == 100.5
    assert int(bar.volume) == 42


def test_time_bar_requires_volume():
    with pytest.raises(ConvertError):
        time_bar_to_fields(
            {
                "symbol": "NQU6",
                "open_price": 1.0,
                "high_price": 1.0,
                "low_price": 1.0,
                "close_price": 1.0,
                "marker": 1_700_000_000,
            }
        )


def test_bar_type_to_rithmic_mapping():
    assert bar_type_to_rithmic(BarType.from_str("NQU6.RITHMIC-1-MINUTE-LAST-EXTERNAL")) == (2, 1)
    assert bar_type_to_rithmic(BarType.from_str("NQU6.RITHMIC-5-MINUTE-LAST-EXTERNAL")) == (2, 5)
    assert bar_type_to_rithmic(BarType.from_str("NQU6.RITHMIC-1-SECOND-LAST-EXTERNAL")) == (1, 1)
    assert bar_type_to_rithmic(BarType.from_str("NQU6.RITHMIC-1-DAY-LAST-EXTERNAL")) == (3, 1)
    assert bar_type_to_rithmic(BarType.from_str("NQU6.RITHMIC-1-HOUR-LAST-EXTERNAL")) == (2, 60)


def test_order_book_entitlement_error_is_explicit():
    class Boom(Exception):
        pass

    class Sess:
        def subscribe_order_book_summary(self, symbol, exchange):
            raise Boom("depth not entitled")

    with pytest.raises(Boom):
        Sess().subscribe_order_book_summary("NQ", "CME")


def test_malformed_history_raises_convert_error():
    with pytest.raises(ConvertError):
        last_trade_to_fields({"symbol": "NQ"})


def test_malformed_bar_raises_convert_error():
    with pytest.raises(ConvertError):
        time_bar_to_fields({"symbol": "NQ", "open_price": 1.0})
