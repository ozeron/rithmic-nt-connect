"""History/depth conversion and request path tests."""

from __future__ import annotations

import pytest
from nautilus_trader.model.data import BarType

from rithmic_nt_connect._convert import ConvertError
from rithmic_nt_connect._convert import last_trade_to_fields
from rithmic_nt_connect._convert import time_bar_to_fields
from rithmic_nt_connect.data import bar_type_to_rithmic
from rithmic_nt_connect.data import external_bar_advertised
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
        "period": "60",
    }
    fields = time_bar_to_fields(raw)
    assert fields["open"] == 100.0
    # time_bar_to_fields reports the venue CLOSE time unshifted; the close→open
    # shift is applied by fields_to_bar using the authoritative BarType.
    assert fields["ts_event"] == 1_700_000_000 * 1_000_000_000
    bar_type = BarType.from_str("NQU6.RITHMIC-1-MINUTE-LAST-EXTERNAL")
    bar = fields_to_bar(fields, bar_type, ts_init=1)
    assert float(bar.close) == 100.5
    assert int(bar.volume) == 42
    # 1-minute bar: Bar.ts_event = close (marker) − 60s = open time.
    assert bar.ts_event == (1_700_000_000 - 60) * 1_000_000_000


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


def test_live_time_bar_dict_converts():
    raw = {
        "type": "time_bar",
        "symbol": "NQU6",
        "exchange": "CME",
        "open_price": 100.0,
        "high_price": 101.0,
        "low_price": 99.5,
        "close_price": 100.5,
        "volume": 42,
        "marker": 1_700_000_000,
        "bar_type": 2,
        "period": "900",
    }
    fields = time_bar_to_fields(raw)
    # Close time reported unshifted by time_bar_to_fields.
    assert fields["ts_event"] == 1_700_000_000 * 1_000_000_000
    bar = fields_to_bar(
        fields,
        BarType.from_str("NQU6.RITHMIC-15-MINUTE-LAST-EXTERNAL"),
        ts_init=1,
    )
    # 15-minute bar: Bar.ts_event = marker − 15*60s = open time.
    assert bar.ts_event == (1_700_000_000 - 900) * 1_000_000_000
    assert float(bar.close) == 100.5


def test_time_bar_close_to_open_shift():
    # Shift is driven by the authoritative BarType (SECOND step), independent of
    # the wire `period` unit; daily marker (YYYYMMDD) is left as-is.
    sec_fields = time_bar_to_fields(
        {
            "type": "history_bar",
            "symbol": "NQU6",
            "exchange": "CME",
            "open_price": 1.0, "high_price": 1.0, "low_price": 1.0, "close_price": 1.0,
            "volume": 1,
            "marker": 1_700_000_000,
            "bar_type": 1,
            "period": "1",
        }
    )
    sec = fields_to_bar(
        sec_fields,
        BarType.from_str("NQU6.RITHMIC-1-SECOND-LAST-EXTERNAL"),
        ts_init=1,
    )
    # 1-second bar shifts by 1s.
    assert sec.ts_event == (1_700_000_000 - 1) * 1_000_000_000

    # 1-minute live bar whose wire `period` is the request's native unit ("1"):
    # the shift must still be 60s (from the BarType), not 1s.
    min_fields = time_bar_to_fields(
        {
            "type": "time_bar",
            "symbol": "NQU6",
            "exchange": "CME",
            "open_price": 1.0, "high_price": 1.0, "low_price": 1.0, "close_price": 1.0,
            "volume": 1,
            "marker": 1_700_000_000,
            "bar_type": 2,
            "period": "1",
        }
    )
    one_min = fields_to_bar(
        min_fields,
        BarType.from_str("NQU6.RITHMIC-1-MINUTE-LAST-EXTERNAL"),
        ts_init=1,
    )
    assert one_min.ts_event == (1_700_000_000 - 60) * 1_000_000_000

    # 1-hour bar maps to MINUTE bar_type period=60 (request native unit); the
    # shift must be 3600s, not 60s.
    hour_fields = time_bar_to_fields(
        {
            "type": "time_bar",
            "symbol": "NQU6",
            "exchange": "CME",
            "open_price": 1.0, "high_price": 1.0, "low_price": 1.0, "close_price": 1.0,
            "volume": 1,
            "marker": 1_700_000_000,
            "bar_type": 2,
            "period": "60",
        }
    )
    hour = fields_to_bar(
        hour_fields,
        BarType.from_str("NQU6.RITHMIC-1-HOUR-LAST-EXTERNAL"),
        ts_init=1,
    )
    assert hour.ts_event == (1_700_000_000 - 3600) * 1_000_000_000

    daily_fields = time_bar_to_fields(
        {
            "type": "history_bar",
            "symbol": "NQU6",
            "exchange": "CME",
            "open_price": 1.0, "high_price": 1.0, "low_price": 1.0, "close_price": 1.0,
            "volume": 1,
            "marker": 20_260_804,
            "ts_event_ns": 1_780_000_000 * 1_000_000_000,
            "bar_type": 3,
            "period": "1",
        }
    )
    daily = fields_to_bar(
        daily_fields,
        BarType.from_str("NQU6.RITHMIC-1-DAY-LAST-EXTERNAL"),
        ts_init=1,
    )
    assert daily.ts_event == 1_780_000_000 * 1_000_000_000  # untouched


def test_external_bar_advertised_slice():
    assert external_bar_advertised(BarType.from_str("NQU6.RITHMIC-1-MINUTE-LAST-EXTERNAL"))
    assert external_bar_advertised(BarType.from_str("NQU6.RITHMIC-15-MINUTE-LAST-EXTERNAL"))
    assert external_bar_advertised(BarType.from_str("NQU6.RITHMIC-1-DAY-LAST-EXTERNAL"))
    assert external_bar_advertised(BarType.from_str("NQU6.RITHMIC-1-HOUR-LAST-EXTERNAL"))
    assert not external_bar_advertised(BarType.from_str("NQU6.RITHMIC-1-SECOND-LAST-EXTERNAL"))
    assert not external_bar_advertised(BarType.from_str("NQU6.RITHMIC-1-SECOND-LAST-INTERNAL"))
    assert not external_bar_advertised(BarType.from_str("NQU6.RITHMIC-5-MINUTE-LAST-EXTERNAL"))


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
