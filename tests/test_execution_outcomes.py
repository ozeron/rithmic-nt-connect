"""Unit tests for exec honesty helpers (deny / dedup / slim fields)."""

from __future__ import annotations

from rithmic_nt_connect._orders import fill_dedup_key
from rithmic_nt_connect._orders import slim_order_fields
from rithmic_nt_connect._orders import trade_id_from_fill_fields


def test_fill_dedup_key_includes_account_and_instrument():
    fields = {
        "fill_id": "F1",
        "basket_id": "B1",
        "account_id": "A1",
        "instrument_id": "NQU6.RITHMIC",
    }
    assert fill_dedup_key(fields, ts_event=1) == "A1|NQU6.RITHMIC|F1"


def test_fill_dedup_key_none_without_fill_id():
    fields = {
        "basket_id": "B1",
        "exchange_order_id": "E1",
        "fill_size": 1,
        "fill_price": 21000.0,
        "account_id": "A",
        "instrument_id": "I",
    }
    assert fill_dedup_key(fields, ts_event=99) is None
    assert trade_id_from_fill_fields(fields, 99) == "B1:E1:99:1:21000.0"


def test_slim_order_fields_omits_noise():
    slim = slim_order_fields(
        {
            "kind": "filled",
            "basket_id": "B1",
            "password": "secret",
            "fill_price": 1.0,
            "symbol": "NQU6",
        }
    )
    assert slim == {"kind": "filled", "basket_id": "B1", "symbol": "NQU6"}
    assert "password" not in slim
    assert "fill_price" not in slim


def test_order_side_from_notification():
    from nautilus_trader.model.enums import OrderSide

    from rithmic_nt_connect._orders import order_side_from_notification

    assert order_side_from_notification({"transaction_type": 1}) == OrderSide.BUY
    assert order_side_from_notification({"transaction_type": 2}) == OrderSide.SELL
    assert order_side_from_notification({}) is None
    assert order_side_from_notification({"transaction_type": "BUY"}) is None
    assert order_side_from_notification({"transaction_type": 99}) is None
