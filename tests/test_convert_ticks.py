"""Fixture tests for venue DTO → field-map converters."""

from __future__ import annotations

import pytest

from rithmic_connect._convert import (
    ConvertError,
    account_pnl_to_fields,
    bbo_to_fields,
    last_trade_to_fields,
    order_book_to_fields,
)
from rithmic_connect.constants import VENUE


LAST_TRADE_FIXTURE = {
    "type": "last_trade",
    "symbol": "NQU6",
    "exchange": "CME",
    "trade_price": 21012.5,
    "trade_size": 1,
    "aggressor": 1,
    "ssboe": 1_700_000_000,
    "usecs": 123456,
}

BBO_FIXTURE = {
    "type": "bbo",
    "symbol": "NQU6",
    "exchange": "CME",
    "bid_price": 21012.0,
    "bid_size": 3,
    "ask_price": 21012.25,
    "ask_size": 2,
    "ssboe": 1_700_000_000,
    "usecs": 200000,
}

ACCOUNT_PNL_FIXTURE = {
    "type": "account_pnl",
    "account_id": "ABC123",
    "fcm_id": "FCM",
    "ib_id": "IB",
    "account_balance": "100000.00",
    "cash_on_hand": "95000.00",
    "margin_balance": "80000.00",
    "day_pnl": "125.50",
    "open_position_pnl": "50.00",
    "closed_position_pnl": "75.50",
    "available_buying_power": "50000.00",
    "currency": "USD",
    "is_snapshot": True,
}

ORDER_BOOK_FIXTURE = {
    "type": "order_book",
    "symbol": "NQU6",
    "exchange": "CME",
    "update_type": 1,
    "bid_price": [21012.0, 21011.75],
    "bid_size": [3, 5],
    "ask_price": [21012.25, 21012.5],
    "ask_size": [2, 4],
    "ssboe": 1_700_000_000,
    "usecs": 300000,
}


def test_last_trade_fixture_to_trade_fields() -> None:
    fields = last_trade_to_fields(LAST_TRADE_FIXTURE)
    assert fields["instrument_id"] == f"NQU6.{VENUE}"
    assert fields["price"] == 21012.5
    assert fields["size"] == 1.0
    assert fields["ts_event"] == 1_700_000_000 * 1_000_000_000 + 123456 * 1_000
    assert fields["type"] == "trade"


def test_bbo_fixture_to_quote_fields() -> None:
    fields = bbo_to_fields(BBO_FIXTURE)
    assert fields["instrument_id"] == f"NQU6.{VENUE}"
    assert fields["bid_price"] == 21012.0
    assert fields["ask_price"] == 21012.25
    assert fields["bid_size"] == 3.0
    assert fields["ask_size"] == 2.0
    assert fields["type"] == "quote"


def test_account_pnl_fixture_fields() -> None:
    fields = account_pnl_to_fields(ACCOUNT_PNL_FIXTURE)
    assert fields["account_id"] == "ABC123"
    assert fields["account_balance"] == "100000.00"
    assert fields["day_pnl"] == "125.50"
    assert fields["currency"] == "USD"
    assert fields["venue"] == VENUE


def test_malformed_last_trade_raises() -> None:
    with pytest.raises(ConvertError) as exc:
        last_trade_to_fields({"symbol": "NQU6"})
    assert "missing required fields" in str(exc.value)


def test_partial_bbo_raises() -> None:
    with pytest.raises(ConvertError):
        bbo_to_fields(
            {
                "symbol": "NQU6",
                "bid_price": 1.0,
                "ask_price": 2.0,
                # missing sizes + timestamp
            }
        )


def test_account_pnl_requires_account_id() -> None:
    with pytest.raises(ConvertError):
        account_pnl_to_fields({"account_balance": "1", "currency": "USD"})


def test_account_pnl_requires_balance_and_currency() -> None:
    with pytest.raises(ConvertError):
        account_pnl_to_fields({"account_id": "A1"})
    with pytest.raises(ConvertError):
        account_pnl_to_fields({"account_id": "A1", "cash_on_hand": "1"})


def test_order_book_fixture_to_fields() -> None:
    fields = order_book_to_fields(ORDER_BOOK_FIXTURE)
    assert fields["instrument_id"] == f"NQU6.{VENUE}"
    assert fields["type"] == "order_book"
    assert len(fields["levels"]) == 4
    assert fields["levels"][0]["side"] == "BUY"
    assert fields["levels"][0]["price"] == 21012.0
    assert fields["levels"][-1]["side"] == "SELL"
    assert fields["ts_event"] == 1_700_000_000 * 1_000_000_000 + 300000 * 1_000


def test_order_book_empty_levels_raises() -> None:
    with pytest.raises(ConvertError):
        order_book_to_fields(
            {
                "symbol": "NQU6",
                "bid_price": [],
                "ask_price": [],
                "ssboe": 1,
            }
        )
