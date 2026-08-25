"""Fixture tests for venue DTO → field-map converters."""

from __future__ import annotations

import pytest
from rithmic_nt_connect._convert import (
    ConvertError,
    account_pnl_to_fields,
    bbo_to_fields,
    instrument_id_from_symbol,
    last_trade_to_fields,
    order_book_to_fields,
    symbol_and_exchange_from_instrument_id,
)
from rithmic_nt_connect.constants import VENUE

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
    assert fields["instrument_id"] == f"NQU6-CME.{VENUE}"
    assert fields["price"] == pytest.approx(21012.5)
    assert fields["size"] == pytest.approx(1.0)
    assert fields["ts_event"] == 1_700_000_000 * 1_000_000_000 + 123456 * 1_000
    assert fields["type"] == "trade"


def test_bbo_fixture_to_quote_fields() -> None:
    fields = bbo_to_fields(BBO_FIXTURE)
    assert fields is not None
    assert fields["instrument_id"] == f"NQU6-CME.{VENUE}"
    assert fields["bid_price"] == pytest.approx(21012.0)
    assert fields["ask_price"] == pytest.approx(21012.25)
    assert fields["bid_size"] == pytest.approx(3.0)
    assert fields["ask_size"] == pytest.approx(2.0)
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


def test_partial_bbo_returns_none() -> None:
    # missing sizes + timestamp → not ready to emit
    assert bbo_to_fields({"symbol": "NQU6", "bid_price": 1.0, "ask_price": 2.0}) is None


def test_bbo_missing_symbol_raises() -> None:
    with pytest.raises(ConvertError):
        bbo_to_fields({"bid_price": 1.0})


def test_one_sided_bbo_merges_into_two_sided_quote() -> None:
    state = {}
    assert (
        bbo_to_fields(
            {
                "symbol": "NQU6",
                "exchange": "CME",
                "bid_price": 21012.0,
                "bid_size": 3,
                "ts_event_ns": 1_700_000_000_000_000_000,
            },
            state,
        )
        is None
    )
    fields = bbo_to_fields(
        {
            "symbol": "NQU6",
            "exchange": "CME",
            "ask_price": 21012.25,
            "ask_size": 2,
            "ts_event_ns": 1_700_000_000_000_000_001,
        },
        state,
    )
    assert fields is not None
    assert fields["instrument_id"] == f"NQU6-CME.{VENUE}"
    assert fields["bid_price"] == pytest.approx(21012.0)
    assert fields["ask_price"] == pytest.approx(21012.25)
    assert fields["bid_size"] == pytest.approx(3.0)
    assert fields["ask_size"] == pytest.approx(2.0)
    assert fields["ts_event"] == 1_700_000_000_000_000_001


def test_account_pnl_requires_account_id() -> None:
    with pytest.raises(ConvertError):
        account_pnl_to_fields({"account_balance": "1", "currency": "USD"})


def test_account_pnl_defaults_currency_and_zero_balance() -> None:
    fields = account_pnl_to_fields({"account_id": "A1"})
    assert fields["currency"] == "USD"
    assert fields["cash_on_hand"] == "0"
    assert fields["account_balance"] == "0"
    filled = account_pnl_to_fields({"account_id": "A1", "cash_on_hand": "1"})
    assert filled["currency"] == "USD"
    assert filled["cash_on_hand"] == "1"


def test_order_book_fixture_to_fields() -> None:
    fields = order_book_to_fields(ORDER_BOOK_FIXTURE)
    assert fields["instrument_id"] == f"NQU6-CME.{VENUE}"
    assert fields["type"] == "order_book"
    assert len(fields["levels"]) == 4
    assert fields["levels"][0]["side"] == "BUY"
    assert fields["levels"][0]["price"] == pytest.approx(21012.0)
    assert fields["levels"][-1]["side"] == "SELL"
    assert fields["ts_event"] == 1_700_000_000 * 1_000_000_000 + 300000 * 1_000


def test_order_book_empty_levels_raises() -> None:
    # Empty snapshot is venue-valid (off-hours) → levels=[] and Clear snapshot.
    fields = order_book_to_fields(
        {
            "symbol": "NQU6",
            "bid_price": [],
            "ask_price": [],
            "ssboe": 1,
        }
    )
    assert fields["type"] == "order_book"
    assert fields["levels"] == []
    assert fields["instrument_id"] == f"NQU6.{VENUE}"


def test_instrument_id_round_trip_with_exchange() -> None:
    encoded = instrument_id_from_symbol("NQU6", "cme")
    assert encoded == f"NQU6-CME.{VENUE}"
    assert symbol_and_exchange_from_instrument_id(encoded) == ("NQU6", "CME")


def test_instrument_id_round_trip_without_exchange() -> None:
    encoded = instrument_id_from_symbol("NQU6")
    assert encoded == f"NQU6.{VENUE}"
    assert symbol_and_exchange_from_instrument_id(encoded) == ("NQU6", None)


def test_instrument_id_rejects_hyphen_or_dot_in_parts() -> None:
    with pytest.raises(ConvertError, match="symbol"):
        instrument_id_from_symbol("NQ-U6", "CME")
    with pytest.raises(ConvertError, match="exchange"):
        instrument_id_from_symbol("NQU6", "CM.E")
    with pytest.raises(ConvertError, match=r"hyphen|empty|symbol|exchange"):
        symbol_and_exchange_from_instrument_id(f"NQU6-.{VENUE}")
    with pytest.raises(ConvertError, match="symbol"):
        symbol_and_exchange_from_instrument_id(f"NQ.U6.{VENUE}")
