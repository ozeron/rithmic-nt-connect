"""Order-book depth conversion tests (U5)."""

from __future__ import annotations

import pytest

from rithmic_nt_connect._convert import ConvertError
from rithmic_nt_connect._convert import order_book_to_fields
from rithmic_nt_connect.constants import VENUE
from rithmic_nt_connect.data import fields_to_order_book_deltas


ORDER_BOOK_SUMMARY = {
    "type": "order_book",
    "symbol": "NQU6",
    "exchange": "CME",
    "update_type": 1,
    "bid_price": [21012.0, 21011.75, 21011.5],
    "bid_size": [2, 4, 6],
    "ask_price": [21012.25, 21012.5],
    "ask_size": [1, 3],
    "ts_event_ns": 1_700_000_000_000_000_000,
}


def test_order_book_summary_to_delta_fields() -> None:
    fields = order_book_to_fields(ORDER_BOOK_SUMMARY)
    assert fields["instrument_id"] == f"NQU6.{VENUE}"
    assert len(fields["levels"]) == 5
    assert fields["levels"][0]["side"] == "BUY"
    assert fields["levels"][-1]["side"] == "SELL"


def test_order_book_fields_to_nautilus_deltas() -> None:
    from nautilus_trader.model.enums import RecordFlag

    fields = order_book_to_fields(ORDER_BOOK_SUMMARY)
    deltas = fields_to_order_book_deltas(fields, ts_init=1)
    # CLEAR + 5 ADD
    assert len(deltas.deltas) == 6
    assert str(deltas.instrument_id) == f"NQU6.{VENUE}"
    assert int(deltas.deltas[-1].flags) == int(
        RecordFlag.F_SNAPSHOT.value | RecordFlag.F_LAST.value
    )


def test_depth_entitlement_style_empty_book_is_explicit() -> None:
    with pytest.raises(ConvertError) as exc:
        order_book_to_fields(
            {
                "symbol": "NQU6",
                "bid_price": [],
                "ask_price": [],
                "ssboe": 1,
            }
        )
    assert "no bid/ask levels" in str(exc.value)
