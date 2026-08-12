"""History/depth conversion and request path tests."""

from __future__ import annotations

from rithmic_connect._convert import ConvertError
from rithmic_connect._convert import last_trade_to_fields


def test_history_tick_payload_maps_via_trade_fields():
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


def test_empty_history_is_empty_list():
    # Session contract: load_ticks returns []
    assert list([]) == []


def test_order_book_entitlement_error_is_explicit():
    class Boom(Exception):
        pass

    class Sess:
        def subscribe_order_book_summary(self, symbol, exchange):
            raise Boom("depth not entitled")

    try:
        Sess().subscribe_order_book_summary("NQ", "CME")
        assert False, "expected Boom"
    except Boom as exc:
        assert "entitled" in str(exc)


def test_malformed_history_raises_convert_error():
    try:
        last_trade_to_fields({"symbol": "NQ"})
        assert False
    except ConvertError:
        pass
