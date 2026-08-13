"""Exec-client order routing tests (notification router)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import StrategyId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

from rithmic_connect._exec_notifications import route_order_notification
from rithmic_connect._orders import is_exchange_not_modified
from rithmic_connect._orders import is_exchange_trigger
from rithmic_connect._orders import is_rithmic_modified
from rithmic_connect._orders import order_notification_to_fields
from rithmic_connect._orders import trade_id_from_fill_fields


def _mock_order() -> SimpleNamespace:
    return SimpleNamespace(
        strategy_id=StrategyId("S-1"),
        instrument_id=InstrumentId.from_str("NQU6.RITHMIC"),
        client_order_id=ClientOrderId("O-1"),
        side=OrderSide.BUY,
        order_type=MagicMock(),
        quantity=Quantity.from_int(1),
        has_price=True,
        price=Price.from_str("21000.0"),
        has_trigger_price=False,
        trigger_price=None,
    )


def _route(fields: dict, emit: MagicMock) -> None:
    cid = ClientOrderId("O-1")
    tag_map = {"O-1": cid}
    venue_map: dict[str, str] = {}

    route_order_notification(
        fields,
        resolve_client_order_id=lambda f: tag_map.get(str(f.get("user_tag") or "")),
        get_order=lambda _cid: _mock_order(),
        bind_venue_id=lambda c, v: venue_map.__setitem__(c.value, v),
        venue_id_for=lambda c, f: str(f.get("basket_id") or venue_map.get(c.value) or c.value),
        clock_ts=lambda: 1_700_000_000_000_000_000,
        emit=emit,
        log_debug=lambda _m: None,
        log_warning=lambda _m: None,
        log_error=lambda _m: None,
    )


def test_predicate_modified_trigger_not_modified() -> None:
    assert is_rithmic_modified({"source": "rithmic", "notify_type": 14, "notify_type_name": "MODIFIED"})
    assert is_exchange_trigger({"source": "exchange", "notify_type": 4, "notify_type_name": "TRIGGER"})
    assert is_exchange_not_modified(
        {"source": "exchange", "notify_type": 7, "notify_type_name": "NOT_MODIFIED"}
    )


def test_route_open_accepted() -> None:
    emit = MagicMock()
    fields = order_notification_to_fields(
        {
            "source": "rithmic",
            "symbol": "NQU6",
            "notify_type": 13,
            "notify_type_name": "OPEN",
            "basket_id": "B1",
            "user_tag": "O-1",
            "ssboe": 1_700_000_000,
        }
    )
    _route(fields, emit)
    emit.generate_order_accepted.assert_called_once()


def test_route_modified_updates() -> None:
    emit = MagicMock()
    fields = order_notification_to_fields(
        {
            "source": "rithmic",
            "symbol": "NQU6",
            "notify_type": 14,
            "notify_type_name": "MODIFIED",
            "basket_id": "B1",
            "user_tag": "O-1",
            "quantity": 2,
            "price": 21001.0,
            "ssboe": 1_700_000_000,
        }
    )
    _route(fields, emit)
    emit.generate_order_updated.assert_called_once()


def test_route_exchange_trigger() -> None:
    emit = MagicMock()
    fields = order_notification_to_fields(
        {
            "source": "exchange",
            "symbol": "NQU6",
            "notify_type": 4,
            "notify_type_name": "TRIGGER",
            "basket_id": "B1",
            "user_tag": "O-1",
            "ssboe": 1_700_000_000,
        }
    )
    _route(fields, emit)
    emit.generate_order_triggered.assert_called_once()


def test_route_not_modified_rejects() -> None:
    emit = MagicMock()
    fields = order_notification_to_fields(
        {
            "source": "exchange",
            "symbol": "NQU6",
            "notify_type": 7,
            "notify_type_name": "NOT_MODIFIED",
            "basket_id": "B1",
            "user_tag": "O-1",
            "ssboe": 1_700_000_000,
        }
    )
    _route(fields, emit)
    emit.generate_order_modify_rejected.assert_called_once()


def test_trade_id_unique_without_fill_id() -> None:
    fields = {
        "basket_id": "B1",
        "exchange_order_id": "E1",
        "fill_size": 1,
        "fill_price": 100.0,
    }
    a = trade_id_from_fill_fields(fields, 100)
    fields["fill_size"] = 2
    b = trade_id_from_fill_fields(fields, 100)
    assert a != b
