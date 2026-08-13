"""Exec-client order routing / report-policy tests (no Cython client construction)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import StrategyId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

from rithmic_connect._order_plant import OrderPlantPolicy
from rithmic_connect._order_plant import OrderPlantState
from rithmic_connect._orders import notification_action
from rithmic_connect._orders import order_notification_to_fields
from rithmic_connect.errors import VenueQueryUnavailable


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


def test_notification_action_open_modified_trigger_not_modified() -> None:
    order = _mock_order()
    open_fields = order_notification_to_fields(
        {
            "type": "order_notification",
            "source": "rithmic",
            "symbol": "NQU6",
            "kind": "accepted",
            "notify_type_name": "OPEN",
            "basket_id": "B1",
            "user_tag": "O-1",
            "ssboe": 1_700_000_000,
        }
    )
    assert notification_action(open_fields, order).kind == "accepted"

    updated = order_notification_to_fields(
        {
            "type": "order_notification",
            "source": "rithmic",
            "symbol": "NQU6",
            "kind": "updated",
            "notify_type_name": "MODIFIED",
            "quantity": 2,
            "price": 21001.0,
            "ssboe": 1_700_000_000,
        }
    )
    action = notification_action(updated, order)
    assert action.kind == "updated"
    assert action.quantity == 2

    triggered = order_notification_to_fields(
        {
            "type": "order_notification",
            "source": "exchange",
            "symbol": "NQU6",
            "kind": "triggered",
            "notify_type_name": "TRIGGER",
            "ssboe": 1_700_000_000,
        }
    )
    assert notification_action(triggered, order).kind == "triggered"

    not_mod = order_notification_to_fields(
        {
            "type": "order_notification",
            "source": "exchange",
            "symbol": "NQU6",
            "kind": "modify_rejected",
            "notify_type_name": "NOT_MODIFIED",
            "ssboe": 1_700_000_000,
        }
    )
    assert notification_action(not_mod, order).kind == "modify_rejected"


def test_notification_action_reject_fill_cancel_complete() -> None:
    order = _mock_order()
    reject = order_notification_to_fields(
        {
            "type": "order_notification",
            "source": "exchange",
            "symbol": "NQU6",
            "kind": "rejected",
            "notify_type_name": "REJECT",
            "text": "bad",
            "ssboe": 1_700_000_000,
        }
    )
    assert notification_action(reject, order).kind == "rejected"

    fill = order_notification_to_fields(
        {
            "type": "order_notification",
            "source": "exchange",
            "symbol": "NQU6",
            "kind": "filled",
            "notify_type_name": "FILL",
            "fill_price": 21000.0,
            "fill_size": 1,
            "basket_id": "B1",
            "ssboe": 1_700_000_000,
        }
    )
    assert notification_action(fill, order).kind == "filled"

    cancel = order_notification_to_fields(
        {
            "type": "order_notification",
            "source": "exchange",
            "symbol": "NQU6",
            "kind": "canceled",
            "notify_type_name": "CANCEL",
            "ssboe": 1_700_000_000,
        }
    )
    assert notification_action(cancel, order).kind == "canceled"

    complete = order_notification_to_fields(
        {
            "type": "order_notification",
            "source": "rithmic",
            "symbol": "NQU6",
            "kind": "canceled",
            "notify_type_name": "COMPLETE",
            "status": "CANCELLED",
            "ssboe": 1_700_000_000,
        }
    )
    assert notification_action(complete, order).kind == "canceled"


def test_report_policy_never_empty_venue_contract() -> None:
    policy = OrderPlantPolicy(OrderPlantState.LIVE)
    assert policy.use_cache_order_reports() is True
    assert policy.fill_reports_available() is False
    try:
        raise VenueQueryUnavailable("no fill snapshot")
    except VenueQueryUnavailable as exc:
        assert "fill" in str(exc).lower()


def test_resync_blocks_submit_allows_cancel() -> None:
    policy = OrderPlantPolicy(OrderPlantState.RESYNCING)
    assert policy.allow_submit() is False
    assert policy.allow_modify() is False
    assert policy.allow_cancel() is True
    assert "resyncing" in policy.reject_reason("submit")
