"""Unit tests for Phase 2 order mapping and notification classification."""

from __future__ import annotations

from types import SimpleNamespace

from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

from rithmic_nt_connect._convert import ConvertError
from rithmic_nt_connect._order_plant import OrderPlantPolicy
from rithmic_nt_connect._order_plant import OrderPlantState
from rithmic_nt_connect._orders import OrderMapError
from rithmic_nt_connect._orders import kind_from_notify
from rithmic_nt_connect._orders import nautilus_order_type_to_rithmic
from rithmic_nt_connect._orders import nautilus_side_to_rithmic
from rithmic_nt_connect._orders import nautilus_tif_to_rithmic
from rithmic_nt_connect._orders import notification_action
from rithmic_nt_connect._orders import order_notification_to_fields
from rithmic_nt_connect._orders import trade_id_from_fill_fields
from rithmic_nt_connect.config import ConnectMode
from rithmic_nt_connect.config import RithmicExecClientConfig
from rithmic_nt_connect.config import SessionConfig


def _order() -> SimpleNamespace:
    return SimpleNamespace(
        quantity=Quantity.from_int(1),
        has_price=True,
        price=Price.from_str("21000.0"),
        has_trigger_price=False,
        trigger_price=None,
    )


def test_side_type_tif_mapping():
    assert nautilus_side_to_rithmic(OrderSide.BUY) == "BUY"
    assert nautilus_side_to_rithmic(OrderSide.SELL) == "SELL"
    assert nautilus_order_type_to_rithmic(OrderType.LIMIT) == "LIMIT"
    assert nautilus_order_type_to_rithmic(OrderType.MARKET) == "MARKET"
    assert nautilus_order_type_to_rithmic(OrderType.STOP_LIMIT) == "STOP_LIMIT"
    assert nautilus_order_type_to_rithmic(OrderType.TRAILING_STOP_MARKET) == "STOP_MARKET"
    assert nautilus_order_type_to_rithmic(OrderType.TRAILING_STOP_LIMIT) == "STOP_LIMIT"
    assert nautilus_tif_to_rithmic(TimeInForce.DAY) == "DAY"
    assert nautilus_tif_to_rithmic(TimeInForce.GTC) == "GTC"


def test_unsupported_order_type_raises():
    try:
        nautilus_order_type_to_rithmic(OrderType.MARKET_TO_LIMIT)
        raised = False
    except OrderMapError:
        raised = True
    assert raised


def test_trailing_ticks_from_order():
    from decimal import Decimal

    from nautilus_trader.model.enums import TrailingOffsetType

    from rithmic_nt_connect._orders import trailing_ticks_from_order

    plain = SimpleNamespace(order_type=OrderType.STOP_MARKET)
    assert trailing_ticks_from_order(plain) is None

    trailing = SimpleNamespace(
        order_type=OrderType.TRAILING_STOP_MARKET,
        trailing_offset_type=TrailingOffsetType.TICKS,
        trailing_offset=Decimal("20"),
    )
    assert trailing_ticks_from_order(trailing) == 20

    bad_type = SimpleNamespace(
        order_type=OrderType.TRAILING_STOP_LIMIT,
        trailing_offset_type=TrailingOffsetType.PRICE,
        trailing_offset=Decimal("5"),
    )
    try:
        trailing_ticks_from_order(bad_type)
        raised = False
    except OrderMapError:
        raised = True
    assert raised


def test_order_notification_requires_type_and_source():
    try:
        order_notification_to_fields(
            {"type": "last_trade", "source": "rithmic", "symbol": "NQU6"}
        )
        raised = False
    except ConvertError:
        raised = True
    assert raised


def test_order_notification_fields_and_kind():
    fields = order_notification_to_fields(
        {
            "type": "order_notification",
            "source": "exchange",
            "symbol": "NQU6",
            "exchange": "CME",
            "kind": "filled",
            "notify_type_name": "FILL",
            "fill_price": 21000.25,
            "fill_size": 1,
            "basket_id": "B1",
            "ssboe": 1_700_000_000,
            "usecs": 0,
        }
    )
    assert fields["instrument_id"] == "NQU6.RITHMIC"
    assert fields["kind"] == "filled"
    assert fields["ts_event"] == 1_700_000_000 * 1_000_000_000


def test_kind_from_notify_covers_main_paths():
    assert kind_from_notify("rithmic", "OPEN") == "accepted"
    assert kind_from_notify("rithmic", "MODIFIED") == "updated"
    assert kind_from_notify("exchange", "FILL") == "filled"
    assert kind_from_notify("exchange", "REJECT") == "rejected"
    assert kind_from_notify("exchange", "CANCEL") == "canceled"
    assert kind_from_notify("exchange", "TRIGGER") == "triggered"
    assert kind_from_notify("exchange", "NOT_MODIFIED") == "modify_rejected"
    assert kind_from_notify("exchange", "NOT_CANCELLED") == "cancel_rejected"
    assert kind_from_notify("rithmic", "COMPLETE", "CANCELLED") == "canceled"
    assert kind_from_notify("rithmic", "COMPLETE", "FILLED") is None
    assert kind_from_notify("exchange", "UNKNOWN") is None


def test_notification_action_accept_reject_fill_cancel():
    order = _order()
    accepted = notification_action({"kind": "accepted"}, order)
    assert accepted is not None and accepted.kind == "accepted"

    rejected = notification_action({"kind": "rejected", "text": "nope"}, order)
    assert rejected is not None and rejected.kind == "rejected" and rejected.reason == "nope"

    filled = notification_action(
        {
            "kind": "filled",
            "fill_price": 100.5,
            "fill_size": 2,
            "basket_id": "B1",
            "ts_event": 10,
        },
        order,
    )
    assert filled is not None and filled.kind == "filled"
    assert filled.fill_qty == 2
    assert filled.trade_id is not None

    canceled = notification_action({"kind": "canceled"}, order)
    assert canceled is not None and canceled.kind == "canceled"

    assert notification_action({"kind": "filled", "fill_price": 1.0}, order) is None
    assert notification_action({"kind": None}, order) is None


def test_trade_id_unique_without_fill_id():
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


def test_order_plant_policy_matrix():
    live = OrderPlantPolicy(OrderPlantState.LIVE)
    assert live.allow_submit() and live.allow_modify() and live.allow_cancel()
    assert live.use_cache_order_reports()
    assert not live.fill_reports_available()

    resync = OrderPlantPolicy(OrderPlantState.RESYNCING)
    assert not resync.allow_submit()
    assert not resync.allow_modify()
    assert resync.allow_cancel()

    down = OrderPlantPolicy(OrderPlantState.DISCONNECTED)
    assert not down.allow_submit()
    assert not down.allow_modify()
    assert not down.allow_cancel()


def test_exec_config_enable_trading_default_false():
    cfg = RithmicExecClientConfig(
        session=SessionConfig(user="u", password="p", connect_mode=ConnectMode.DIRECT),
    )
    assert cfg.enable_trading is False


def test_exec_config_from_env_enable_trading():
    cfg = RithmicExecClientConfig.from_env(
        {
            "RITHMIC_USER": "u",
            "RITHMIC_PASSWORD": "p",
            "RITHMIC_ENABLE_TRADING": "true",
            "RITHMIC_CONNECT_MODE": "direct",
        }
    )
    assert cfg.enable_trading is True
