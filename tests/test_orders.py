"""Unit tests for Phase 2 order mapping and readonly-vs-trading config."""

from __future__ import annotations

from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce

from rithmic_connect._orders import OrderMapError
from rithmic_connect._orders import is_exchange_fill
from rithmic_connect._orders import is_rithmic_open
from rithmic_connect._orders import nautilus_order_type_to_rithmic
from rithmic_connect._orders import nautilus_side_to_rithmic
from rithmic_connect._orders import nautilus_tif_to_rithmic
from rithmic_connect._orders import order_notification_to_fields
from rithmic_connect.config import RithmicExecClientConfig
from rithmic_connect.config import SessionConfig


def test_side_type_tif_mapping():
    assert nautilus_side_to_rithmic(OrderSide.BUY) == "BUY"
    assert nautilus_side_to_rithmic(OrderSide.SELL) == "SELL"
    assert nautilus_order_type_to_rithmic(OrderType.LIMIT) == "LIMIT"
    assert nautilus_order_type_to_rithmic(OrderType.MARKET) == "MARKET"
    assert nautilus_order_type_to_rithmic(OrderType.STOP_LIMIT) == "STOP_LIMIT"
    assert nautilus_tif_to_rithmic(TimeInForce.DAY) == "DAY"
    assert nautilus_tif_to_rithmic(TimeInForce.GTC) == "GTC"


def test_unsupported_order_type_raises():
    try:
        nautilus_order_type_to_rithmic(OrderType.TRAILING_STOP_MARKET)
        raised = False
    except OrderMapError:
        raised = True
    assert raised


def test_order_notification_fields_require_symbol_and_source():
    fields = order_notification_to_fields(
        {
            "type": "order_notification",
            "source": "exchange",
            "symbol": "NQU6",
            "exchange": "CME",
            "notify_type": 5,
            "notify_type_name": "FILL",
            "fill_price": 21000.25,
            "fill_size": 1,
            "basket_id": "B1",
            "ssboe": 1_700_000_000,
            "usecs": 0,
        }
    )
    assert fields["instrument_id"] == "NQU6.RITHMIC"
    assert is_exchange_fill(fields)
    assert fields["ts_event"] == 1_700_000_000 * 1_000_000_000


def test_rithmic_open_detection():
    fields = order_notification_to_fields(
        {
            "source": "rithmic",
            "symbol": "NQU6",
            "notify_type": 13,
            "notify_type_name": "OPEN",
            "basket_id": "B2",
            "ssboe": 1_700_000_000,
        }
    )
    assert is_rithmic_open(fields)


def test_exec_config_enable_trading_default_false():
    cfg = RithmicExecClientConfig(
        session=SessionConfig(user="u", password="p"),
    )
    assert cfg.enable_trading is False


def test_exec_config_from_env_enable_trading():
    cfg = RithmicExecClientConfig.from_env(
        {
            "RITHMIC_USER": "u",
            "RITHMIC_PASSWORD": "p",
            "RITHMIC_ENABLE_TRADING": "true",
        }
    )
    assert cfg.enable_trading is True


def test_wire_session_documents_order_apis():
    import inspect

    from rithmic_connect import session as sess_mod

    src = inspect.getsource(sess_mod.WireSession)
    assert "place_order" in src
    assert "cancel_order" in src
    assert "modify_order" in src
    assert "poll_order_event" in src
