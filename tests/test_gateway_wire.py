"""Tests for gateway WireSession adapter and dual-mode create_session."""

from __future__ import annotations

import inspect
from typing import Any

import pytest
from rithmic_gateway.types import AccountRmsInfo, ProductRmsInfo
from rithmic_nt_connect.config import ConfigError, ConnectMode, SessionConfig
from rithmic_nt_connect.gateway_wire import (
    GatewayWireSession,
    gateway_config_from_session,
)
from rithmic_nt_connect.session import create_session


def _cfg(**kwargs: object) -> SessionConfig:
    # ``connect_mode`` accepts a ``str`` at runtime (``__post_init__`` coerces);
    # ``Any`` keeps the override dict valid for both str and ConnectMode values.
    base: dict[str, Any] = {
        "user": "alice",
        "password": "pw",
        "system_name": "LucidTrading",
        "url": "wss://rprotocol.rithmic.com:443",
        "connect_mode": ConnectMode.DIRECT,
    }
    base.update(kwargs)
    return SessionConfig(**base)


def test_connect_mode_required() -> None:
    # ``connect_mode`` carries no default: the dataclass contract requires it.
    params = inspect.signature(SessionConfig).parameters
    assert params["connect_mode"].default is inspect.Parameter.empty


def test_connect_mode_invalid() -> None:
    with pytest.raises(ConfigError, match="connect_mode"):
        _cfg(connect_mode="pooled")


def test_connect_mode_coerces_string() -> None:
    cfg = _cfg(connect_mode="gateway")
    assert cfg.connect_mode is ConnectMode.GATEWAY
    assert cfg.connect_mode == "gateway"


def test_connect_mode_from_env() -> None:
    cfg = SessionConfig.from_env(
        {
            "RITHMIC_USER": "alice",
            "RITHMIC_PASSWORD": "pw",
            "RITHMIC_CONNECT_MODE": "gateway",
            "RITHMIC_GATEWAY_LISTEN": "unix:///tmp/rgw.sock",
            "RITHMIC_GATEWAY_AUTO_SPAWN": "0",
        }
    )
    assert cfg.connect_mode is ConnectMode.GATEWAY
    assert cfg.gateway_listen == "unix:///tmp/rgw.sock"
    assert cfg.gateway_auto_spawn is False


def test_connect_mode_missing_from_env() -> None:
    with pytest.raises(ConfigError, match="RITHMIC_CONNECT_MODE"):
        SessionConfig.from_env(
            {
                "RITHMIC_USER": "alice",
                "RITHMIC_PASSWORD": "pw",
            }
        )


def test_gateway_config_fingerprint() -> None:
    cfg = _cfg(connect_mode=ConnectMode.GATEWAY, gateway_listen="unix:///tmp/x.sock")
    gcfg = gateway_config_from_session(cfg)
    assert gcfg.user == "alice"
    assert gcfg.listen == "unix:///tmp/x.sock"
    assert gcfg.auto_spawn is True


def test_create_session_gateway_returns_adapter_without_pyo3() -> None:
    cfg = _cfg(
        connect_mode=ConnectMode.GATEWAY,
        gateway_listen="unix:///tmp/no-such.sock",
        gateway_auto_spawn=False,
    )
    session = create_session(cfg)
    assert isinstance(session, GatewayWireSession)
    for name in (
        "place_bracket_order",
        "subscribe_bracket_updates",
        "adjust_bracket_stop",
        "adjust_bracket_target",
        "resolved_account",
        "load_product_rms_info",
        "load_account_rms_info",
        "reset_ticker",
        "reset_ticker_plant",
    ):
        assert hasattr(session, name), (
            f"gateway wire missing {name} (direct/gateway parity)"
        )


class _RecordingGatewayClient:
    """GatewayClient double: records every intent-channel call the façade
    forwards (the adapter's reconnect sequence re-issues these after a drop)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.trading_enabled = True
        self.cancel_all_enabled = False

    def subscribe(self, *args: object) -> None:
        self.calls.append(("ticker", args))

    def subscribe_order_book_summary(self, *args: object) -> None:
        self.calls.append(("book", args))

    def subscribe_time_bars(self, *args: object) -> None:
        self.calls.append(("time_bars", args))

    def subscribe_pnl(self) -> None:
        self.calls.append(("pnl", ()))

    def subscribe_order_updates(self) -> None:
        self.calls.append(("order", ()))

    def subscribe_bracket_updates(self) -> None:
        self.calls.append(("brackets", ()))

    def load_product_rms_info(self) -> list[ProductRmsInfo]:
        self.calls.append(("load_product_rms_info", ()))
        return []

    def load_account_rms_info(self) -> list[AccountRmsInfo]:
        self.calls.append(("load_account_rms_info", ()))
        return []

    def disconnect_order_plant(self) -> None:
        self.calls.append(("disconnect_order_plant", ()))

    def disconnect_pnl_plant(self) -> None:
        self.calls.append(("disconnect_pnl_plant", ()))

    def disconnect(self) -> None:
        self.calls.append(("disconnect", ()))

    def connect(self) -> None:
        self.calls.append(("connect", ()))

    def reset_ticker_plant(self) -> None:
        self.calls.append(("reset_ticker_plant", ()))


def test_gateway_wire_restore_covers_every_intent_channel() -> None:
    """P4: the Python façade re-issues every intent channel after a plant
    drop — ticker, book, time bars, PnL, order, brackets — mirroring
    ``reconnect.rs::restore_plan_covers_every_intent_channel``. Order and
    bracket intents both ride the order-plant stream."""
    inner = _RecordingGatewayClient()
    session = GatewayWireSession(inner)  # type: ignore[arg-type]

    # Plant drop -> the adapter's resync sequence re-arms each stream.
    session.disconnect_order_plant()
    session.disconnect_pnl_plant()
    session.subscribe("NQ", "CME")
    session.subscribe_order_book_summary("NQ", "CME")
    session.subscribe_time_bars("NQ", "CME", 2, 1)
    session.subscribe_pnl()
    session.subscribe_order_updates()
    session.subscribe_bracket_updates()

    kinds = [kind for kind, _ in inner.calls]
    assert kinds == [
        "disconnect_order_plant",
        "disconnect_pnl_plant",
        "ticker",
        "book",
        "time_bars",
        "pnl",
        "order",
        "brackets",
    ]
    assert set(kinds) >= {
        "ticker",
        "book",
        "time_bars",
        "pnl",
        "order",
        "brackets",
    }


def test_gateway_wire_forwards_rms_fetches() -> None:
    """Parity: the gateway façade forwards the RMS fetch RPCs to the client
    (commission rates ride the same wire surface on both modes)."""
    inner = _RecordingGatewayClient()
    session = GatewayWireSession(inner)  # type: ignore[arg-type]

    product = session.load_product_rms_info()
    account = session.load_account_rms_info()

    assert product == []
    assert account == []
    assert inner.calls == [
        ("load_product_rms_info", ()),
        ("load_account_rms_info", ()),
    ]


def test_gateway_wire_reset_ticker_detaches_and_redials_client_only() -> None:
    """Gateway ``reset_ticker`` recovers THIS client's ticker stream by
    detach + re-dial, never tearing down parent plants for peers."""
    inner = _RecordingGatewayClient()
    session = GatewayWireSession(inner)  # type: ignore[arg-type]

    session.reset_ticker()

    assert inner.calls == [("disconnect", ()), ("connect", ())]


def test_gateway_wire_forwards_reset_ticker_plant_rpc() -> None:
    """Parity: the gateway wire exposes the plant-level ticker reset RPC the
    direct path has (the adapter's own gateway resync prefers the client-level
    ``reset_ticker``, but the surface must not drift)."""
    inner = _RecordingGatewayClient()
    session = GatewayWireSession(inner)  # type: ignore[arg-type]

    session.reset_ticker_plant()

    assert inner.calls == [("reset_ticker_plant", ())]
