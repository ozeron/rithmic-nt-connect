"""Tests for gateway WireSession adapter and dual-mode create_session."""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from rithmic_nt_connect.config import ConfigError, ConnectMode, SessionConfig
from rithmic_nt_connect.gateway_wire import GatewayWireSession, gateway_config_from_session
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
    ):
        assert hasattr(session, name), f"gateway wire missing {name} (direct/gateway parity)"
