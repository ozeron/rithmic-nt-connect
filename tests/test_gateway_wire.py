"""Tests for gateway WireSession adapter and dual-mode create_session."""

from __future__ import annotations

from rithmic_nt_connect.config import SessionConfig
from rithmic_nt_connect.gateway_wire import GatewayWireSession, gateway_config_from_session
from rithmic_nt_connect.session import create_session


def _cfg(**kwargs: object) -> SessionConfig:
    base = dict(
        user="alice",
        password="pw",
        system_name="LucidTrading",
        url="wss://rprotocol.rithmic.com:443",
    )
    base.update(kwargs)
    return SessionConfig(**base)  # type: ignore[arg-type]


def test_session_mode_defaults_direct() -> None:
    cfg = _cfg()
    assert cfg.session_mode == "direct"


def test_session_mode_from_env() -> None:
    cfg = SessionConfig.from_env(
        {
            "RITHMIC_USER": "alice",
            "RITHMIC_PASSWORD": "pw",
            "RITHMIC_SESSION_MODE": "gateway",
            "RITHMIC_GATEWAY_LISTEN": "unix:///tmp/rgw.sock",
            "RITHMIC_GATEWAY_AUTO_SPAWN": "0",
        }
    )
    assert cfg.session_mode == "gateway"
    assert cfg.gateway_listen == "unix:///tmp/rgw.sock"
    assert cfg.gateway_auto_spawn is False


def test_gateway_config_fingerprint() -> None:
    cfg = _cfg(session_mode="gateway", gateway_listen="unix:///tmp/x.sock")
    gcfg = gateway_config_from_session(cfg)
    assert gcfg.user == "alice"
    assert gcfg.listen == "unix:///tmp/x.sock"
    assert gcfg.auto_spawn is True


def test_create_session_gateway_returns_adapter_without_pyo3() -> None:
    cfg = _cfg(session_mode="gateway", gateway_listen="unix:///tmp/no-such.sock", gateway_auto_spawn=False)
    session = create_session(cfg)
    assert isinstance(session, GatewayWireSession)
