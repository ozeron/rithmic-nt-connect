"""Retired gateway client knobs fail fast; values equal to the sole remaining
behavior are accepted as no-ops (openspec: simplify-gateway-modes)."""

from __future__ import annotations

import logging

import pytest
from rithmic_gateway import config as gw_config
from rithmic_gateway.config import GatewayConfig, GatewayConfigError
from rithmic_nt_connect import config as nt_config
from rithmic_nt_connect.config import ConfigError, SessionConfig

_GATEWAY_ENV = {
    "RITHMIC_USER": "u",
    "RITHMIC_SYSTEM_NAME": "LucidTrading",
    "RITHMIC_URL": "wss://example",
}

_SESSION_ENV = {
    "RITHMIC_USER": "u",
    "RITHMIC_CONNECT_MODE": "gateway",
    "RITHMIC_GATEWAY_LISTEN": "unix:///tmp/rgw-retired-knobs.sock",
}


def _gateway(**extra: str) -> GatewayConfig:
    return GatewayConfig.from_env({**_GATEWAY_ENV, **extra})


def _session(**extra: str) -> SessionConfig:
    return SessionConfig.from_env({**_SESSION_ENV, **extra})


@pytest.fixture(autouse=True)
def _reset_deprecation_state() -> None:
    gw_config._DEPRECATION_WARNED.clear()
    nt_config._DEPRECATION_WARNED.clear()


def test_the_adapter_and_the_lake_reject_the_same_knobs() -> None:
    """Each package keeps its own table (neither may import the other)."""
    assert nt_config._RETIRED_KNOBS == gw_config._RETIRED_KNOBS


# -- removed behavior: fail fast ------------------------------------------------


@pytest.mark.parametrize(
    ("name", "value", "needle"),
    [
        ("RITHMIC_GATEWAY_CLIENT_MODE", "dual", "dual"),
        ("RITHMIC_GATEWAY_CLIENT_MODE", "DUAL", "dual"),
        ("RITHMIC_GATEWAY_SPAWN_POLICY", "if_missing", "spawn"),
        ("RITHMIC_GATEWAY_SPAWN_POLICY", "auto", "spawn"),
        ("RITHMIC_GATEWAY_SPAWN_POLICY", "default", "spawn"),
        ("RITHMIC_GATEWAY_AUTO_SPAWN", "1", "spawn"),
        ("RITHMIC_GATEWAY_AUTO_SPAWN", "true", "spawn"),
        ("RITHMIC_GATEWAY_AUTO_SPAWN", "yes", "spawn"),
        ("RITHMIC_GATEWAY_AUTO_SPAWN", "on", "spawn"),
    ],
)
def test_removed_behavior_raises_config_error(
    name: str, value: str, needle: str
) -> None:
    with pytest.raises(GatewayConfigError, match=needle):
        _gateway(**{name: value})
    with pytest.raises(ConfigError, match=needle):
        _session(**{name: value})


def test_spawn_error_tells_operator_to_start_the_parent() -> None:
    with pytest.raises(GatewayConfigError, match="start the parent"):
        _gateway(RITHMIC_GATEWAY_AUTO_SPAWN="1")


def test_dual_error_says_mux_is_the_only_mode() -> None:
    with pytest.raises(GatewayConfigError, match="mux"):
        _gateway(RITHMIC_GATEWAY_CLIENT_MODE="dual")


# -- sole remaining behavior: accepted no-op ------------------------------------


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("RITHMIC_GATEWAY_CLIENT_MODE", "mux"),
        ("RITHMIC_GATEWAY_SPAWN_POLICY", "never"),
        ("RITHMIC_GATEWAY_AUTO_SPAWN", "0"),
        ("RITHMIC_GATEWAY_AUTO_SPAWN", "false"),
        ("RITHMIC_GATEWAY_AUTO_SPAWN", "no"),
        ("RITHMIC_GATEWAY_AUTO_SPAWN", "off"),
    ],
)
def test_sole_behavior_values_are_accepted(name: str, value: str) -> None:
    assert _gateway(**{name: value}).user == "u"
    assert _session(**{name: value}).user == "u"


def test_unset_knobs_are_accepted() -> None:
    assert _gateway().user == "u"
    assert _session().user == "u"


# -- invalid values ------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("RITHMIC_GATEWAY_CLIENT_MODE", "pooled"),
        ("RITHMIC_GATEWAY_SPAWN_POLICY", "nevver"),
        ("RITHMIC_GATEWAY_AUTO_SPAWN", "maybe"),
    ],
)
def test_unrecognized_value_is_invalid(name: str, value: str) -> None:
    with pytest.raises(GatewayConfigError, match=f"invalid {name}"):
        _gateway(**{name: value})


# -- one-time deprecation warning for accepted no-ops --------------------------


def test_accepted_noop_logs_one_deprecation_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="rithmic_gateway.config")
    _gateway(RITHMIC_GATEWAY_CLIENT_MODE="mux")
    _gateway(RITHMIC_GATEWAY_CLIENT_MODE="mux")
    records = [r for r in caplog.records if "RITHMIC_GATEWAY_CLIENT_MODE" in r.message]
    assert len(records) == 1
    assert "deprecated" in records[0].message


def test_unset_knobs_do_not_warn(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="rithmic_gateway.config")
    _gateway()
    assert not [r for r in caplog.records if "deprecated" in r.message]


# -- library surface -----------------------------------------------------------


def test_gateway_config_has_no_spawn_fields() -> None:
    import dataclasses

    names = {f.name for f in dataclasses.fields(GatewayConfig)}
    assert not {n for n in names if "spawn" in n or "environ" in n}
    assert "attach_timeout_sec" in names
    assert "gateway_bin" not in names


def test_session_config_has_no_spawn_or_dual_fields() -> None:
    import dataclasses

    names = {f.name for f in dataclasses.fields(SessionConfig)}
    assert not {n for n in names if "spawn" in n or "client_mode" in n}
    assert "gateway_bin" not in names
