"""Retired gateway client knobs fail fast for any set value; unset is the only
valid state (openspec: retire-python-mux-and-noop-knobs)."""

from __future__ import annotations

import dataclasses

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


def test_the_adapter_and_the_lake_reject_the_same_knobs() -> None:
    """Each package keeps its own table (neither may import the other)."""
    assert nt_config._RETIRED_KNOBS == gw_config._RETIRED_KNOBS


# Values that asked for removed behavior and values that used to be accepted as
# deprecated no-ops both raise now.
_RETIRED_VALUES = [
    ("RITHMIC_GATEWAY_CLIENT_MODE", "dual", "one topology"),
    ("RITHMIC_GATEWAY_CLIENT_MODE", "DUAL", "one topology"),
    ("RITHMIC_GATEWAY_CLIENT_MODE", "mux", "one topology"),
    ("RITHMIC_GATEWAY_CLIENT_MODE", "pooled", "one topology"),
    ("RITHMIC_GATEWAY_SPAWN_POLICY", "if_missing", "no longer spawns"),
    ("RITHMIC_GATEWAY_SPAWN_POLICY", "auto", "no longer spawns"),
    ("RITHMIC_GATEWAY_SPAWN_POLICY", "default", "no longer spawns"),
    ("RITHMIC_GATEWAY_SPAWN_POLICY", "never", "no longer spawns"),
    ("RITHMIC_GATEWAY_SPAWN_POLICY", "nevver", "no longer spawns"),
    ("RITHMIC_GATEWAY_AUTO_SPAWN", "1", "no longer spawns"),
    ("RITHMIC_GATEWAY_AUTO_SPAWN", "true", "no longer spawns"),
    ("RITHMIC_GATEWAY_AUTO_SPAWN", "0", "no longer spawns"),
    ("RITHMIC_GATEWAY_AUTO_SPAWN", "false", "no longer spawns"),
    ("RITHMIC_GATEWAY_AUTO_SPAWN", "off", "no longer spawns"),
    ("RITHMIC_GATEWAY_AUTO_SPAWN", "maybe", "no longer spawns"),
]


@pytest.mark.parametrize(("name", "value", "needle"), _RETIRED_VALUES)
def test_any_set_value_raises_on_both_configs(
    name: str, value: str, needle: str
) -> None:
    with pytest.raises(GatewayConfigError, match=needle) as gw:
        _gateway(**{name: value})
    with pytest.raises(ConfigError, match=needle) as nt:
        _session(**{name: value})
    assert name in str(gw.value)
    assert str(gw.value) == str(nt.value)


def test_spawn_error_tells_operator_to_start_the_parent() -> None:
    with pytest.raises(GatewayConfigError, match="python -m rithmic_gateway"):
        _gateway(RITHMIC_GATEWAY_AUTO_SPAWN="1")


@pytest.mark.parametrize("name", sorted(gw_config._RETIRED_KNOBS))
def test_empty_value_counts_as_unset(name: str) -> None:
    assert _gateway(**{name: ""}).user == "u"
    assert _session(**{name: "  "}).user == "u"


def test_unset_knobs_are_accepted() -> None:
    assert _gateway().user == "u"
    assert _session().user == "u"


# -- library surface -----------------------------------------------------------


def test_gateway_config_has_no_spawn_fields() -> None:
    names = {f.name for f in dataclasses.fields(GatewayConfig)}
    assert not {n for n in names if "spawn" in n or "environ" in n}
    assert "attach_timeout_sec" in names
    assert "gateway_bin" not in names


def test_session_config_has_no_spawn_or_dual_fields() -> None:
    names = {f.name for f in dataclasses.fields(SessionConfig)}
    assert not {n for n in names if "spawn" in n or "client_mode" in n}
    assert "gateway_bin" not in names
