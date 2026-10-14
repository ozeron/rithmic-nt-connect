"""Supported-modes contract: explicit connect mode, unix-only transport."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, cast

import pytest
from rithmic_gateway.config import GatewayConfig, GatewayConfigError, parse_listen_url
from rithmic_nt_connect.config import ConfigError, ConnectMode, SessionConfig

_BASE = {"RITHMIC_USER": "alice", "RITHMIC_PASSWORD": "pw"}


# -- connect mode: explicit, direct | gateway, no default -----------------------


def test_connect_mode_unset_is_an_error_naming_the_setting() -> None:
    with pytest.raises(ConfigError, match="RITHMIC_CONNECT_MODE"):
        SessionConfig.from_env(dict(_BASE))


def test_connect_mode_blank_is_treated_as_unset() -> None:
    with pytest.raises(ConfigError, match="RITHMIC_CONNECT_MODE"):
        SessionConfig.from_env({**_BASE, "RITHMIC_CONNECT_MODE": "  "})


@pytest.mark.parametrize("value", ["pooled", "dual", "auto", "mux", "both"])
def test_connect_mode_unknown_value_does_not_fall_back(value: str) -> None:
    with pytest.raises(ConfigError, match="RITHMIC_CONNECT_MODE"):
        SessionConfig.from_env({**_BASE, "RITHMIC_CONNECT_MODE": value})


@pytest.mark.parametrize("value", ["pooled", "", "auto"])
def test_connect_mode_unknown_value_rejected_by_constructor(value: str) -> None:
    with pytest.raises(ConfigError, match="connect_mode"):
        SessionConfig(user="alice", password="pw", connect_mode=cast(Any, value))


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("direct", ConnectMode.DIRECT),
        ("gateway", ConnectMode.GATEWAY),
        (" Gateway ", ConnectMode.GATEWAY),
        ("DIRECT", ConnectMode.DIRECT),
    ],
)
def test_connect_mode_accepts_direct_and_gateway(
    value: str, expected: ConnectMode
) -> None:
    cfg = SessionConfig.from_env({**_BASE, "RITHMIC_CONNECT_MODE": value})
    assert cfg.connect_mode is expected


def test_connect_mode_has_exactly_two_members() -> None:
    assert {m.value for m in ConnectMode} == {"direct", "gateway"}


# -- gateway transport: unix only -----------------------------------------------


@pytest.mark.parametrize(
    "url", ["tcp://0.0.0.0:7600", "tls://gw.example:7600", " tcp://127.0.0.1:1 "]
)
def test_tcp_and_tls_listen_urls_are_not_implemented(url: str) -> None:
    with pytest.raises(GatewayConfigError, match="not implemented"):
        parse_listen_url(url)


def test_tcp_listen_url_rejected_at_config_construction() -> None:
    with pytest.raises(GatewayConfigError, match="not implemented"):
        GatewayConfig(
            user="u",
            system_name="LucidTrading",
            url="wss://example",
            listen="tcp://127.0.0.1:7600",
            attest_flock=False,
        )


def test_relative_unix_listen_url_resolves_absolute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    resolved = parse_listen_url("unix://run/gw.sock")
    assert os.path.isabs(resolved)
    assert Path(resolved) == (tmp_path / "run" / "gw.sock").resolve()


def test_absolute_unix_listen_url_is_unchanged() -> None:
    assert parse_listen_url("unix:///tmp/rgw.sock") == "/tmp/rgw.sock"


def test_unknown_listen_scheme_is_rejected() -> None:
    with pytest.raises(GatewayConfigError, match="unix://"):
        parse_listen_url("http://example")
