"""Unit tests for spawn resolve, env aliases, and history window chunking."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from rithmic_gateway.client import GatewayClient
from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.history_window import (
    BAR_TYPE_DAILY,
    BAR_TYPE_MINUTE,
    bar_slice_secs,
    dedupe_bars_by_marker,
    window_slices,
)
from rithmic_gateway.spawn import SpawnError, resolve_gateway_bin, spawn_gateway


def test_from_env_prefers_gateway_and_system_aliases() -> None:
    cfg = GatewayConfig.from_env(
        {
            "RITHMIC_USER": "u1",
            "RITHMIC_GATEWAY": "wss://preferred.example:443",
            "RITHMIC_URL": "wss://legacy.example:443",
            "RITHMIC_SYSTEM": "LucidTrading",
            "RITHMIC_SYSTEM_NAME": "OtherSystem",
            "RITHMIC_GATEWAY_AUTO_SPAWN": "0",
        }
    )
    assert cfg.url == "wss://preferred.example:443"
    assert cfg.system_name == "LucidTrading"
    assert cfg.auto_spawn is False


def test_from_env_accepts_username_alias() -> None:
    cfg = GatewayConfig.from_env(
        {
            "RITHMIC_USERNAME": "alice",
            "RITHMIC_URL": "wss://rprotocol.rithmic.com:443",
        }
    )
    assert cfg.user == "alice"


def test_from_env_never_treats_listen_as_wss() -> None:
    cfg = GatewayConfig.from_env(
        {
            "RITHMIC_USER": "u",
            "RITHMIC_URL": "wss://rprotocol.rithmic.com:443",
            "RITHMIC_GATEWAY_LISTEN": "unix:///tmp/rgw-test.sock",
        }
    )
    assert cfg.url == "wss://rprotocol.rithmic.com:443"
    assert cfg.listen == "unix:///tmp/rgw-test.sock"


def test_spawn_environ_password_wins_over_stale_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bin_path = tmp_path / "rithmic-gateway"
    bin_path.write_text("#!/bin/sh\n")
    bin_path.chmod(0o755)
    captured: dict[str, str] = {}

    class _Proc:
        pid = 99
        returncode = None
        stderr = None

        def poll(self) -> int | None:
            return None

        def terminate(self) -> None:
            pass

    def _popen(*_a: Any, **kwargs: Any) -> _Proc:
        captured.update(kwargs.get("env") or {})
        return _Proc()

    monkeypatch.setattr("rithmic_gateway.spawn.subprocess.Popen", _popen)
    cfg = GatewayConfig(
        user="u",
        system_name="LucidTrading",
        url="wss://from-config.example",
        env="Live",
        listen=f"unix://{tmp_path / 'g.sock'}",
        gateway_bin=str(bin_path),
        auto_spawn=True,
        spawn_environ={"RITHMIC_PASSWORD": "fresh-from-creds"},
    )
    spawn_gateway(
        cfg,
        wait_socket=False,
        environ={
            "RITHMIC_USER": "u",
            "RITHMIC_PASSWORD": "stale-process-env",
        },
    )
    assert captured["RITHMIC_PASSWORD"] == "fresh-from-creds"


def test_spawn_missing_password_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bin_path = tmp_path / "rithmic-gateway"
    bin_path.write_text("#!/bin/sh\n")
    bin_path.chmod(0o755)
    monkeypatch.setattr(
        "rithmic_gateway.spawn.subprocess.Popen",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("must not spawn")),
    )
    cfg = GatewayConfig(
        user="u",
        system_name="LucidTrading",
        url="wss://example",
        env="Live",
        listen=f"unix://{tmp_path / 'g.sock'}",
        gateway_bin=str(bin_path),
    )
    with pytest.raises(SpawnError, match="RITHMIC_PASSWORD"):
        spawn_gateway(cfg, wait_socket=False, environ={"RITHMIC_USER": "u"})


def test_spawn_overwrites_conflicting_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bin_path = tmp_path / "rithmic-gateway"
    bin_path.write_text("#!/bin/sh\n")
    bin_path.chmod(0o755)
    captured: dict[str, str] = {}

    class _Proc:
        pid = 99
        returncode = None
        stderr = None

        def poll(self) -> int | None:
            return None

        def terminate(self) -> None:
            pass

    def _popen(*_a: Any, **kwargs: Any) -> _Proc:
        captured.update(kwargs.get("env") or {})
        return _Proc()

    monkeypatch.setattr("rithmic_gateway.spawn.subprocess.Popen", _popen)
    cfg = GatewayConfig(
        user="u",
        system_name="LucidTrading",
        url="wss://from-config.example",
        env="Live",
        listen=f"unix://{tmp_path / 'g.sock'}",
        gateway_bin=str(bin_path),
        auto_spawn=True,
    )
    spawn_gateway(
        cfg,
        wait_socket=False,
        environ={
            "RITHMIC_USER": "u",
            "RITHMIC_PASSWORD": "unit-test-secret-zz9",
            "RITHMIC_URL": "wss://stale-env.example",
            "RITHMIC_SYSTEM_NAME": "StaleSystem",
        },
    )
    assert captured["RITHMIC_URL"] == "wss://from-config.example"
    assert captured["RITHMIC_SYSTEM_NAME"] == "LucidTrading"
    assert captured["RITHMIC_PASSWORD"] == "unit-test-secret-zz9"


def test_resolve_gateway_bin_finds_cargo_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release = tmp_path / "target" / "release" / "rithmic-gateway"
    release.parent.mkdir(parents=True)
    release.write_text("#!/bin/sh\n")
    release.chmod(0o755)
    (tmp_path / "Cargo.toml").write_text("[workspace]\n")
    (tmp_path / "crates").mkdir()
    monkeypatch.delenv("RITHMIC_GATEWAY_BIN", raising=False)
    monkeypatch.delenv("CARGO_TARGET_DIR", raising=False)
    monkeypatch.setattr("rithmic_gateway.spawn.shutil.which", lambda _n: None)
    monkeypatch.setattr("rithmic_gateway.spawn._bin_search_starts", lambda: [tmp_path])
    found = resolve_gateway_bin()
    assert Path(found).resolve() == release.resolve()


def test_resolve_gateway_bin_missing_mentions_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("RITHMIC_GATEWAY_BIN", raising=False)
    monkeypatch.delenv("CARGO_TARGET_DIR", raising=False)
    monkeypatch.setattr("rithmic_gateway.spawn.shutil.which", lambda _n: None)
    monkeypatch.setattr("rithmic_gateway.spawn._bin_search_starts", lambda: [tmp_path])
    with pytest.raises(SpawnError, match="RITHMIC_GATEWAY_BIN"):
        resolve_gateway_bin()


def test_window_slices_1m_four_hour() -> None:
    assert bar_slice_secs(BAR_TYPE_MINUTE, 1) == 4 * 60 * 60
    slices = window_slices(0, 12 * 3600, bar_slice_secs(BAR_TYPE_MINUTE, 1))
    assert len(slices) >= 3
    assert slices[0] == (0, 4 * 3600)
    # Adjacent slices share boundary.
    assert slices[0][1] == slices[1][0]


def test_window_slices_daily_single() -> None:
    step = bar_slice_secs(BAR_TYPE_DAILY, 1)
    slices = window_slices(1_700_000_000, 1_700_000_000 + 30 * 86400, step)
    assert len(slices) == 1


def test_dedupe_bars_by_marker() -> None:
    bars = [
        {"marker": 1, "close_price": 1.0},
        {"marker": 1, "close_price": 2.0},
        {"marker": 2, "close_price": 3.0},
    ]
    out = dedupe_bars_by_marker(bars)
    assert len(out) == 2
    assert out[0]["close_price"] == 1.0


def test_load_time_bars_range_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = GatewayConfig(
        user="u",
        system_name="LucidTrading",
        url="wss://example",
        listen="unix:///tmp/rgw-unit-range.sock",
        auto_spawn=False,
        attest_flock=False,
    )
    client = GatewayClient(cfg)
    calls: list[tuple[int, int]] = []

    def _fake_load(
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        bar_type: int = 2,
        period: int = 1,
        *,
        rpc_timeout_sec: float | None = None,
    ) -> list[dict[str, Any]]:
        calls.append((start_ssboe, end_ssboe))
        # Boundary marker shared across adjacent slices.
        return [
            {"marker": start_ssboe, "close_price": 1.0},
            {"marker": end_ssboe, "close_price": 2.0},
        ]

    monkeypatch.setattr(client, "load_time_bars", _fake_load)
    start = 0
    end = 12 * 3600
    bars = client.load_time_bars_range(
        "NQU6", "CME", start, end, bar_type=2, period=1, max_workers=1
    )
    assert len(calls) >= 3
    markers = [b["marker"] for b in bars]
    assert markers == sorted(set(markers))


def test_load_time_bars_range_async(monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncio

    cfg = GatewayConfig(
        user="u",
        system_name="LucidTrading",
        url="wss://example",
        listen="unix:///tmp/rgw-unit-range-async.sock",
        auto_spawn=False,
        attest_flock=False,
    )
    client = GatewayClient(cfg)

    def _fake_load(
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        bar_type: int = 2,
        period: int = 1,
        *,
        rpc_timeout_sec: float | None = None,
    ) -> list[dict[str, Any]]:
        return [{"marker": start_ssboe, "close_price": 1.0}]

    monkeypatch.setattr(client, "load_time_bars", _fake_load)
    bars = asyncio.run(
        client.load_time_bars_range_async(
            "NQU6", "CME", 0, 3600, bar_type=2, period=1, max_workers=1
        )
    )
    assert len(bars) == 1
