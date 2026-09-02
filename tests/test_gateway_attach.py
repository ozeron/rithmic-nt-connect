"""Unit tests for gateway attach coordinator and spawn policy."""

from __future__ import annotations

import importlib.util
import os
import threading
import time
from pathlib import Path

import pytest
from rithmic_gateway.attach import (
    AttachError,
    GatewayAttachCoordinator,
    wait_for_parent_socket,
)
from rithmic_gateway.config import GatewayConfig, parse_spawn_policy
from rithmic_gateway.flock import SessionLock
from rithmic_gateway.spawn import spawn_gateway

_shared = importlib.util.spec_from_file_location(
    "test_gateway_shared_consumers",
    Path(__file__).with_name("test_gateway_shared_consumers.py"),
)
assert _shared and _shared.loader
_shared_mod = importlib.util.module_from_spec(_shared)
_shared.loader.exec_module(_shared_mod)
_serve_shared_md_parent = _shared_mod._serve_shared_md_parent


def test_parse_spawn_policy_auto_spawn_zero_is_never() -> None:
    assert parse_spawn_policy({"RITHMIC_GATEWAY_AUTO_SPAWN": "0"}) == "never"
    assert parse_spawn_policy({"RITHMIC_GATEWAY_AUTO_SPAWN": "1"}) == "if_missing"
    assert parse_spawn_policy({}) == "if_missing"
    assert parse_spawn_policy({"RITHMIC_GATEWAY_SPAWN_POLICY": "never"}) == "never"


def test_spawn_policy_never_dial_fail_no_popen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    popen_calls: list[object] = []

    def _boom(*_a: object, **_k: object) -> object:
        popen_calls.append(1)
        raise AssertionError("Popen must not run when spawn policy is never")

    monkeypatch.setattr("rithmic_gateway.spawn.subprocess.Popen", _boom)
    cfg = GatewayConfig(
        user="u-never",
        system_name="LucidTrading",
        url="wss://example",
        listen="unix:///tmp/rgw-never-missing.sock",
        spawn_policy="never",
        attest_flock=False,
    )
    with pytest.raises(AttachError) as exc:
        GatewayAttachCoordinator.resolve_dial_failure(cfg)
    assert exc.value.code == "dial_failed_spawn_disabled"
    assert popen_calls == []


def test_from_env_auto_spawn_zero_sets_never() -> None:
    cfg = GatewayConfig.from_env({"RITHMIC_GATEWAY_AUTO_SPAWN": "0", **_minimal_env()})
    assert cfg.spawn_policy == "never"
    assert cfg.auto_spawn is False


def test_spawn_policy_typo_raises() -> None:
    from typing import Any, cast

    from rithmic_gateway.config import GatewayConfigError

    with pytest.raises(GatewayConfigError, match="spawn_policy"):
        GatewayConfig(
            user="u-typo",
            system_name="LucidTrading",
            url="wss://example",
            spawn_policy=cast(Any, "nevver"),
            attest_flock=False,
        )


def test_auto_spawn_false_beats_default_if_missing_policy() -> None:
    cfg = GatewayConfig(
        user="u-legacy-dial",
        system_name="LucidTrading",
        url="wss://example",
        auto_spawn=False,
        spawn_policy="if_missing",
        attest_flock=False,
    )
    assert cfg.spawn_policy == "never"
    assert cfg.auto_spawn is False


def test_flock_held_socket_late_zero_popen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sock = Path(f"/tmp/rgw-attach-late-{os.getpid()}.sock")
    popen_calls: list[object] = []

    def _boom(*_a: object, **_k: object) -> object:
        popen_calls.append(1)
        raise AssertionError("Popen must not run when flock is held")

    monkeypatch.setattr("rithmic_gateway.spawn.subprocess.Popen", _boom)

    lock = SessionLock.try_acquire(
        "u-attach-late", "LucidTrading", "wss://example", "Live"
    )
    try:

        def _late_parent() -> None:
            time.sleep(0.15)
            _serve_shared_md_parent(sock, clients=3)

        threading.Thread(target=_late_parent, daemon=True).start()
        cfg = GatewayConfig(
            user="u-attach-late",
            system_name="LucidTrading",
            url="wss://example",
            env="Live",
            listen=f"unix://{sock}",
            spawn_policy="if_missing",
            attest_flock=False,
            spawn_timeout_sec=3.0,
        )
        GatewayAttachCoordinator.resolve_dial_failure(cfg)
        assert popen_calls == []
    finally:
        lock.close()
        if sock.exists():
            sock.unlink()


def test_parallel_attach_at_most_one_popen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent cold-start attach must elect at most one Popen."""
    popen_calls: list[object] = []
    popen_lock = threading.Lock()
    flock_after_first = {"held": False}

    class _FakeProc:
        def poll(self) -> int | None:
            return None

        stderr = None

    def _fake_popen(*_a: object, **_k: object) -> _FakeProc:
        with popen_lock:
            popen_calls.append(1)
            flock_after_first["held"] = True
        time.sleep(0.2)
        return _FakeProc()

    def _flock_held(_cfg: GatewayConfig) -> bool:
        return flock_after_first["held"]

    monkeypatch.setattr("rithmic_gateway.spawn.subprocess.Popen", _fake_popen)
    monkeypatch.setattr("rithmic_gateway.attach.config_flock_held", _flock_held)
    monkeypatch.setattr("rithmic_gateway.spawn.config_flock_held", _flock_held)
    monkeypatch.setattr(
        "rithmic_gateway.spawn._wait_for_socket",
        lambda _proc, _cfg: None,
    )
    monkeypatch.setattr(
        "rithmic_gateway.spawn.resolve_gateway_bin", lambda _b: "/bin/true"
    )

    sock = Path(f"/tmp/rgw-parallel-{os.getpid()}.sock")
    cfg = GatewayConfig(
        user=f"u-parallel-{os.getpid()}",
        system_name="LucidTrading",
        url="wss://parallel-example",
        env="Live",
        listen=f"unix://{sock}",
        spawn_policy="if_missing",
        spawn_environ={"RITHMIC_PASSWORD": "unit-test-secret-zz9"},
        spawn_timeout_sec=5.0,
    )

    errors: list[BaseException] = []

    def _attach() -> None:
        try:
            spawn_gateway(cfg, wait_socket=True)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=_attach) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert len(popen_calls) <= 1


def test_wait_for_parent_socket_timeout_is_listen_path_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock = SessionLock.try_acquire("u-timeout", "LucidTrading", "wss://example", "Live")
    try:
        cfg = GatewayConfig(
            user="u-timeout",
            system_name="LucidTrading",
            url="wss://example",
            env="Live",
            listen=f"unix:///tmp/rgw-no-socket-{os.getpid()}.sock",
            spawn_timeout_sec=0.2,
        )
        with pytest.raises(AttachError) as exc:
            wait_for_parent_socket(cfg)
        assert exc.value.code == "listen_path_mismatch"
    finally:
        lock.close()


def _minimal_env() -> dict[str, str]:
    return {
        "RITHMIC_USER": "u",
        "RITHMIC_SYSTEM_NAME": "LucidTrading",
        "RITHMIC_URL": "wss://example",
    }
