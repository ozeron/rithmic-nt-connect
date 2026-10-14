"""Attach-only gateway client: dial an explicitly started parent or fail.

The client library never starts the parent (openspec: simplify-gateway-modes).
"""

from __future__ import annotations

import dataclasses
import importlib.util
import os
import subprocess  # nosec B404 - tests assert nothing is ever spawned
import threading
import time
from pathlib import Path

import pytest
import rithmic_gateway
from rithmic_gateway.attach import (
    AttachError,
    GatewayAttachCoordinator,
    wait_for_parent_socket,
)
from rithmic_gateway.client import GatewayClient
from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.flock import SessionLock

_shared = importlib.util.spec_from_file_location(
    "test_gateway_shared_consumers",
    Path(__file__).with_name("test_gateway_shared_consumers.py"),
)
assert _shared and _shared.loader
_shared_mod = importlib.util.module_from_spec(_shared)
_shared.loader.exec_module(_shared_mod)
_serve_shared_md_parent = _shared_mod._serve_shared_md_parent


@pytest.fixture
def no_popen(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """Fail the test if anything tries to start a process."""
    calls: list[object] = []

    def _boom(*_a: object, **_k: object) -> object:
        calls.append(1)
        raise AssertionError("the gateway client must never start a process")

    monkeypatch.setattr(subprocess, "Popen", _boom)
    return calls


def _cfg(
    user: str,
    sock: Path,
    *,
    attest_flock: bool = True,
    attach_timeout_sec: float = 90.0,
) -> GatewayConfig:
    return GatewayConfig(
        user=user,
        system_name="LucidTrading",
        url="wss://example",
        env="Live",
        listen=f"unix://{sock}",
        attest_flock=attest_flock,
        attach_timeout_sec=attach_timeout_sec,
    )


def test_no_parent_and_no_flock_is_parent_not_running(no_popen: list[object]) -> None:
    sock = Path(f"/tmp/rgw-attach-none-{os.getpid()}.sock")
    cfg = _cfg("u-attach-none", sock, attest_flock=False)
    with pytest.raises(AttachError) as exc:
        GatewayAttachCoordinator.resolve_dial_failure(cfg)
    assert exc.value.code == "parent_not_running"
    msg = str(exc.value)
    assert str(sock) in msg
    assert "python -m rithmic_gateway" in msg
    assert no_popen == []


def test_client_connect_without_parent_is_parent_not_running(
    no_popen: list[object],
) -> None:
    sock = Path(f"/tmp/rgw-attach-client-{os.getpid()}.sock")
    cfg = _cfg("u-attach-client", sock)
    with pytest.raises(AttachError) as exc:
        GatewayClient(cfg).connect()
    assert exc.value.code == "parent_not_running"
    assert str(sock) in str(exc.value)
    assert no_popen == []


def test_flock_held_and_socket_appears_attaches(no_popen: list[object]) -> None:
    sock = Path(f"/tmp/rgw-attach-late-{os.getpid()}.sock")
    lock = SessionLock.try_acquire(
        "u-attach-late", "LucidTrading", "wss://example", "Live"
    )
    try:

        def _late_parent() -> None:
            time.sleep(0.15)
            _serve_shared_md_parent(sock, clients=3)

        threading.Thread(target=_late_parent, daemon=True).start()
        cfg = _cfg("u-attach-late", sock, attest_flock=False, attach_timeout_sec=3.0)
        GatewayAttachCoordinator.resolve_dial_failure(cfg)
        assert no_popen == []
    finally:
        lock.close()
        if sock.exists():
            sock.unlink()


def test_flock_released_early_is_parent_flock_held_no_socket() -> None:
    sock = Path(f"/tmp/rgw-attach-gone-{os.getpid()}.sock")
    lock = SessionLock.try_acquire(
        "u-attach-gone", "LucidTrading", "wss://example", "Live"
    )
    threading.Timer(0.2, lock.close).start()
    cfg = _cfg("u-attach-gone", sock, attach_timeout_sec=10.0)
    started = time.monotonic()
    with pytest.raises(AttachError) as exc:
        wait_for_parent_socket(cfg)
    assert exc.value.code == "parent_flock_held_no_socket"
    assert time.monotonic() - started < 5.0


def test_flock_held_wrong_path_is_listen_path_mismatch() -> None:
    sock = Path(f"/tmp/rgw-no-socket-{os.getpid()}.sock")
    lock = SessionLock.try_acquire("u-timeout", "LucidTrading", "wss://example", "Live")
    try:
        cfg = _cfg("u-timeout", sock, attach_timeout_sec=0.2)
        with pytest.raises(AttachError) as exc:
            wait_for_parent_socket(cfg)
        assert exc.value.code == "listen_path_mismatch"
        assert str(sock) in str(exc.value)
    finally:
        lock.close()


def test_library_has_no_spawn_path() -> None:
    assert not hasattr(rithmic_gateway, "spawn_gateway")
    assert importlib.util.find_spec("rithmic_gateway.spawn") is None
    import rithmic_gateway.binary as binary

    assert not hasattr(binary, "spawn_gateway")
    names = {f.name for f in dataclasses.fields(GatewayConfig)}
    assert not {n for n in names if "spawn" in n}
