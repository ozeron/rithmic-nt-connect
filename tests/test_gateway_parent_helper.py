"""Harness-side parent helper (``scripts/_gateway_parent.py``).

The helper is how smokes and live e2e start a parent now that the client library
never spawns. It lives outside the shipped package; these tests drive it with a
fake parent binary (a script that takes the credential flock and listens).
"""

from __future__ import annotations

import importlib.util
import os
import subprocess  # nosec B404 - asserts the helper's Popen guard
import sys
import textwrap
from pathlib import Path

import pytest
from _gateway_parent import (
    HARNESS_IDLE_EXIT_SEC,
    ParentStartError,
    parent_env,
    running_parent,
    start_parent,
    stop_parent,
)
from rithmic_gateway.attach import config_flock_held
from rithmic_gateway.config import GatewayConfig

PYTHON_SRC = Path(__file__).resolve().parents[1] / "python"

_FAKE_PARENT = textwrap.dedent(
    """\
    #!{python}
    import os, signal, socket, sys, time
    sys.path.insert(0, {src!r})
    from rithmic_gateway.flock import SessionLock

    mode = os.environ.get("FAKE_PARENT_MODE", "ok")
    if mode == "crash":
        print("fake parent: RITHMIC_USER required", file=sys.stderr)
        sys.exit(3)
    lock = SessionLock.try_acquire(
        os.environ["RITHMIC_USER"],
        os.environ["RITHMIC_SYSTEM_NAME"],
        os.environ["RITHMIC_URL"],
        os.environ["RITHMIC_ENV"],
    )
    if mode == "silent":
        time.sleep(60)
    if mode == "chatty":
        # More than a 64 KB pipe buffer: would block forever if stderr were an
        # undrained pipe.
        sys.stderr.write("x" * 300_000)
        sys.stderr.flush()
    path = os.environ["RITHMIC_GATEWAY_LISTEN"].removeprefix("unix://")
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(path)
    srv.listen(8)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    while True:
        time.sleep(0.05)
    """
)


def _cfg(tmp_path: Path, user: str) -> GatewayConfig:
    return GatewayConfig(
        user=user,
        system_name="LucidTrading",
        url="wss://example",
        env="Live",
        listen=f"unix:///tmp/rgwp-{os.getpid()}-{user}.sock",
        attest_flock=False,
    )


def _fake_binary(tmp_path: Path) -> str:
    path = tmp_path / "fake-gateway"
    path.write_text(_FAKE_PARENT.format(python=sys.executable, src=str(PYTHON_SRC)))
    path.chmod(0o755)
    return str(path)


def test_start_parent_waits_for_socket_and_flock_then_stops(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "u-helper-ok")
    proc = start_parent(
        cfg, password="pw", bin_path=_fake_binary(tmp_path), timeout_sec=20.0
    )
    try:
        assert config_flock_held(cfg)
        assert Path(cfg.socket_path).exists()
    finally:
        stop_parent(proc)
        Path(cfg.socket_path).unlink(missing_ok=True)
    assert proc.poll() is not None
    assert not config_flock_held(cfg)


def test_running_parent_context_manager_cleans_up(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "u-helper-ctx")
    with running_parent(
        cfg, password="pw", bin_path=_fake_binary(tmp_path), timeout_sec=20.0
    ) as proc:
        assert proc.poll() is None
        assert config_flock_held(cfg)
    assert proc.poll() is not None
    assert not config_flock_held(cfg)
    assert not Path(cfg.socket_path).exists()


def test_start_parent_reports_early_exit_with_stderr(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "u-helper-crash")
    # parent_env only forwards an allowlist; extra_env carries the test switch.
    with pytest.raises(ParentStartError, match="RITHMIC_USER required"):
        start_parent(
            cfg,
            password="pw",
            extra_env={"FAKE_PARENT_MODE": "crash"},
            bin_path=_fake_binary(tmp_path),
            timeout_sec=20.0,
        )


def test_start_parent_times_out_and_reaps_the_child(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "u-helper-silent")
    with pytest.raises(ParentStartError, match="timed out"):
        start_parent(
            cfg,
            password="pw",
            extra_env={"FAKE_PARENT_MODE": "silent"},
            bin_path=_fake_binary(tmp_path),
            timeout_sec=1.0,
        )
    assert not config_flock_held(cfg)


def test_chatty_parent_does_not_block_on_stderr(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "u-helper-chatty")
    with running_parent(
        cfg,
        password="pw",
        extra_env={"FAKE_PARENT_MODE": "chatty"},
        bin_path=_fake_binary(tmp_path),
        timeout_sec=20.0,
    ) as proc:
        assert proc.poll() is None
        assert config_flock_held(cfg)


def test_harness_parent_gets_an_idle_exit_safety_net(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "u-helper-idle")
    env = parent_env(cfg, password="pw", source={})
    assert env["RITHMIC_GATEWAY_IDLE_EXIT_SEC"] == HARNESS_IDLE_EXIT_SEC


def test_operator_idle_exit_overrides_the_harness_default(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "u-helper-idle2")
    env = parent_env(cfg, password="pw", source={"RITHMIC_GATEWAY_IDLE_EXIT_SEC": "-1"})
    assert env["RITHMIC_GATEWAY_IDLE_EXIT_SEC"] == "-1"


def test_start_parent_refuses_when_a_parent_already_holds_the_flock(
    tmp_path: Path,
) -> None:
    from rithmic_gateway.flock import SessionLock

    cfg = _cfg(tmp_path, "u-helper-held")
    lock = SessionLock.try_acquire(cfg.user, cfg.system_name, cfg.url, cfg.env)
    try:
        with pytest.raises(ParentStartError, match="already holds"):
            start_parent(cfg, password="pw", bin_path=_fake_binary(tmp_path))
    finally:
        lock.close()


def test_parent_env_pins_fingerprint_and_ignores_alias_keys(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path, "u-helper-env")
    env = parent_env(
        cfg,
        password="pw",
        source={
            "PATH": "/usr/bin",
            "RITHMIC_GATEWAY": "wss://stale-alias.example",
            "RITHMIC_SYSTEM": "StaleSystem",
            "RITHMIC_URL": "wss://stale-url.example",
            "RITHMIC_ENABLE_TRADING": "1",
        },
    )
    assert env["RITHMIC_URL"] == "wss://example"
    assert env["RITHMIC_SYSTEM_NAME"] == "LucidTrading"
    assert env["RITHMIC_USER"] == "u-helper-env"
    assert env["RITHMIC_PASSWORD"] == "pw"
    assert env["RITHMIC_GATEWAY_LISTEN"] == cfg.listen
    assert env["RITHMIC_ENABLE_TRADING"] == "1"
    assert "RITHMIC_GATEWAY" not in env
    assert "RITHMIC_SYSTEM" not in env


def test_helper_is_not_part_of_the_shipped_package() -> None:
    assert importlib.util.find_spec("rithmic_gateway._gateway_parent") is None
    spec = importlib.util.find_spec("_gateway_parent")
    assert spec is not None and spec.origin is not None
    assert Path(spec.origin).parent.name == "scripts"


def test_the_helper_never_starts_a_process_when_the_flock_is_held(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rithmic_gateway.flock import SessionLock

    def _boom(*_a: object, **_k: object) -> object:
        raise AssertionError("must not Popen while a parent holds the flock")

    monkeypatch.setattr(subprocess, "Popen", _boom)
    cfg = _cfg(tmp_path, "u-helper-nopopen")
    lock = SessionLock.try_acquire(cfg.user, cfg.system_name, cfg.url, cfg.env)
    try:
        with pytest.raises(ParentStartError):
            start_parent(cfg, password="pw", bin_path=_fake_binary(tmp_path))
    finally:
        lock.close()
