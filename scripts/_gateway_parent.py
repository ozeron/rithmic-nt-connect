"""Harness-side launcher for a ``rithmic-gateway`` parent (smokes and live e2e).

Deliberately lives outside the shipped ``rithmic_gateway`` package: the client
library never starts a parent. Scripts and live tests that need one start it
here, wait until it is attachable, and terminate it on exit. Operators use
``python -m rithmic_gateway`` (or a service manager) instead.
"""

from __future__ import annotations

import contextlib
import os
import subprocess  # nosec B404 - launches the gateway binary under test
import tempfile
import time
from collections.abc import Iterator, Mapping
from pathlib import Path

from rithmic_gateway.attach import config_flock_held, socket_listening
from rithmic_gateway.binary import resolve_gateway_bin
from rithmic_gateway.config import GatewayConfig
from rithmic_nt_connect.config import SessionConfig

# Env forwarded to the parent. The parent's fingerprint comes only from the
# GatewayConfig (below), never from alias keys such as RITHMIC_GATEWAY or
# RITHMIC_SYSTEM, so a stale alias cannot send a Test login to production.
_FORWARDED_ENV = (
    "RITHMIC_ACCOUNT_ID",
    "RITHMIC_FCM_ID",
    "RITHMIC_IB_ID",
    "RITHMIC_ENABLE_TRADING",
    "RITHMIC_GATEWAY_CANCEL_ALL",
    "RITHMIC_GATEWAY_AUTH_TOKEN",
    "RITHMIC_GATEWAY_IDLE_EXIT_SEC",
    "RITHMIC_HISTORY_READY_PROBE",
    "RITHMIC_HISTORY_READY_SYMBOL",
    "RITHMIC_HISTORY_READY_EXCHANGE",
    "RITHMIC_SYMBOL",
    "RITHMIC_EXCHANGE",
    "XDG_RUNTIME_DIR",
    "PATH",
    "HOME",
    "TMPDIR",
)


#: Grace (seconds) after the last client before a harness-started parent exits.
HARNESS_IDLE_EXIT_SEC = "60"


class ParentStartError(RuntimeError):
    """The harness could not bring up an attachable parent."""


def gateway_config_from_session(session: SessionConfig) -> GatewayConfig:
    """The ``GatewayConfig`` fingerprint for a gateway-mode ``SessionConfig``."""
    return GatewayConfig(
        user=session.user,
        system_name=session.system_name,
        url=session.url,
        env=session.env,
        account_id=session.account_id or "",
        fcm_id=session.fcm_id or "",
        ib_id=session.ib_id or "",
        auth_token=session.gateway_auth_token or "",
        listen=session.gateway_listen,
    )


def password_from_env(environ: Mapping[str, str] | None = None) -> str:
    """The parent's Rithmic password, resolved with ``SessionConfig`` alias rules.

    Gateway-mode ``SessionConfig`` deliberately holds no password, so the harness
    resolves it through direct mode's lookup and hands it only to the parent.
    """
    source = os.environ if environ is None else environ
    cfg = SessionConfig.from_env({**source, "RITHMIC_CONNECT_MODE": "direct"})
    if cfg.password is None:
        raise ParentStartError("no Rithmic password in the environment")
    return cfg.password


def parent_env(
    config: GatewayConfig,
    *,
    password: str,
    extra: Mapping[str, str] | None = None,
    source: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Build the parent's environment: forwarded keys + config fingerprint."""
    src = os.environ if source is None else source
    env = {k: str(src[k]) for k in _FORWARDED_ENV if str(src.get(k, "")) != ""}
    env.update(
        RITHMIC_USER=config.user,
        RITHMIC_PASSWORD=password,
        RITHMIC_SYSTEM_NAME=config.system_name,
        RITHMIC_URL=config.url,
        RITHMIC_ENV=config.env,
        RITHMIC_GATEWAY_LISTEN=config.listen or "",
    )
    # The harness owns an ephemeral parent: if the harness is SIGKILLed the parent
    # must not outlive it holding the Rithmic login. An operator-forwarded value
    # (or ``extra``) wins; the library and launcher never set this.
    env.setdefault("RITHMIC_GATEWAY_IDLE_EXIT_SEC", HARNESS_IDLE_EXIT_SEC)
    if extra:
        env.update({str(k): str(v) for k, v in extra.items()})
    return env


def start_parent(
    config: GatewayConfig,
    *,
    password: str,
    extra_env: Mapping[str, str] | None = None,
    bin_path: str | None = None,
    timeout_sec: float = 90.0,
) -> subprocess.Popen[bytes]:
    """Start a parent for ``config`` and wait until a client can attach.

    Refuses to run when another process already holds the credential flock (that
    parent is not ours to start or stop). The returned process must be passed to
    :func:`stop_parent`.
    """
    if config_flock_held(config):
        raise ParentStartError(
            "a gateway parent already holds the credential flock; stop it or use "
            "it directly (this harness only manages parents it started)"
        )
    binary = bin_path or resolve_gateway_bin()
    env = parent_env(config, password=password, extra=extra_env)
    # A file, not a pipe: nothing reads the child's stderr while it runs, and a
    # full pipe would block a long-lived parent. The child keeps its own fd.
    with tempfile.TemporaryFile() as stderr_file:
        proc = subprocess.Popen(  # nosec B603 - argv is the resolved binary only
            [binary],
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=stderr_file,
            start_new_session=True,
        )
        sock = Path(config.socket_path)
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            if config_flock_held(config) and socket_listening(sock):
                return proc
            if proc.poll() is not None:
                stderr_file.seek(0)
                err = stderr_file.read().decode(errors="replace")
                raise ParentStartError(
                    f"gateway exited early (code={proc.returncode}): {err}"
                )
            time.sleep(0.05)
    stop_parent(proc)
    raise ParentStartError(f"timed out waiting for the gateway parent at {sock}")


def stop_parent(proc: subprocess.Popen[bytes], *, grace_sec: float = 10.0) -> None:
    """Terminate a parent started by :func:`start_parent` (SIGTERM, then kill)."""
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=grace_sec)
        except subprocess.TimeoutExpired:
            proc.kill()
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=5.0)


@contextlib.contextmanager
def running_parent(
    config: GatewayConfig,
    *,
    password: str,
    extra_env: Mapping[str, str] | None = None,
    bin_path: str | None = None,
    timeout_sec: float = 90.0,
) -> Iterator[subprocess.Popen[bytes]]:
    """Context manager: start a parent, yield it, always stop it and drop the socket."""
    proc = start_parent(
        config,
        password=password,
        extra_env=extra_env,
        bin_path=bin_path,
        timeout_sec=timeout_sec,
    )
    try:
        yield proc
    finally:
        stop_parent(proc)
        with contextlib.suppress(Exception):
            Path(config.socket_path).unlink(missing_ok=True)
