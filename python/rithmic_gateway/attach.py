"""Gateway parent attach coordinator (flock, spawn policy, cold-start races)."""

from __future__ import annotations

import socket
import subprocess  # nosec B404
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from rithmic_gateway.config import GatewayConfig

SpawnPolicy = Literal["never", "if_missing"]


class AttachError(RuntimeError):
    """Attach / spawn-policy failure with a stable machine code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class AttachedParent:
    """Parent is dialable; ``spawned_proc`` set only when this client spawned it."""

    spawned_proc: subprocess.Popen[bytes] | None = None


def config_flock_held(config: GatewayConfig) -> bool:
    """True when another live process holds the credential flock."""
    from rithmic_gateway.flock import session_flock_held

    return session_flock_held(config.user, config.system_name, config.url, config.env)


def _socket_listening(sock: Path) -> bool:
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(0.2)
        s.connect(str(sock))
        s.close()
        return True
    except OSError:
        return False


def wait_for_parent_socket(config: GatewayConfig) -> None:
    """Block until flock is held and the configured listen path accepts.

    Raises ``AttachError`` on timeout (``listen_path_mismatch``) or if the flock
    is released before the socket comes up (``parent_flock_held_no_socket``).
    """
    deadline = time.monotonic() + float(config.spawn_timeout_sec)
    sock = Path(config.socket_path)
    while time.monotonic() < deadline:
        if _socket_listening(sock) and config_flock_held(config):
            return
        if not config_flock_held(config):
            raise AttachError(
                "parent_flock_held_no_socket",
                "credential flock not held while waiting for existing parent — "
                "no gateway is starting",
            )
        time.sleep(0.05)
    raise AttachError(
        "listen_path_mismatch",
        f"timed out waiting for existing gateway parent at {sock} "
        f"(flock held — check RITHMIC_GATEWAY_LISTEN matches the running parent)",
    )


class GatewayAttachCoordinator:
    """Single entry for dial-fail recovery and explicit spawn requests."""

    @staticmethod
    def resolve_dial_failure(
        config: GatewayConfig,
        *,
        environ: Mapping[str, str] | None = None,
    ) -> AttachedParent:
        """Ensure a parent exists after ``GatewayClient`` dial failed."""
        if config_flock_held(config):
            wait_for_parent_socket(config)
            return AttachedParent()
        if config.spawn_policy == "never":
            raise AttachError(
                "dial_failed_spawn_disabled",
                "dial failed and spawn policy is never "
                "(set RITHMIC_GATEWAY_SPAWN_POLICY=if_missing or start the parent)",
            )
        from rithmic_gateway.spawn import spawn_gateway

        proc = spawn_gateway(config, environ=environ, wait_socket=True)
        return AttachedParent(spawned_proc=proc)
