"""Gateway parent attach (flock check + bounded wait). The client never spawns."""

from __future__ import annotations

import socket
import time
from pathlib import Path

from rithmic_gateway.config import GatewayConfig


class AttachError(RuntimeError):
    """Attach failure with a stable machine code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def config_flock_held(config: GatewayConfig) -> bool:
    """True when another live process holds the credential flock."""
    from rithmic_gateway.flock import session_flock_held

    return session_flock_held(config.user, config.system_name, config.url, config.env)


def socket_listening(sock: Path) -> bool:
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
    deadline = time.monotonic() + float(config.attach_timeout_sec)
    sock = Path(config.socket_path)
    while time.monotonic() < deadline:
        if not config_flock_held(config):
            raise AttachError(
                "parent_flock_held_no_socket",
                "credential flock not held while waiting for existing parent — "
                "no gateway is starting",
            )
        if socket_listening(sock):
            return
        time.sleep(0.05)
    raise AttachError(
        "listen_path_mismatch",
        f"timed out waiting for existing gateway parent at {sock} "
        f"(flock held — check RITHMIC_GATEWAY_LISTEN matches the running parent)",
    )


class GatewayAttachCoordinator:
    """Dial-fail recovery: wait for a starting parent, else report none running."""

    @staticmethod
    def resolve_dial_failure(config: GatewayConfig) -> None:
        """Classify a failed dial; returns once the parent socket accepts.

        The credential flock held means a parent was started by someone else and
        is not listening yet, so wait (bounded). Otherwise no parent exists:
        raise ``parent_not_running``. This never starts a process.
        """
        if config_flock_held(config):
            wait_for_parent_socket(config)
            return
        raise AttachError(
            "parent_not_running",
            f"no gateway parent is listening at {config.socket_path} and none "
            f"holds the credential flock; start it with `python -m rithmic_gateway` "
            f"(the parent needs RITHMIC_PASSWORD, clients do not)",
        )
