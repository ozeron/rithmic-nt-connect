"""Shared credential flock (matches Rust ``rithmic_gateway::singleton``)."""

from __future__ import annotations

import os
from pathlib import Path

from rithmic_gateway.config import default_unix_path


class SessionLockError(RuntimeError):
    """Could not acquire the exclusive credential flock."""


def lock_path(user: str, system_name: str, url: str) -> Path:
    """Lock file sibling to the default unix socket (``.lock`` instead of ``.sock``)."""
    sock = Path(default_unix_path(user, system_name, url))
    return sock.with_suffix(".lock")


class SessionLock:
    """Exclusive ``fcntl`` flock released when this object is closed / GC'd."""

    def __init__(self, path: Path, fd: int) -> None:
        self.path = path
        self._fd = fd

    @classmethod
    def try_acquire(cls, user: str, system_name: str, url: str) -> SessionLock:
        import fcntl

        path = lock_path(user, system_name, url)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise SessionLockError(
                f"session already held by another process at {path}"
            ) from exc
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode())
        return cls(path, fd)

    def close(self) -> None:
        if self._fd < 0:
            return
        try:
            import fcntl

            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = -1

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
