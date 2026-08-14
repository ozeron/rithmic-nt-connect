"""Shared credential flock (matches Rust ``rithmic_gateway::singleton``)."""

from __future__ import annotations

import os
from pathlib import Path

from rithmic_gateway.config import default_unix_path


class SessionLockError(RuntimeError):
    """Could not acquire the exclusive credential flock."""


def lock_path(user: str, system_name: str, url: str, env: str = "Live") -> Path:
    """Lock file sibling to the default unix socket (``.lock`` instead of ``.sock``)."""
    sock = Path(default_unix_path(user, system_name, url, env))
    return sock.with_suffix(".lock")


def session_flock_held(user: str, system_name: str, url: str, env: str = "Live") -> bool:
    """True when another live process holds the credential flock for this fingerprint."""
    try:
        lock = SessionLock.try_acquire(user, system_name, url, env)
    except SessionLockError:
        return True
    lock.close()
    return False


def read_lock_pid(user: str, system_name: str, url: str, env: str = "Live") -> int | None:
    """PID written into the credential lock file, if present and parseable."""
    path = lock_path(user, system_name, url, env)
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not text:
        return None
    try:
        return int(text.split()[0])
    except ValueError:
        return None


class SessionLock:
    """Exclusive ``fcntl`` flock released when this object is closed / GC'd."""

    def __init__(self, path: Path, fd: int) -> None:
        self.path = path
        self._fd = fd

    @classmethod
    def try_acquire(
        cls, user: str, system_name: str, url: str, env: str = "Live"
    ) -> SessionLock:
        import fcntl

        path = lock_path(user, system_name, url, env)
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
