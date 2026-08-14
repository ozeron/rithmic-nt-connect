"""Flock refuse behavior for direct sessions."""

from __future__ import annotations

import pytest

from rithmic_gateway.flock import SessionLock, SessionLockError


def test_second_flock_acquire_fails() -> None:
    user = f"flock-test-{id(object())}"
    system = "LucidTrading"
    url = "wss://rprotocol.rithmic.com:443"
    first = SessionLock.try_acquire(user, system, url)
    with pytest.raises(SessionLockError, match="already held"):
        SessionLock.try_acquire(user, system, url)
    first.close()
    second = SessionLock.try_acquire(user, system, url)
    second.close()
