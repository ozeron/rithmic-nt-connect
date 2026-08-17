"""Flock refuse behavior for direct sessions."""

from __future__ import annotations

import pytest

from rithmic_gateway.flock import SessionLock, SessionLockError


def test_flocked_session_connect_is_idempotent() -> None:
    from rithmic_nt_connect.errors import AlreadyConnectedError
    from rithmic_nt_connect.session import _FlockedDirectSession

    class _Inner:
        def __init__(self) -> None:
            self.calls = 0

        def connect(self) -> None:
            self.calls += 1
            if self.calls > 1:
                raise AlreadyConnectedError("already connected")

        def request_plants(self, _plants: str) -> None:
            return None

    inner = _Inner()
    wrapped = _FlockedDirectSession(inner, lock=object())
    wrapped.connect()
    wrapped.connect()
    assert inner.calls == 1
    assert callable(wrapped.request_plants)

    from concurrent.futures import ThreadPoolExecutor

    inner2 = _Inner()
    wrapped2 = _FlockedDirectSession(inner2, lock=object())
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: wrapped2.connect(), range(2)))
    assert inner2.calls == 1


def test_flocked_session_reconnects_after_disconnect() -> None:
    from rithmic_nt_connect.session import _FlockedDirectSession

    class _Inner:
        def __init__(self) -> None:
            self.calls = 0
            self.disconnects = 0

        def connect(self) -> None:
            self.calls += 1

        def disconnect(self) -> None:
            self.disconnects += 1

    inner = _Inner()
    wrapped = _FlockedDirectSession(inner, lock=object())
    wrapped.connect()
    assert inner.calls == 1
    wrapped.disconnect()
    assert inner.disconnects == 1
    # A later connect() must not short-circuit on a stale _connected flag.
    wrapped.connect()
    assert inner.calls == 2


def test_gateway_factory_does_not_share_wire_session(monkeypatch: pytest.MonkeyPatch) -> None:
    from rithmic_nt_connect.config import ConnectMode, SessionConfig
    from rithmic_nt_connect.factories import _shared_session
    import rithmic_nt_connect.factories as fac

    created: list[object] = []

    def _fake_create(cfg, *, plants):  # noqa: ARG001
        obj = object()
        created.append(obj)
        return obj

    monkeypatch.setattr(fac, "create_session", _fake_create)
    cfg = SessionConfig(
        user="u",
        password="p",
        connect_mode=ConnectMode.GATEWAY,
    )
    a = _shared_session(cfg, {}, plants="market_data")
    b = _shared_session(cfg, {}, plants="execution")
    assert a is not b
    assert len(created) == 2


def test_connect_once_swallows_only_typed_already_connected() -> None:
    from rithmic_nt_connect.errors import AlreadyConnectedError
    from rithmic_nt_connect.session import _connect_once

    class _OkInner:
        def connect(self) -> None:
            raise AlreadyConnectedError("already connected")

    class _BadInner:
        def connect(self) -> None:
            raise RuntimeError("already connected was the last line of a real failure")

    _connect_once(_OkInner())  # typed error: idempotent, swallowed
    with pytest.raises(RuntimeError, match="already connected"):
        _connect_once(_BadInner())  # same text, different type: must propagate


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
