"""Flock refuse behavior for direct sessions."""

from __future__ import annotations

from typing import Any

import pytest
from _stubs import WireSessionStub
from rithmic_gateway.flock import SessionLock, SessionLockError
from rithmic_nt_connect.config import SessionConfig


def test_flocked_session_connect_is_idempotent() -> None:
    from rithmic_nt_connect.errors import AlreadyConnectedError
    from rithmic_nt_connect.session import _FlockedDirectSession

    class _Inner(WireSessionStub):
        # Models the real Rust session: connect() is a safe no-op when already
        # connected (it raises AlreadyConnectedError without reconnecting).
        def __init__(self) -> None:
            self.connected = False
            self.calls = 0

        def connect(self) -> None:
            self.calls += 1
            if self.connected:
                raise AlreadyConnectedError("already connected")
            self.connected = True

        def disconnect(self) -> None:
            self.connected = False

        def request_plants(self, plants: str) -> None:
            return None

    inner = _Inner()
    wrapped = _FlockedDirectSession(inner, lock=object())
    wrapped.connect()
    wrapped.connect()
    # No parallel _connected flag: idempotency comes from the inner refusing a
    # redundant connect (AlreadyConnectedError, swallowed). Still connected.
    assert inner.connected
    assert callable(wrapped.request_plants)

    from concurrent.futures import ThreadPoolExecutor

    inner2 = _Inner()
    wrapped2 = _FlockedDirectSession(inner2, lock=object())
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: wrapped2.connect(), range(2)))
    assert inner2.connected


def test_flocked_session_reconnects_after_disconnect() -> None:
    from rithmic_nt_connect.session import _FlockedDirectSession

    class _Inner(WireSessionStub):
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


def test_flocked_session_forwards_resolved_account() -> None:
    from rithmic_nt_connect.session import _FlockedDirectSession

    class _Inner(WireSessionStub):
        def resolved_account(self) -> dict[str, object] | None:
            return {"account_id": "A1", "fcm_id": "F1", "ib_id": "I1"}

    wrapped = _FlockedDirectSession(_Inner(), lock=object())
    assert wrapped.resolved_account() == {
        "account_id": "A1",
        "fcm_id": "F1",
        "ib_id": "I1",
    }


def test_gateway_create_session_returns_fresh_wire_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Gateway mode never shares: each Nautilus client gets its own
    ``GatewayClient`` (the parent owns the single login). Pins the contract at
    the factory boundary so an accidental cache cannot be introduced.
    """
    import rithmic_nt_connect.gateway_wire as gw
    from rithmic_nt_connect.config import ConnectMode, SessionConfig
    from rithmic_nt_connect.session import create_session

    created: list[object] = []

    def _fake_gateway(cfg):
        obj = object()
        created.append(obj)
        return obj

    monkeypatch.setattr(gw, "create_gateway_wire_session", _fake_gateway)
    cfg = SessionConfig(
        user="u",
        password="p",
        connect_mode=ConnectMode.GATEWAY,
    )
    a = create_session(cfg)
    b = create_session(cfg)
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


class _FakeSessionInner(WireSessionStub):
    """Stand-in for the Rust ``Session`` (no extension module on CI)."""

    def __init__(self, **kwargs: Any) -> None:
        self.plant_requests: list[str] = []

    def request_plants(self, plants: str) -> None:
        self.plant_requests.append(plants)


class _FakeLock:
    """Stand-in for ``SessionLock``; counts acquisitions per test."""

    acquired = 0

    @classmethod
    def try_acquire(cls, *args: Any, **kwargs: Any) -> _FakeLock:
        return cls(*args, **kwargs)

    def __init__(self, *args: Any) -> None:
        _FakeLock.acquired += 1

    def close(self) -> None:
        pass  # Stub: pool tests don't exercise the release path.


def _direct_session_config(*, env: str = "Demo") -> SessionConfig:
    from rithmic_nt_connect.config import ConnectMode

    return SessionConfig(
        user=f"singleton-{id(object())}",
        password="p",
        connect_mode=ConnectMode.DIRECT,
        system_name="TEST-SYS",
        url="wss://rprotocol.rithmic.com:443",
        env=env,
    )


def _fake_rust_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the lazy ``_lib`` import and flock at fakes."""
    import sys
    import types

    import rithmic_nt_connect.session as session_mod

    fake_lib = types.SimpleNamespace(Session=_FakeSessionInner)
    monkeypatch.setitem(sys.modules, "rithmic_nt_connect._lib", fake_lib)
    monkeypatch.setattr(session_mod, "_load_session_lock", lambda: _FakeLock)
    monkeypatch.setattr(session_mod, "_SESSION_CACHE", {})
    _FakeLock.acquired = 0


def test_create_rust_session_is_process_singleton(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct: data factory, exec factory, and history path share ONE client.

    A second ``create_rust_session`` for the same credentials must reuse the
    cached session (acquiring a holder) instead of building a second Rithmic
    login that would close the first — the flock also refuses a second one.
    ``request_plants`` unions the execution plant set on the shared session;
    the credential flock is taken exactly once.
    """
    from rithmic_nt_connect.session import _FlockedDirectSession, create_rust_session

    _fake_rust_session(monkeypatch)
    cfg = _direct_session_config()

    data = create_rust_session(cfg, plants="market_data")
    data_again = create_rust_session(cfg, plants="market_data")
    exec_session = create_rust_session(cfg, plants="execution")

    assert data is data_again is exec_session
    assert isinstance(data, _FlockedDirectSession)
    assert isinstance(data._inner, _FakeSessionInner)
    # The execution hand-out unions the PnL plant into the shared session.
    assert data._inner.plant_requests == ["execution"]
    # Every hand-out holds the session; the flock was taken once (first build).
    assert data._holders == 3
    assert _FakeLock.acquired == 1


def test_create_rust_session_env_partitions_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Live/Demo/Test are distinct login identities: never shared."""
    from rithmic_nt_connect.session import create_rust_session

    _fake_rust_session(monkeypatch)
    demo = create_rust_session(_direct_session_config(env="Demo"))
    test_env = create_rust_session(_direct_session_config(env="Test"))
    assert demo is not test_env
    assert _FakeLock.acquired == 2


def test_flocked_session_disconnect_is_refcounted() -> None:
    """One client's disconnect must not close plants the other still uses."""
    from rithmic_nt_connect.session import _FlockedDirectSession

    class _Inner(WireSessionStub):
        def __init__(self) -> None:
            self.disconnects = 0

        def disconnect(self) -> None:
            self.disconnects += 1

    inner = _Inner()
    wrapped = _FlockedDirectSession(inner, lock=object())
    wrapped.acquire()  # second client (e.g. exec factory) joins the session
    wrapped.disconnect()  # first client (data) leaves: plants stay up
    assert inner.disconnects == 0
    wrapped.disconnect()  # last client leaves: real teardown
    assert inner.disconnects == 1
    wrapped.disconnect()  # already torn down: no-op
    assert inner.disconnects == 1


def test_flocked_session_reset_ticker_is_refcount_blind() -> None:
    """The data resync must actually recreate the ticker plant while others
    hold the session (a refcounted disconnect would be a no-op)."""
    from rithmic_nt_connect.session import _FlockedDirectSession

    class _Inner(WireSessionStub):
        def __init__(self) -> None:
            self.resets = 0

        def reset_ticker(self) -> None:
            self.resets += 1

    inner = _Inner()
    wrapped = _FlockedDirectSession(inner, lock=object())
    wrapped.acquire()
    wrapped.reset_ticker()
    assert inner.resets == 1


def test_flocked_session_last_disconnect_releases_flock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The credential flock lives as long as the owners: the last holder's
    disconnect tears plants down, closes the flock, and evicts the session
    from the singleton, so a stopped node no longer blocks a separate process.
    """
    import rithmic_nt_connect.session as session_mod
    from rithmic_nt_connect.session import _FlockedDirectSession

    class _Inner(WireSessionStub):
        def __init__(self) -> None:
            self.disconnects = 0

        def disconnect(self) -> None:
            self.disconnects += 1

    class _RecordingLock:
        def __init__(self) -> None:
            self.closed = 0

        def close(self) -> None:
            self.closed += 1

    cache: dict[str, object] = {}
    monkeypatch.setattr(session_mod, "_SESSION_CACHE", cache)
    lock = _RecordingLock()
    wrapped = _FlockedDirectSession(_Inner(), lock, cache_key="k")
    cache["k"] = wrapped
    wrapped.acquire()  # second client

    wrapped.disconnect()  # first client leaves: plants + flock stay
    assert lock.closed == 0
    assert "k" in cache

    wrapped.disconnect()  # last client leaves: teardown + release + evict
    assert lock.closed == 1
    assert "k" not in cache

    wrapped.disconnect()  # already gone: no-op
    assert lock.closed == 1
