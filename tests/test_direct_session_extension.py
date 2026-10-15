"""The real PyO3 ``Session`` (no network): trait-backed direct session contract.

Everything here runs against an unconnected session, so it needs neither
credentials nor a venue. It pins what the extension promises after the
``Session`` trait refactor: ``poll_*`` take an optional ``timeout_ms`` (default
non-blocking, as before), unconnected plants raise typed errors, the GIL is
released while a call blocks, and direct-only history methods exist.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

_lib = pytest.importorskip("rithmic_nt_connect._lib")


def _session() -> Any:
    return _lib.Session(
        user="ext-test",
        password="pw",
        system_name="LucidTrading",
        url="wss://rprotocol.rithmic.com:443",
        env="Live",
        plants="market_data",
    )


def test_poll_pnl_event_defaults_to_non_blocking_none() -> None:
    session = _session()
    started = time.monotonic()
    assert session.poll_pnl_event() is None
    assert time.monotonic() - started < 0.5


def test_poll_accepts_a_positive_timeout_and_waits() -> None:
    session = _session()
    started = time.monotonic()
    assert session.poll_pnl_event(timeout_ms=60) is None
    assert time.monotonic() - started >= 0.06


def test_unconnected_ticker_poll_is_a_typed_error() -> None:
    with pytest.raises(_lib.NotConnectedError):
        _session().poll_event()


def test_resolved_account_is_none_until_resolved() -> None:
    assert _session().resolved_account() is None


def test_blocking_poll_releases_the_gil() -> None:
    """A thread blocked in ``poll_pnl_event(timeout)`` must not freeze Python."""
    session = _session()
    ticks = 0
    done = threading.Event()

    def _blocked() -> None:
        session.poll_pnl_event(timeout_ms=300)
        done.set()

    worker = threading.Thread(target=_blocked)
    worker.start()
    deadline = time.monotonic() + 0.25
    while time.monotonic() < deadline and not done.is_set():
        time.sleep(0.01)
        ticks += 1
    worker.join(timeout=5)
    assert done.is_set()
    # With the GIL held for the whole poll this loop would barely advance.
    assert ticks >= 5


def test_concurrent_poll_and_other_calls_do_not_deadlock() -> None:
    session = _session()
    results: list[object] = []

    def _poll() -> None:
        results.append(session.poll_pnl_event(timeout_ms=150))

    threads = [threading.Thread(target=_poll) for _ in range(4)]
    for t in threads:
        t.start()
    for _ in range(20):
        assert session.resolved_account() is None
    for t in threads:
        t.join(timeout=5)
    assert len(results) == 4


def test_history_replay_methods_exist_on_the_direct_session() -> None:
    session = _session()
    for name in ("load_fill_history", "load_order_history", "load_order_history_dates"):
        assert callable(getattr(session, name))


def test_invalid_env_is_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="invalid env"):
        _lib.Session(user="u", password="pw", env="Staging")


def test_session_repr_and_errors_do_not_leak_the_password() -> None:
    session = _lib.Session(user="u", password="top-secret-pw-9", env="Live")
    assert "top-secret-pw-9" not in repr(session)
    with pytest.raises(Exception) as excinfo:
        session.poll_event()
    assert "top-secret-pw-9" not in str(excinfo.value)


@pytest.mark.skipif(
    not hasattr(_lib.Session, "_fault_drop_plant"),
    reason="transport fault hook is compiled only with the testing feature",
)
def test_fault_hook_reaches_the_direct_plant_and_rejects_gateway_sessions() -> None:
    session = _lib.Session(
        user="offline-fault-test",
        password="unused",
        system_name="Rithmic Test",
        url="ws://127.0.0.1:1",
        env="Test",
        connect_mode="direct",
    )
    # Construction never connects. A valid fault reaches the missing plant;
    # unsupported targets must not silently fault the ticker instead.
    with pytest.raises(ValueError, match="supports ticker, pnl, or order"):
        session._fault_drop_plant("history")
    for plant in ("ticker", "pnl", "order"):
        with pytest.raises(_lib.NotConnectedError):
            session._fault_drop_plant(plant)
    gateway = _lib.Session(user="offline-fault-test", connect_mode="gateway")
    with pytest.raises(ValueError, match="real direct session"):
        gateway._fault_drop_plant("ticker")
