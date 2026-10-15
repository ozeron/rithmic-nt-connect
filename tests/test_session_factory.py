"""Direct factories and attachment lifetime on a real, credential-free Rust host."""

from __future__ import annotations

import asyncio
import fcntl
import gc
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, cast

import pytest
from rithmic_nt_connect import _lib
from rithmic_nt_connect.config import ConnectMode, SessionConfig
from rithmic_nt_connect.data import resync_ticker_session
from rithmic_nt_connect.session import _connect_once, create_session
from rithmic_nt_connect.transport import Interrupted, Resumed


def _wait_for(check: Any) -> None:
    deadline = time.monotonic() + 3
    while not check():
        assert time.monotonic() < deadline, "host did not finish background detach"
        time.sleep(0.005)


def _fake(tmp_path: Path, **kwargs: Any) -> Any:
    return _lib.Session.fake(lock_dir=str(tmp_path), **kwargs)


def _count(script: Any, prefix: str) -> int:
    return sum(c.startswith(prefix) for c in script.calls())


def _factory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake = _lib.Session.fake

    def construct(**kwargs: Any) -> Any:
        for name in ("password", "connect_mode", "gateway_listen"):
            kwargs.pop(name, None)
        return fake(lock_dir=str(tmp_path), **kwargs)

    monkeypatch.setattr(_lib, "Session", construct)


def _config(**kwargs: Any) -> SessionConfig:
    return SessionConfig(
        user="factory-test",
        password="never-a-credential",
        connect_mode=ConnectMode.DIRECT,
        system_name="Fake",
        url="wss://fake",
        env="Test",
        **kwargs,
    )


def test_factories_share_one_login_and_union_execution_plants(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _factory(monkeypatch, tmp_path)
    data = create_session(_config())
    again = create_session(_config())
    execution = create_session(_config(), plants="execution")
    assert data is not again and again is not execution
    script = cast(Any, data).script
    assert _count(script, "connect") == 0, "construction does no plant I/O"
    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda s: s.connect(), (data, again, execution)))
    assert _count(script, "connect") == 1
    assert any("request_plants" in c and "pnl: true" in c for c in script.calls())
    data.disconnect()
    again.disconnect()
    assert _count(script, "disconnect") == 0
    execution.disconnect()
    assert _count(script, "disconnect") == 1


def test_password_is_not_part_of_the_host_fingerprint() -> None:
    user = f"password-key-{time.monotonic_ns()}"
    a = _lib.Session(user=user, password="first", connect_mode="direct")
    b = _lib.Session(user=user, password="changed", connect_mode="direct")
    a.open_gates(True, False)
    assert b.open_gates(False, False).trading
    a.disconnect()
    b.disconnect()


def test_environment_partitions_hosts(tmp_path: Path) -> None:
    demo = _fake(tmp_path, env="Demo")
    test = _fake(tmp_path, env="Test")
    demo.connect()
    test.connect()
    demo.disconnect()
    assert _count(test.script, "disconnect") == 0
    test.subscribe("NQU6", "CME")
    test.disconnect()


def test_session_intents_are_independent_and_repeated_subscribe_is_a_set(
    tmp_path: Path,
) -> None:
    a = _fake(tmp_path)
    b = _fake(tmp_path)
    a.connect()
    b.connect()
    a.subscribe("NQU6", "CME")
    a.subscribe("NQU6", "CME")
    b.subscribe("NQU6", "CME")
    assert _count(a.script, "subscribe NQU6 CME") == 1
    a.unsubscribe("NQU6", "CME")
    assert _count(a.script, "unsubscribe NQU6 CME") == 0
    b.script.emit(
        "ticker",
        {
            "type": "last_trade",
            "symbol": "NQU6",
            "exchange": "CME",
            "trade_price": 21000.0,
            "trade_size": 1,
        },
    )
    assert b.next_events("ticker", timeout_ms=1000)[0]["trade_price"] == pytest.approx(
        21000.0
    )
    assert a.next_events("ticker", timeout_ms=10) == []
    a.disconnect()
    b.disconnect()


@pytest.mark.parametrize("change", ["resync", "peer_book_join", "recovery"])
@pytest.mark.parametrize("poll_timeout_ms", [0, 100])
def test_direct_factory_delivers_after_rejoin_and_recovery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    change: str,
    poll_timeout_ms: int,
) -> None:
    _factory(monkeypatch, tmp_path)
    session = cast(Any, create_session(_config()))
    peer = None
    subscription = None
    symbol, exchange = "NQZ6", "CME"
    session.connect()
    session.subscribe(symbol, exchange)
    session.subscribe_order_book_summary(symbol, exchange)

    def expect_delivery(price: float) -> None:
        emitted_ns = time.time_ns()
        for kind in ("order_book", "last_trade", "last_trade"):
            row: dict[str, Any] = {
                "type": kind,
                "symbol": symbol,
                "exchange": exchange,
                "ts_event_ns": emitted_ns,
            }
            if kind == "order_book":
                row.update(update_type=1, bid_price=[price], bid_size=[1])
            else:
                row.update(trade_price=price, trade_size=1)
            session.script.emit("ticker", row)
        book = session.next_events("ticker", max_events=1, timeout_ms=1000)
        assert book[0]["type"] == "order_book"
        assert book[0]["bid_price"] == [price]
        deadline = time.monotonic() + 3
        for _ in range(2):
            event = None
            while event is None and time.monotonic() < deadline:
                event = session.poll_event(timeout_ms=poll_timeout_ms)
                if event is None:
                    time.sleep(0.005)
            assert event is not None
            assert event["type"] == "last_trade"
            assert event["symbol"] == symbol and event["exchange"] == exchange
            assert event["trade_price"] == price
            assert event["ts_event_ns"] >= emitted_ns

    try:
        expect_delivery(21000.0)
        if change == "resync":
            asyncio.run(
                resync_ticker_session(
                    session, {(symbol, exchange)}, {(symbol, exchange)}
                )
            )
        elif change == "peer_book_join":
            peer = cast(Any, create_session(_config()))
            peer.connect()
            peer.subscribe_order_book_summary(symbol, exchange)
        else:
            events: list[Any] = []
            resumed = threading.Event()

            def listener(event: Any) -> None:
                events.append(event)
                if isinstance(event, Resumed):
                    resumed.set()

            subscription = session.subscribe_session_events(listener)
            session.script.fail_stream("ticker")
            assert resumed.wait(3)
            assert len(events) == 2
            assert isinstance(events[0], Interrupted)
            assert isinstance(events[1], Resumed)
        expect_delivery(22000.0)
    finally:
        if subscription is not None:
            subscription.cancel()
        if peer is not None:
            peer.disconnect()
        session.disconnect()


def test_last_disconnect_releases_flock_even_while_facades_remain_alive(
    tmp_path: Path,
) -> None:
    a = _fake(tmp_path)
    b = _fake(tmp_path)
    a.connect()
    path = next(tmp_path.glob("*.lock"))
    with path.open("r+") as lock:
        a.disconnect()
        with pytest.raises(BlockingIOError):
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        b.disconnect()
        b.disconnect()
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(_lib.SessionLockError):
            _fake(tmp_path)
        fcntl.flock(lock, fcntl.LOCK_UN)
    replacement = _fake(tmp_path)
    replacement.connect()
    assert _count(replacement.script, "connect") == 1
    replacement.disconnect()


def test_dropping_a_session_outside_the_runtime_does_not_wait_for_the_venue(
    tmp_path: Path,
) -> None:
    session = _fake(tmp_path)
    session.connect()
    script = session.script
    script.set_delay("disconnect", 250)
    started = time.monotonic()
    del session
    gc.collect()
    assert time.monotonic() - started < 0.2
    _wait_for(lambda: _count(script, "disconnect") == 1)
    replacement = _fake(tmp_path)
    replacement.disconnect()


def test_dropping_one_attachment_preserves_another_sessions_intents(
    tmp_path: Path,
) -> None:
    a = _fake(tmp_path)
    b = _fake(tmp_path)
    a.connect()
    a.subscribe("NQU6", "CME")
    b.connect()
    b.subscribe("NQU6", "CME")
    del a
    gc.collect()
    b.script.emit(
        "ticker",
        {
            "type": "last_trade",
            "symbol": "NQU6",
            "exchange": "CME",
            "trade_price": 42.0,
        },
    )
    assert b.next_events("ticker", timeout_ms=1000)[0]["trade_price"] == pytest.approx(
        42.0
    )
    assert _count(b.script, "unsubscribe NQU6 CME") == 0
    b.disconnect()


def test_script_rejects_a_request_without_a_venue_connection(tmp_path: Path) -> None:
    session = _fake(tmp_path)
    session.connect()
    session.open_gates(True, False)
    session.script.reject_next("cancel_order", "venue says no")
    with pytest.raises(_lib.GatewayError, match="venue says no") as failure:
        session.cancel_order("basket-1")
    assert failure.value.code == "venue_rejected"
    session.disconnect()


def test_connect_once_swallows_only_typed_already_connected() -> None:
    class AlreadyUp:
        def connect(self) -> None:
            raise _lib.AlreadyConnectedError("already connected")

    class Broken:
        def connect(self) -> None:
            raise RuntimeError("already connected was the last line of a real failure")

    _connect_once(AlreadyUp())
    with pytest.raises(RuntimeError, match="already connected"):
        _connect_once(Broken())


def test_the_extension_flock_is_the_one_the_python_lake_client_probes() -> None:
    from rithmic_gateway.flock import lock_path

    user = f"flock-path-{time.monotonic_ns()}"
    held = _lib.SessionLock.try_acquire(user, "Fake", "wss://fake", "Test")
    try:
        assert Path(held.path) == lock_path(user, "Fake", "wss://fake", "Test")
        with pytest.raises(_lib.SessionLockError, match="already held"):
            _lib.SessionLock.try_acquire(user, "Fake", "wss://fake", "Test")
    finally:
        held.close()
