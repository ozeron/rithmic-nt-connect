"""Data client: MD replay after gateway TRANSPORT_UP (characterization)."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest
from _stubs import _CaptureLog, _TestDataClient
from rithmic_nt_connect.transport import (
    RecoveryFailed,
    TransportDown,
    TransportFault,
    TransportFaultKind,
    TransportUp,
)


@dataclass
class _Harness:
    client: _TestDataClient
    log: _CaptureLog
    failures: int
    resyncs: int = 0


def _harness(
    loop: asyncio.AbstractEventLoop | None, replay_failures: int = 0
) -> _Harness:
    client = _TestDataClient.__new__(_TestDataClient)
    log = _CaptureLog()
    client._log = log
    client._bar_types = {}
    if loop is not None:
        client._loop = loop
    harness = _Harness(client=client, log=log, failures=replay_failures)

    async def _resync() -> None:
        harness.resyncs += 1
        if harness.resyncs <= harness.failures:
            raise ConnectionError("resync failed")

    client.__dict__["_resync_ticker_subscription"] = _resync
    client._recovery = client._build_recovery()
    return harness


async def _finish(task: asyncio.Task | None) -> None:
    assert task is not None
    await task


def _down(gen: int = 1) -> TransportDown:
    return TransportDown(
        gen=gen,
        fault=TransportFault(kind=TransportFaultKind.UNIX_EOF, observed_gen=gen),
    )


def test_up_replays_once_and_ignores_same_generation() -> None:
    async def _run() -> None:
        h = _harness(asyncio.get_running_loop())
        recovery = h.client._recovery
        recovery.on_event(TransportUp(gen=2))
        await asyncio.sleep(0)
        await _finish(recovery.task)
        assert h.resyncs == 1
        assert recovery.last_generation == 2

        first = recovery.task
        recovery.on_event(TransportUp(gen=2))
        await asyncio.sleep(0)
        assert recovery.task is first
        assert h.resyncs == 1

    asyncio.run(_run())


def test_stale_generation_is_skipped() -> None:
    async def _run() -> None:
        h = _harness(asyncio.get_running_loop())
        recovery = h.client._recovery
        recovery.last_generation = 5
        recovery.on_event(TransportUp(gen=3))
        await asyncio.sleep(0)
        assert recovery.task is None
        assert h.resyncs == 0

    asyncio.run(_run())


def test_replay_retries_with_doubling_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    delays: list[float] = []
    real_sleep = asyncio.sleep

    async def _sleep(delay: float) -> None:
        delays.append(delay)
        await real_sleep(0)

    async def _run() -> None:
        h = _harness(asyncio.get_running_loop(), replay_failures=2)
        recovery = h.client._recovery
        monkeypatch.setattr(asyncio, "sleep", _sleep)
        recovery.on_event(TransportUp(gen=2))
        await real_sleep(0)
        await _finish(recovery.task)
        assert h.resyncs == 3
        assert recovery.last_generation == 2

    asyncio.run(_run())
    assert delays == [0.5, 1.0]


def test_replay_exhaustion_does_not_record_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_sleep = asyncio.sleep

    async def _sleep(delay: float) -> None:
        await real_sleep(0)

    async def _run() -> None:
        h = _harness(asyncio.get_running_loop(), replay_failures=99)
        recovery = h.client._recovery
        recovery.max_attempts = 3
        monkeypatch.setattr(asyncio, "sleep", _sleep)
        recovery.on_event(TransportUp(gen=2))
        await real_sleep(0)
        await _finish(recovery.task)
        assert h.resyncs == 3
        assert recovery.last_generation == 0
        assert any("exhausted" in m for m in h.log.messages)

    asyncio.run(_run())


def test_down_and_recovery_failed_do_not_schedule_replay() -> None:
    async def _run() -> None:
        h = _harness(asyncio.get_running_loop())
        recovery = h.client._recovery
        recovery.on_event(_down())
        recovery.on_event(RecoveryFailed(gen=2, attempts=3, cause=None))
        await asyncio.sleep(0)
        assert recovery.task is None
        assert h.resyncs == 0
        assert any("recovery failed" in m for m in h.log.messages)

    asyncio.run(_run())


def test_missing_loop_logs_instead_of_raising() -> None:
    h = _harness(None)
    h.client._recovery.on_event(TransportUp(gen=2))
    assert any("loop missing" in m for m in h.log.messages)


def test_up_from_foreign_thread_hops_onto_the_loop() -> None:
    async def _run() -> None:
        h = _harness(asyncio.get_running_loop())
        recovery = h.client._recovery
        worker = threading.Thread(target=recovery.on_event, args=(TransportUp(gen=2),))
        worker.start()
        await asyncio.to_thread(worker.join)
        for _ in range(50):
            if recovery.task is not None:
                break
            await asyncio.sleep(0.01)
        await _finish(recovery.task)
        assert h.resyncs == 1

    asyncio.run(_run())


def test_disconnect_drops_the_listener_and_cancels_a_replay() -> None:
    from _stubs import WireSessionStub

    class _Sess(WireSessionStub):
        def __init__(self) -> None:
            self.unsubscribed = 0
            self.disconnected = False

        def subscribe_transport_events(
            self, listener: Callable[..., Any], *args: Any, **kwargs: Any
        ) -> Callable[[], None]:
            def _unsubscribe() -> None:
                self.unsubscribed += 1

            return _unsubscribe

        def disconnect(self) -> None:
            self.disconnected = True

    async def _run() -> None:
        h = _harness(asyncio.get_running_loop())
        sess = _Sess()
        client = h.client
        client._session = sess
        client._poll_task = None
        client._history_poll_task = None
        client._recovery.attach(sess)
        in_flight = asyncio.ensure_future(asyncio.sleep(3600))
        client._recovery.task = in_flight

        await client._disconnect()

        assert in_flight.cancelled()
        assert sess.unsubscribed == 1
        assert sess.disconnected
        assert client._recovery.task is None

    asyncio.run(_run())
