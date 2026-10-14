"""TransportRecoveryBridge: policy shared by the data and exec clients."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from typing import Any

import pytest
from _stubs import _CaptureLog
from rithmic_nt_connect.recovery_bridge import TransportRecoveryBridge
from rithmic_nt_connect.transport import (
    RecoveryFailed,
    TransportDown,
    TransportFault,
    TransportFaultKind,
    TransportUp,
)


class _Session:
    """Gateway-like session: records listeners and unsubscribe calls."""

    def __init__(self) -> None:
        self.listeners: list[Callable[[object], None]] = []
        self.unsubscribed = 0

    def subscribe_transport_events(
        self, listener: Callable[[object], None]
    ) -> Callable[[], None]:
        self.listeners.append(listener)

        def _unsubscribe() -> None:
            self.unsubscribed += 1

        return _unsubscribe


class _Rig:
    def __init__(self, loop: asyncio.AbstractEventLoop | None) -> None:
        self.log = _CaptureLog()
        self.calls: list[tuple[int, int]] = []
        self.downs: list[TransportDown] = []
        self.failed: list[RecoveryFailed] = []
        self.attempt_failures: list[BaseException] = []
        self.fail_first = 0
        self.gate: asyncio.Event | None = None
        self.bridge = TransportRecoveryBridge(
            label="test restore",
            task_name="test_restore",
            log=lambda: self.log,
            get_loop=lambda: loop,
            spawn=lambda lp, coro, name: lp.create_task(coro, name=name),
            restore=self._restore,
            on_down=self.downs.append,
            on_failed=self.failed.append,
            on_attempt_failed=self.attempt_failures.append,
        )

    async def _restore(self, generation: int, attempt: int) -> None:
        self.calls.append((generation, attempt))
        if self.gate is not None:
            await self.gate.wait()
        if len(self.attempt_failures) < self.fail_first:
            raise ConnectionError("restore failed")


def _down(gen: int = 1) -> TransportDown:
    return TransportDown(
        gen=gen,
        fault=TransportFault(kind=TransportFaultKind.UNIX_EOF, observed_gen=gen),
    )


async def _settle(rig: _Rig) -> None:
    await asyncio.sleep(0)
    task = rig.bridge.task
    assert task is not None
    await task


def test_up_restores_once_and_records_the_generation() -> None:
    async def _run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        rig.bridge.on_event(TransportUp(gen=2))
        await _settle(rig)
        assert rig.calls == [(2, 1)]
        assert rig.bridge.last_generation == 2

    asyncio.run(_run())


def test_duplicate_or_stale_generation_is_skipped() -> None:
    async def _run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        rig.bridge.on_event(TransportUp(gen=3))
        await _settle(rig)
        rig.bridge.on_event(TransportUp(gen=3))
        rig.bridge.on_event(TransportUp(gen=2))
        await asyncio.sleep(0)
        assert rig.calls == [(3, 1)]

    asyncio.run(_run())


def test_newer_generation_during_a_restore_runs_after_it() -> None:
    async def _run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        rig.gate = asyncio.Event()
        rig.bridge.on_event(TransportUp(gen=2))
        await asyncio.sleep(0)
        rig.bridge.on_event(TransportUp(gen=3))
        rig.bridge.on_event(TransportUp(gen=3))
        await asyncio.sleep(0)
        assert rig.calls == [(2, 1)]  # single-flight: gen 3 waits
        rig.gate.set()
        task = rig.bridge.task
        assert task is not None
        await task
        assert rig.calls == [(2, 1), (3, 1)]
        assert rig.bridge.last_generation == 3

    asyncio.run(_run())


def test_retry_backs_off_and_does_not_sleep_after_the_last_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delays: list[float] = []
    real_sleep = asyncio.sleep

    async def _sleep(delay: float) -> None:
        delays.append(delay)
        await real_sleep(0)

    async def _run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        rig.fail_first = 99
        rig.bridge.max_attempts = 4
        monkeypatch.setattr(asyncio, "sleep", _sleep)
        rig.bridge.on_event(TransportUp(gen=2))
        await real_sleep(0)
        task = rig.bridge.task
        assert task is not None
        await task
        assert [c[1] for c in rig.calls] == [1, 2, 3, 4]
        assert len(rig.attempt_failures) == 4
        assert rig.bridge.last_generation == 0
        assert any("exhausted" in m for m in rig.log.messages)

    asyncio.run(_run())
    assert delays == [0.5, 1.0, 2.0]


def test_down_and_failed_go_to_their_callbacks() -> None:
    async def _run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        event = RecoveryFailed(gen=2, attempts=3, cause=None)
        rig.bridge.on_event(_down())
        rig.bridge.on_event(event)
        await asyncio.sleep(0)
        assert len(rig.downs) == 1
        assert rig.failed == [event]
        assert rig.bridge.task is None

    asyncio.run(_run())


def test_missing_loop_logs_instead_of_raising() -> None:
    rig = _Rig(None)
    rig.bridge.on_event(TransportUp(gen=2))
    assert any("loop missing" in m for m in rig.log.messages)


def test_up_from_a_foreign_thread_hops_onto_the_loop() -> None:
    async def _run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        worker = threading.Thread(
            target=rig.bridge.on_event, args=(TransportUp(gen=2),)
        )
        worker.start()
        await asyncio.to_thread(worker.join)
        for _ in range(50):
            if rig.bridge.task is not None:
                break
            await asyncio.sleep(0.01)
        task = rig.bridge.task
        assert task is not None
        await task
        assert rig.calls == [(2, 1)]

    asyncio.run(_run())


def test_reattach_replaces_the_previous_listener() -> None:
    rig = _Rig(None)
    session = _Session()
    rig.bridge.attach(session)
    rig.bridge.attach(session)
    assert len(session.listeners) == 2
    assert session.unsubscribed == 1  # only the latest subscription stays live


def test_attach_ignores_sessions_without_transport_events() -> None:
    rig = _Rig(None)
    rig.bridge.attach(object())  # direct sessions have no transport events
    rig.bridge.attach(object())


def test_detach_unsubscribes_cancels_and_forgets_generations() -> None:
    async def _run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        session = _Session()
        rig.bridge.attach(session)
        rig.gate = asyncio.Event()
        rig.bridge.on_event(TransportUp(gen=2))
        await asyncio.sleep(0)
        task = rig.bridge.task
        assert task is not None and not task.done()

        await rig.bridge.detach()

        assert session.unsubscribed == 1
        assert task.cancelled()
        assert rig.bridge.task is None
        assert rig.bridge.last_generation == 0
        # A new connection may restart generation numbering at a low value.
        rig.gate = None
        rig.bridge.on_event(TransportUp(gen=1))
        await asyncio.sleep(0)
        assert rig.bridge.task is not None
        await rig.bridge.task
        assert rig.calls[-1] == (1, 1)

    asyncio.run(_run())


def test_failing_unsubscribe_does_not_break_detach() -> None:
    class _Bad:
        def subscribe_transport_events(self, listener: Any) -> Callable[[], None]:
            def _boom() -> None:
                raise RuntimeError("gone")

            return _boom

    async def _run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        rig.bridge.attach(_Bad())
        await rig.bridge.detach()

    asyncio.run(_run())
