"""TransportRecoveryBridge: policy shared by the data and exec clients."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from _stubs import WireSessionStub, _CaptureLog
from rithmic_nt_connect.recovery_bridge import TransportRecoveryBridge
from rithmic_nt_connect.session import _FlockedDirectSession, _RustGatewaySession
from rithmic_nt_connect.transport import (
    RecoveryFailed,
    SessionEvent,
    TransportDown,
    TransportUp,
)


class _Session(WireSessionStub):
    """Session in either mode: records listeners and unsubscribe calls."""

    def __init__(self) -> None:
        self.listeners: list[Callable[[SessionEvent], None]] = []
        self.unsubscribed = 0
        self.mode = "direct"

    def subscribe_session_events(self, listener: Callable[[SessionEvent], None]) -> Any:
        self.listeners.append(listener)

        def _unsubscribe() -> None:
            self.unsubscribed += 1

        return SimpleNamespace(cancel=_unsubscribe)


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
        generation=gen,
        cause="socket closed",
    )


async def _settle(rig: _Rig) -> None:
    await asyncio.sleep(0)
    task = rig.bridge.task
    assert task is not None
    await task


def test_up_restores_once_and_records_the_generation() -> None:
    async def _run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        rig.bridge.on_event(TransportUp(generation=2))
        await _settle(rig)
        assert rig.calls == [(2, 1)]
        assert rig.bridge.last_generation == 2

    asyncio.run(_run())


def test_duplicate_or_stale_generation_is_skipped() -> None:
    async def _run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        rig.bridge.on_event(TransportUp(generation=3))
        await _settle(rig)
        rig.bridge.on_event(TransportUp(generation=3))
        rig.bridge.on_event(TransportUp(generation=2))
        await asyncio.sleep(0)
        assert rig.calls == [(3, 1)]

    asyncio.run(_run())


def test_newer_generation_during_a_restore_runs_after_it() -> None:
    async def _run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        rig.gate = asyncio.Event()
        rig.bridge.on_event(TransportUp(generation=2))
        await asyncio.sleep(0)
        rig.bridge.on_event(TransportUp(generation=3))
        rig.bridge.on_event(TransportUp(generation=3))
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
        rig.bridge.on_event(TransportUp(generation=2))
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
        event = RecoveryFailed(generation=2, attempts=3, cause=None)
        rig.bridge.on_event(_down())
        rig.bridge.on_event(event)
        await asyncio.sleep(0)
        assert len(rig.downs) == 1
        assert rig.failed == [event]
        assert rig.bridge.task is None

    asyncio.run(_run())


def test_missing_loop_logs_instead_of_raising() -> None:
    rig = _Rig(None)
    rig.bridge.on_event(TransportUp(generation=2))
    assert any("loop missing" in m for m in rig.log.messages)


def test_up_from_a_foreign_thread_hops_onto_the_loop() -> None:
    async def _run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        worker = threading.Thread(
            target=rig.bridge.on_event, args=(TransportUp(generation=2),)
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


@pytest.mark.parametrize("mode", ["direct", "gateway"])
def test_bridge_runs_in_both_modes(mode: str) -> None:
    async def run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        session = _Session()
        wrapper = (
            _FlockedDirectSession(session, object())
            if mode == "direct"
            else _RustGatewaySession(session)
        )
        rig.bridge.attach(wrapper)
        listener = session.listeners[0]
        listener(_down())
        listener(TransportUp(generation=2))
        await _settle(rig)
        assert rig.downs == [_down()]
        assert rig.calls == [(2, 1)]
        await rig.bridge.detach()
        assert session.unsubscribed == 1

    asyncio.run(run())


def test_detach_unsubscribes_cancels_and_forgets_generations() -> None:
    async def _run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        session = _Session()
        rig.bridge.attach(session)
        rig.gate = asyncio.Event()
        rig.bridge.on_event(TransportUp(generation=2))
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
        rig.bridge.on_event(TransportUp(generation=1))
        await asyncio.sleep(0)
        assert rig.bridge.task is not None
        await rig.bridge.task
        assert rig.calls[-1] == (1, 1)

    asyncio.run(_run())


def test_failing_unsubscribe_does_not_break_detach() -> None:
    class _Bad(WireSessionStub):
        def subscribe_session_events(self, listener: Any) -> Any:
            def _boom() -> None:
                raise RuntimeError("gone")

            return SimpleNamespace(cancel=_boom)

    async def _run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        rig.bridge.attach(_Bad())
        await rig.bridge.detach()

    asyncio.run(_run())


def test_down_and_failed_from_the_transport_thread_run_on_the_loop() -> None:
    """Handlers touch client state, so they run on the client's loop; only the
    thread-safe latch runs at once on the transport thread."""

    async def _run() -> None:
        loop_thread = threading.get_ident()
        rig = _Rig(asyncio.get_running_loop())
        latched: list[int] = []
        handler_threads: list[int] = []
        rig.bridge._latch_now = lambda: latched.append(threading.get_ident())
        rig.bridge._on_down = lambda e: handler_threads.append(threading.get_ident())
        rig.bridge._on_failed = lambda e: handler_threads.append(threading.get_ident())
        failed = RecoveryFailed(generation=2, attempts=3, cause=None)
        for event in (_down(), failed):
            worker = threading.Thread(target=rig.bridge.on_event, args=(event,))
            worker.start()
            await asyncio.to_thread(worker.join)
        assert len(latched) == 2 and loop_thread not in latched, "latched at once"
        for _ in range(50):
            if len(handler_threads) == 2:
                break
            await asyncio.sleep(0.01)
        assert handler_threads == [loop_thread, loop_thread]

    asyncio.run(_run())


@pytest.mark.parametrize("failed", [False, True])
def test_interruption_cancels_old_rearm_before_a_new_resume(failed: bool) -> None:
    async def run() -> None:
        rig = _Rig(asyncio.get_running_loop())
        rig.gate = asyncio.Event()
        rig.bridge.on_event(TransportUp(generation=2))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        old_task = rig.bridge.task
        assert old_task is not None
        event = (
            RecoveryFailed(generation=2, attempts=3, cause="gone")
            if failed
            else _down(2)
        )
        rig.bridge.on_event(event)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert old_task.cancelled()
        assert rig.bridge.last_generation == 0
        rig.gate.set()
        rig.bridge.on_event(TransportUp(generation=3))
        await _settle(rig)
        assert rig.bridge.last_generation == 3

    asyncio.run(run())
