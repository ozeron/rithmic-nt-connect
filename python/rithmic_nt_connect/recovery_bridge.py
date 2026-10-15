"""Shared gateway transport-recovery wiring for the data and exec clients.

The gateway runtime (Rust client) owns dialling and emits
``TransportDown`` / ``TransportUp`` / ``RecoveryFailed``. What to restore after
a re-dial is client policy (MD replay vs order-plant re-arm) and stays in the
client's ``restore`` callback; this module owns the rest: listener wiring, the
hop from the transport thread onto the client's loop, single-flight, generation
dedupe and bounded retry with backoff.

A client attaches on connect and detaches on disconnect. Detaching drops the
listener (a shared gateway runtime outlives any one client), cancels an
in-flight restore and forgets generations, which are only comparable within one
connection.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from typing import Any

from rithmic_nt_connect.transport import (
    RecoveryFailed,
    TransportDown,
    TransportUp,
)

_INITIAL_BACKOFF_S = 0.5
_MAX_BACKOFF_S = 8.0


class TransportRecoveryBridge:
    """Run one client's ``restore(generation, attempt)`` after each TRANSPORT_UP."""

    def __init__(
        self,
        *,
        label: str,
        task_name: str,
        log: Callable[[], Any],
        get_loop: Callable[[], asyncio.AbstractEventLoop | None],
        spawn: Callable[
            [asyncio.AbstractEventLoop, Coroutine[Any, Any, None], str],
            asyncio.Task[Any],
        ],
        restore: Callable[[int, int], Coroutine[Any, Any, None]],
        on_down: Callable[[TransportDown], None],
        on_failed: Callable[[RecoveryFailed], None],
        on_attempt_failed: Callable[[BaseException], None] | None = None,
        latch_now: Callable[[], None] | None = None,
        max_attempts: int = 5,
    ) -> None:
        """``on_down`` / ``on_failed`` run on the client's loop. ``latch_now``,
        when given, runs at once on the transport thread for Down and Failed: it
        must be thread-safe (the exec client's plant latch is a single store), and
        it closes the order path before the loop gets to the handler."""
        self._label = label
        self._task_name = task_name
        self._log = log
        self._get_loop = get_loop
        self._spawn = spawn
        self._restore = restore
        self._on_down = on_down
        self._on_failed = on_failed
        self._on_attempt_failed = on_attempt_failed
        self._latch_now = latch_now
        self._unsubscribe: Callable[[], None] | None = None
        self._pending_generation = 0
        self.max_attempts = max_attempts
        self.task: asyncio.Task[Any] | None = None
        self.last_generation = 0

    def attach(self, session: Any) -> None:
        """Subscribe to the session's transport events (gateway sessions only).

        Re-attaching (a reconnect) first drops the previous subscription so the
        shared runtime never holds two listeners for one client.
        """
        self._drop_listener()
        subscribe = getattr(session, "subscribe_transport_events", None)
        if callable(subscribe):
            unsubscribe = subscribe(self.on_event)
            self._unsubscribe = unsubscribe if callable(unsubscribe) else None

    async def detach(self) -> None:
        """Drop the listener, cancel an in-flight restore, forget generations."""
        self._drop_listener()
        task, self.task = self.task, None
        self._pending_generation = 0
        self.last_generation = 0
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    def _drop_listener(self) -> None:
        unsubscribe, self._unsubscribe = self._unsubscribe, None
        if unsubscribe is not None:
            try:
                unsubscribe()
            except Exception as exc:
                self._log().warning(f"{self._label} unsubscribe failed: {exc}")

    def on_event(self, event: object) -> None:
        """Called on the transport thread."""
        if isinstance(event, TransportDown):
            self._latch_and_hop(lambda: self._on_down(event))
        elif isinstance(event, TransportUp):
            self._log().warning(
                f"gateway TRANSPORT_UP gen={event.gen}; scheduling {self._label}"
            )
            self.schedule(event.gen)
        elif isinstance(event, RecoveryFailed):
            self._latch_and_hop(lambda: self._on_failed(event))

    def _latch_and_hop(self, handler: Callable[[], None]) -> None:
        if self._latch_now is not None:
            self._latch_now()
        loop = self._get_loop()
        if loop is None:
            self._log().error(f"{self._label}: client loop missing; handling inline")
            handler()
            return
        # A closed loop (shutdown) leaves nobody to read the state.
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(handler)

    def schedule(self, generation: int) -> None:
        """Thread-safe: transport events fire on the Rust runtime thread."""
        loop = self._get_loop()
        if loop is None:
            self._log().error(f"{self._label} not scheduled: client loop missing")
            return

        def _start() -> None:
            if generation <= self.last_generation:
                return
            if self.task is not None and not self.task.done():
                # A newer generation must not be lost behind a running restore.
                self._pending_generation = max(self._pending_generation, generation)
                return
            self.task = self._spawn(loop, self._run(generation), self._task_name)

        try:
            loop.call_soon_threadsafe(_start)
        except Exception as exc:
            self._log().error(f"{self._label} schedule failed: {exc}")

    async def _run(self, generation: int) -> None:
        while True:
            if generation > self.last_generation:
                await self._attempts(generation)
            pending, self._pending_generation = self._pending_generation, 0
            if pending <= max(generation, self.last_generation):
                return
            generation = pending

    async def _attempts(self, generation: int) -> None:
        backoff = _INITIAL_BACKOFF_S
        last_exc: BaseException | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                await self._restore(generation, attempt)
            except Exception as exc:
                last_exc = exc
                self._log().error(
                    f"{self._label} attempt {attempt}/{self.max_attempts} failed: {exc}"
                )
                if self._on_attempt_failed is not None:
                    self._on_attempt_failed(exc)
                if attempt < self.max_attempts:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2.0, _MAX_BACKOFF_S)
            else:
                self.last_generation = generation
                return
        self._log().error(
            f"{self._label} exhausted (generation={generation}): {last_exc}"
        )
