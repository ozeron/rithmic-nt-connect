"""Process-wide L3 unix transport recovery (GatewayRuntime-owned).

Owns single-flight re-dial + generation. Does **not** own MD replay,
order-plant rearm, or capability/history policy (R4).

Oracle 2026-09-02 (transport-recovery-module-v3): APPROVE_WITH_FIXES.
"""

from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum


class FaultDomain(Enum):
    TRANSPORT = "transport"
    PLANT = "plant"
    CAPABILITY = "capability"


class TransportFaultKind(Enum):
    UNIX_EOF = "unix_eof"
    UNIX_DESYNC = "unix_desync"
    UNIX_NOT_CONNECTED = "unix_not_connected"
    UNIX_FRAME = "unix_frame"
    UNIX_DRAIN_FAILED = "unix_drain_failed"


@dataclass(frozen=True)
class TransportFault:
    kind: TransportFaultKind
    observed_gen: int
    cause: BaseException | None = None
    domain: FaultDomain = FaultDomain.TRANSPORT


@dataclass(frozen=True)
class TransportDown:
    gen: int
    fault: TransportFault


@dataclass(frozen=True)
class TransportUp:
    gen: int


@dataclass(frozen=True)
class RecoveryFailed:
    gen: int
    attempts: int
    cause: BaseException | None


type TransportEvent = TransportDown | TransportUp | RecoveryFailed
type TransportListener = Callable[[TransportEvent], None]
type ReplaceTransport = Callable[[], None]
type Pred = Callable[[], bool]


class TransportPhase(Enum):
    NEVER = "never"
    LIVE = "live"
    DOWN = "down"
    RECOVERING = "recovering"
    FAILED = "failed"
    STOPPING = "stopping"


class TransportRecoveryError(RuntimeError):
    """``ensure_live`` exhausted its bounded recovery budget."""


_GATEWAY_CODE_TO_KIND: dict[str, TransportFaultKind] = {
    "eof": TransportFaultKind.UNIX_EOF,
    "desync": TransportFaultKind.UNIX_DESYNC,
    "not_connected": TransportFaultKind.UNIX_NOT_CONNECTED,
    "frame_too_large": TransportFaultKind.UNIX_FRAME,
    "drain_failed": TransportFaultKind.UNIX_DRAIN_FAILED,
}


def fault_from_gateway_code(
    code: str,
    *,
    observed_gen: int,
    cause: BaseException | None = None,
) -> TransportFault | None:
    """Map a mux/gateway wire code to an L3 fault, or ``None`` if not transport.

    Capability / history denials (R4) and plant-channel codes return ``None``
    so callers never start L3 from a bare string blacklist alone.
    """
    key = str(code).strip().lower()
    if key in {
        "capability_denied",
        "history_denied_live_md",
        "permission_denied",
    }:
        return None
    kind = _GATEWAY_CODE_TO_KIND.get(key)
    if kind is None:
        return None
    return TransportFault(
        kind=kind,
        observed_gen=int(observed_gen),
        cause=cause,
        domain=FaultDomain.TRANSPORT,
    )


class TransportRecovery:
    """Single-flight L3 recovery for one ``GatewayRuntime`` / mux.

    Invariants:
    - Generation bumps **only** after a successful ``replace_transport``.
    - At most one DOWN emit per outage; duplicate faults coalesce.
    - Stale faults (``observed_gen != live_gen``) are ignored once live.
    - Listeners are never awaited (fire-and-forget).
    - ``STOPPING`` ignores faults and does not re-dial.
    """

    def __init__(
        self,
        *,
        replace_transport: ReplaceTransport,
        is_connected: Pred,
        max_attempts: int = 8,
        initial_backoff_sec: float = 0.5,
        max_backoff_sec: float = 15.0,
        ensure_timeout_sec: float = 60.0,
    ) -> None:
        self._replace_transport = replace_transport
        self._is_connected = is_connected
        self._max_attempts = max(1, int(max_attempts))
        self._initial_backoff_sec = float(initial_backoff_sec)
        self._max_backoff_sec = float(max_backoff_sec)
        self._ensure_timeout_sec = float(ensure_timeout_sec)
        self._lock = threading.RLock()
        self._cv = threading.Condition(self._lock)
        self._phase = TransportPhase.NEVER
        self._gen = 0
        self._listeners: list[TransportListener] = []
        self._flight_thread: threading.Thread | None = None
        self._last_failure: BaseException | None = None
        self._attempts = 0

    @property
    def generation(self) -> int:
        with self._lock:
            return self._gen

    @property
    def phase(self) -> TransportPhase:
        with self._lock:
            return self._phase

    def subscribe(self, listener: TransportListener) -> Callable[[], None]:
        with self._lock:
            self._listeners.append(listener)

        def _unsubscribe() -> None:
            with self._lock, contextlib.suppress(ValueError):
                self._listeners.remove(listener)

        return _unsubscribe

    def mark_stopping(self) -> None:
        with self._cv:
            self._phase = TransportPhase.STOPPING
            self._cv.notify_all()

    def mark_live_after_connect(self) -> int:
        """First successful dial (or attach): enter LIVE without DOWN/UP fan-out."""
        with self._cv:
            if self._phase is TransportPhase.STOPPING:
                return self._gen
            if self._gen == 0:
                self._gen = 1
            self._phase = TransportPhase.LIVE
            self._last_failure = None
            self._attempts = 0
            self._cv.notify_all()
            return self._gen

    def report_fault(self, fault: TransportFault) -> None:
        """Mux / wire reports an L3 fault. Non-transport domains are ignored."""
        if fault.domain is not FaultDomain.TRANSPORT:
            return
        with self._cv:
            if self._phase is TransportPhase.STOPPING:
                return
            if self._gen > 0 and int(fault.observed_gen) != int(self._gen):
                return
            if self._phase in (
                TransportPhase.DOWN,
                TransportPhase.RECOVERING,
                TransportPhase.FAILED,
            ):
                self._arm_flight_unlocked()
                return
            # NEVER or LIVE → first DOWN for this outage
            down_gen = self._gen
            self._phase = TransportPhase.DOWN
            self._last_failure = fault.cause
            event = TransportDown(gen=down_gen, fault=fault)
            self._arm_flight_unlocked()
        self._emit(event)

    def ensure_live(self) -> int:
        """Block until LIVE (or raise after budget). Returns live generation."""
        deadline = time.monotonic() + self._ensure_timeout_sec
        with self._cv:
            while True:
                if self._phase is TransportPhase.STOPPING:
                    raise TransportRecoveryError("transport stopping")
                if self._phase is TransportPhase.LIVE and self._is_connected():
                    return self._gen
                if self._phase is TransportPhase.NEVER:
                    if self._is_connected():
                        return self.mark_live_after_connect()
                    raise TransportRecoveryError("transport never connected")
                self._arm_flight_unlocked()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TransportRecoveryError(
                        f"transport recovery timed out "
                        f"(phase={self._phase.value}, attempts={self._attempts}, "
                        f"last={self._last_failure!r})"
                    )
                self._cv.wait(timeout=min(remaining, 1.0))

    def _arm_flight_unlocked(self) -> None:
        if self._phase is TransportPhase.STOPPING:
            return
        t = self._flight_thread
        if t is not None and t.is_alive():
            return
        if self._phase is TransportPhase.LIVE and self._is_connected():
            return
        self._phase = TransportPhase.RECOVERING
        thread = threading.Thread(
            target=self._flight_loop,
            name="gateway-transport-recovery",
            daemon=True,
        )
        self._flight_thread = thread
        thread.start()

    def _flight_loop(self) -> None:
        backoff = self._initial_backoff_sec
        attempts = 0
        last_exc: BaseException | None = None
        while attempts < self._max_attempts:
            with self._lock:
                if self._phase is TransportPhase.STOPPING:
                    return
            attempts += 1
            try:
                self._replace_transport()
                if not self._is_connected():
                    raise RuntimeError("replace_transport left mux disconnected")
                with self._cv:
                    if self._phase is TransportPhase.STOPPING:
                        return
                    self._gen += 1
                    self._phase = TransportPhase.LIVE
                    self._attempts = attempts
                    self._last_failure = None
                    up = TransportUp(gen=self._gen)
                    self._cv.notify_all()
                self._emit(up)
                return
            except Exception as exc:
                last_exc = exc
                with self._lock:
                    self._last_failure = exc
                    self._attempts = attempts
                time.sleep(backoff)
                backoff = min(backoff * 2.0, self._max_backoff_sec)
        with self._cv:
            if self._phase is TransportPhase.STOPPING:
                return
            self._phase = TransportPhase.FAILED
            failed = RecoveryFailed(gen=self._gen, attempts=attempts, cause=last_exc)
            self._cv.notify_all()
        self._emit(failed)

    def _emit(self, event: TransportEvent) -> None:
        with self._lock:
            listeners = list(self._listeners)
        for cb in listeners:
            with contextlib.suppress(Exception):
                cb(event)
