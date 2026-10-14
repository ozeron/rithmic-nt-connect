"""Gateway transport events the adapter reacts to.

The Rust gateway runtime recovers a lost parent socket on its own and reports it
as ``down`` / ``up`` / ``recovery_failed``. The data client replays market-data
intent on ``TransportUp``; the exec client latches the order plant on
``TransportDown`` and re-arms it after ``TransportUp``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class TransportFaultKind(Enum):
    UNIX_EOF = "unix_eof"


@dataclass(frozen=True)
class TransportFault:
    kind: TransportFaultKind
    observed_gen: int
    cause: BaseException | None = None


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


def event_from_rust(
    kind: str, generation: int, attempts: int, cause: str | None
) -> TransportEvent | None:
    """Build the adapter event for a Rust ``subscribe_transport_events`` callback."""
    if kind == "down":
        fault = TransportFault(TransportFaultKind.UNIX_EOF, observed_gen=generation)
        return TransportDown(gen=generation, fault=fault)
    if kind == "up":
        return TransportUp(gen=generation)
    if kind == "recovery_failed":
        return RecoveryFailed(
            gen=generation,
            attempts=attempts,
            cause=RuntimeError(cause) if cause else None,
        )
    return None
