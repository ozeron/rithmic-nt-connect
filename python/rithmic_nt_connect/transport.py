"""Session recovery events shared by direct and gateway consumers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import final

# TODO(task 5.4): re-export the _lib pyclasses here once D9 lands in the extension.


@final
@dataclass(frozen=True)
class Interrupted:
    generation: int
    cause: str | None


@final
@dataclass(frozen=True)
class Resumed:
    generation: int


@final
@dataclass(frozen=True)
class RecoveryFailed:
    generation: int
    attempts: int
    cause: str | None


type SessionEvent = Interrupted | Resumed | RecoveryFailed

# Compatibility aliases for one release.
TransportDown = Interrupted
TransportUp = Resumed
