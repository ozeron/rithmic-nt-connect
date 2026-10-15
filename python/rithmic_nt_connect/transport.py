"""Session recovery events shared by direct and gateway consumers."""

from rithmic_nt_connect._lib import Interrupted, RecoveryFailed, Resumed

type SessionEvent = Interrupted | Resumed | RecoveryFailed

# Compatibility aliases for one release.
TransportDown = Interrupted
TransportUp = Resumed

__all__ = [
    "Interrupted",
    "RecoveryFailed",
    "Resumed",
    "SessionEvent",
    "TransportDown",
    "TransportUp",
]
