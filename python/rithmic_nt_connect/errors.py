"""Typed adapter errors (Python-visible)."""

from __future__ import annotations


class ChannelError(RuntimeError):
    """Base for plant channel failures that require resync."""


try:
    # Native classes when the extension is built, else the fallbacks below. The
    # ``_lib.pyi`` mirrors the fallback hierarchy (see that file's note) so the
    # two branches stay structurally identical to the checker.
    from rithmic_nt_connect._lib import (
        AlreadyConnectedError,
        ChannelClosedError,
        ChannelLaggedError,
        GatewayError,
        NotConnectedError,
        ReconciliationUnavailableError,
    )
except ImportError:  # pragma: no cover

    class GatewayError(RuntimeError):
        """Gateway-mode failure carrying the wire ``code`` and ``message``."""

        code: str
        message: str

    class AlreadyConnectedError(RuntimeError):
        """Session already connected (idempotent connect is safe to suppress)."""

    class ChannelLaggedError(ChannelError):
        """Broadcast receiver lagged; messages were skipped."""

    class ChannelClosedError(ChannelError):
        """Plant subscription channel closed."""

    class NotConnectedError(ChannelError):
        """Required plant is not connected."""

    class ReconciliationUnavailableError(RuntimeError):
        """Order-history reconciliation cannot be answered authoritatively."""


class UnreadableEventError(ValueError):
    """A venue event the adapter could not convert. The stream itself is
    healthy: the event is lost, so the consumer re-reads venue state (as after a
    gap) instead of stopping."""


class VenueQueryUnavailable(RuntimeError):
    """Venue query cannot be answered authoritatively (no snapshot API)."""


# Channel failures that require plant resync (PyO3 types when built, else stubs).
CHANNEL_ERRORS: tuple[type[BaseException], ...] = (
    ChannelLaggedError,
    ChannelClosedError,
    NotConnectedError,
    ChannelError,
)

# Gateway connection-loss codes: the host owns reconnecting; these alone do
# not prove that a consumer's stream has ended.
_RECONNECTABLE_GATEWAY_CODES = frozenset(
    {
        "not_connected",
        "eof",
        "shutting_down",
        "transport_reset",
        "frame_too_large",
    }
)


def is_reconnectable_poll_error(exc: BaseException) -> bool:
    """True when a poll failure indicates interrupted transport or a channel.

    ``GatewayError`` uses code ``not_connected`` (underscore). Matching only
    the spaced phrase ``not connected`` left gateway-mode clients in a
    half-dead transient loop (2026-08-25 MY043 incident RC2.1). Host recovery now
    owns reconnecting; consumers use ``is_terminal_poll_error`` to decide when
    polling must stop.
    """
    if isinstance(exc, CHANNEL_ERRORS):
        return True
    code = getattr(exc, "code", None)
    if isinstance(code, str) and code.lower() in _RECONNECTABLE_GATEWAY_CODES:
        return True
    text = str(exc).lower()
    return (
        "forced logout" in text
        or "connection closed" in text
        or "not connected" in text
        or "not_connected" in text
        or "channel closed" in text
        or "channel lagged" in text
    )


def is_terminal_poll_error(exc: BaseException) -> bool:
    """True when polling cannot continue on this session.

    PyO3 maps host ChannelClosed/NotConnected to ChannelClosedError and
    NotConnectedError (their messages can be just the plant name). Under D8.4,
    stream closure is exposed only after RecoveryFailed or session close.
    Gateway connection-loss errors remain transient while RemoteSession re-dials.
    Legacy RuntimeError messages match only explicit session closure or forced
    logout; generic connection-loss text does not prove that recovery has ended.
    A lagged channel or generic ChannelError alone is not proof of closure.
    """
    if isinstance(exc, (ChannelClosedError, NotConnectedError)):
        return True
    if isinstance(exc, ChannelLaggedError):
        return False
    text = str(exc).lower()
    return "forced logout" in text or "session closed" in text
