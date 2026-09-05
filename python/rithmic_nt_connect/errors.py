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
        NotConnectedError,
        ReconciliationUnavailableError,
    )
except ImportError:  # pragma: no cover

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


class VenueQueryUnavailable(RuntimeError):
    """Venue query cannot be answered authoritatively (no snapshot API)."""


# Channel failures that require plant resync (PyO3 types when built, else stubs).
CHANNEL_ERRORS: tuple[type[BaseException], ...] = (
    ChannelLaggedError,
    ChannelClosedError,
    NotConnectedError,
    ChannelError,
)

# Gateway unix-client codes that mean the parent sock / session is gone or
# not usable — must take the reconnect/resync path, not "transient" spam.
_RECONNECTABLE_GATEWAY_CODES = frozenset(
    {
        "not_connected",
        "eof",
        "shutting_down",
        "desync",
        "transport_reset",
        "frame_too_large",
    }
)


def is_reconnectable_poll_error(exc: BaseException) -> bool:
    """True when a poll failure should resync / reconnect, not soft-retry.

    ``GatewayError`` uses code ``not_connected`` (underscore). Matching only
    the spaced phrase ``not connected`` left gateway-mode clients in a
    half-dead transient loop (2026-08-25 MY043 incident RC2.1).
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
