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
