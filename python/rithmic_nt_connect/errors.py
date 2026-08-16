"""Typed adapter errors (Python-visible)."""

from __future__ import annotations

try:
    from rithmic_nt_connect._lib import AlreadyConnectedError
    from rithmic_nt_connect._lib import ChannelLaggedError
    from rithmic_nt_connect._lib import ChannelClosedError
    from rithmic_nt_connect._lib import NotConnectedError
    from rithmic_nt_connect._lib import ReconciliationUnavailableError
except ImportError:  # pragma: no cover

    class ChannelError(RuntimeError):
        """Base for plant channel failures that require resync."""

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

else:

    class ChannelError(RuntimeError):
        """Base for plant channel failures that require resync."""


class VenueQueryUnavailable(RuntimeError):
    """Venue query cannot be answered authoritatively (no snapshot API)."""


# Channel failures that require plant resync (PyO3 types when built, else stubs).
CHANNEL_ERRORS: tuple[type[BaseException], ...] = (
    ChannelLaggedError,
    ChannelClosedError,
    NotConnectedError,
    ChannelError,
)
