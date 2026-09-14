"""Plant event polling coordinator with backoff and error streak tracking."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from rithmic_nt_connect.errors import CHANNEL_ERRORS, is_reconnectable_poll_error

logger = logging.getLogger(__name__)


class PollTransientError(Exception):
    """Raised when a poll attempt fails with a non-channel, retryable error."""


class PlantPoller:
    """Encapsulates stream polling, transient error streak tracking, and resync."""

    def __init__(
        self,
        name: str,
        poll_fn: Callable[[], dict[str, Any] | None],
        on_event: Callable[[dict[str, Any]], None],
        on_resync: Callable[[], Any],
        max_transient: int = 5,
        on_latch: Callable[[str, str], None] | None = None,
        on_resync_start: Callable[[], None] | None = None,
        on_resync_failed: Callable[[], None] | None = None,
        log: Any | None = None,
    ) -> None:
        self.name = name
        self.poll_fn = poll_fn
        self.on_event = on_event
        self.on_resync = on_resync
        self.max_transient = max_transient
        self.on_latch = on_latch
        self.on_resync_start = on_resync_start
        self.on_resync_failed = on_resync_failed
        self._log = log or logger

    async def poll_session_event(self) -> dict[str, Any] | None:
        """Poll the next event from the wire session.

        Raises PollTransientError on non-channel failure; lets channel/reconnect
        errors propagate for resync.
        """
        try:
            return await asyncio.to_thread(self.poll_fn)
        except CHANNEL_ERRORS:
            raise
        except Exception as exc:
            if is_reconnectable_poll_error(exc):
                raise
            self._log.warning(f"poll transient error: {exc}")
            raise PollTransientError(str(exc)) from exc

    async def poll_iteration(
        self, backoff: float, transient_streak: int
    ) -> tuple[float, int] | None:
        """Run one iteration; return (backoff, streak) or None if plant latched."""
        try:
            event = await self.poll_session_event()
        except PollTransientError as exc:
            if self.name == "order":
                transient_streak += 1
                if transient_streak >= self.max_transient:
                    self._log.error(
                        f"{self.name} poll stream failing persistently: {exc}"
                    )
                    if self.on_latch is not None:
                        self.on_latch("order poll stream failure", str(exc))
                    return None
            await asyncio.sleep(0.1)
            return backoff, transient_streak
        except Exception as exc:
            self._log.error(f"{self.name} poll channel error: {exc}")
            if self.name == "order" and self.on_resync_start is not None:
                self.on_resync_start()
            try:
                await self.on_resync()
                self._log.warning(
                    f"{self.name} subscription resynced after channel error"
                )
                backoff = 0.05
                if self.name == "order":
                    transient_streak = 0
            except Exception as resync_exc:
                self._log.error(f"{self.name} subscription resync failed: {resync_exc}")
                if self.name == "order" and self.on_resync_failed is not None:
                    self.on_resync_failed()
                backoff = min(backoff * 2, 2.0)
            await asyncio.sleep(backoff)
            return backoff, transient_streak

        if event is None:
            await asyncio.sleep(0.05)
            return backoff, transient_streak

        try:
            self.on_event(event)
        except Exception as exc:
            self._log.exception(f"{self.name} event handler error (suppressed)", exc)
            if self.name == "order":
                if self.on_latch is not None:
                    self.on_latch(
                        "order handler failure",
                        f"order stream stopped; cache/venue state divergent: {exc}",
                    )
                return None
        return backoff, transient_streak

    async def run(self) -> None:
        """Continuously run polling iterations."""
        backoff = 0.05
        transient_streak = 0
        while True:
            outcome = await self.poll_iteration(backoff, transient_streak)
            if outcome is None:
                return
            backoff, transient_streak = outcome
