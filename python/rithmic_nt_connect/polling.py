"""Plant event polling coordinator with backoff and error streak tracking."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from rithmic_nt_connect.errors import (
    CHANNEL_ERRORS,
    UnreadableEventError,
    is_reconnectable_poll_error,
    is_terminal_poll_error,
)

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
        batch_fn: Callable[[], list[dict[str, Any]]] | None = None,
        on_gap: Callable[[], Any] | None = None,
    ) -> None:
        """``batch_fn`` (the extension's ``next_events``) is preferred when given:
        it blocks in Rust until events arrive and returns a batch, so there is no
        idle sleep and one thread hop serves many events. ``poll_fn`` is the
        single-event form kept for callers and tests that supply their own.

        ``on_gap`` handles a ``gap`` event (the stream dropped events but its plant
        is healthy). It is retried until it succeeds, because a gap is reported once
        and nothing else would run it again. Without it a gap is handled like a
        channel error (``on_resync``)."""
        self.name = name
        self.batch_fn = batch_fn
        self.poll_fn = poll_fn
        self.on_event = on_event
        self.on_resync = on_resync
        self.on_gap = on_gap
        self.max_transient = max_transient
        self.on_latch = on_latch
        self.on_resync_start = on_resync_start
        self.on_resync_failed = on_resync_failed
        self._log = log or logger

    async def poll_session_events(self) -> list[dict[str, Any]]:
        """Wait for the next batch of events from the wire session.

        Raises PollTransientError on other non-channel failures; lets channel
        and transport failures propagate for classification.
        """
        try:
            if self.batch_fn is not None:
                return await asyncio.to_thread(self.batch_fn)
            event = await asyncio.to_thread(self.poll_fn)
            return [] if event is None else [event]
        except CHANNEL_ERRORS:
            raise
        except Exception as exc:
            if is_terminal_poll_error(exc) or is_reconnectable_poll_error(exc):
                raise
            self._log.warning(f"poll transient error: {exc}")
            raise PollTransientError(str(exc)) from exc

    async def poll_session_event(self) -> dict[str, Any] | None:
        """The first event of the next batch (single-event form)."""
        events = await self.poll_session_events()
        return events[0] if events else None

    async def _resync(self, backoff: float, transient_streak: int) -> tuple[float, int]:
        """Resubscribe after a channel error or a gap; returns the next state."""
        if self.name == "order" and self.on_resync_start is not None:
            self.on_resync_start()
        try:
            await self.on_resync()
            self._log.warning(f"{self.name} subscription resynced after channel error")
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

    async def _recover_gap(self) -> None:
        """Run ``on_gap`` until it succeeds."""
        on_gap = self.on_gap
        if on_gap is None:
            return
        delay = 0.05
        while True:
            try:
                await on_gap()
                self._log.warning(f"{self.name} stream recovered after a gap")
                return
            except Exception as exc:
                self._log.error(f"{self.name} gap recovery failed: {exc}")
                if self.name == "order" and self.on_resync_failed is not None:
                    self.on_resync_failed()
                await asyncio.sleep(delay)
                delay = min(delay * 2, 2.0)

    async def poll_iteration(
        self, backoff: float, transient_streak: int
    ) -> tuple[float, int] | None:
        """Run one iteration; return (backoff, streak) or None if plant latched."""
        try:
            events = await self.poll_session_events()
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
            if is_terminal_poll_error(exc):
                self._log.error(f"{self.name} stream failed or session closed: {exc}")
                if self.name == "order" and self.on_resync_failed is not None:
                    self.on_resync_failed()
                return None
            if not isinstance(exc, CHANNEL_ERRORS):
                # RemoteSession owns transport recovery. Keep polling without
                # replaying intents or latching before RecoveryFailed closes it.
                self._log.warning(f"poll transient error: {exc}")
                await asyncio.sleep(backoff)
                return min(backoff * 2, 2.0), transient_streak
            self._log.error(f"{self.name} poll channel error: {exc}")
            return await self._resync(backoff, transient_streak)

        if not events and self.batch_fn is None:
            await asyncio.sleep(0.05)
            return backoff, transient_streak

        # A gap or an unreadable event means venue state must be re-read. However
        # many there are in one batch, it is re-read once, after the batch: one
        # bad row must not cost one full recovery (and its denial window) each.
        lost = 0
        for index, event in enumerate(events):
            if event.get("type") == "gap":
                # The stream dropped events for a slow consumer: same recovery as
                # a channel error.
                self._log.error(
                    f"{self.name} stream dropped {event.get('skipped')} events"
                )
                lost += 1
                continue
            try:
                self.on_event(event)
            except UnreadableEventError as exc:
                # One event lost on a healthy stream: re-read venue state, keep going.
                self._log.error(
                    f"{self.name} event unreadable ({exc}); re-reading venue state"
                )
                lost += 1
                continue
            except Exception as exc:
                self._log.exception(
                    f"{self.name} event handler error (suppressed)", exc
                )
                if self.name == "order":
                    if self.on_latch is not None:
                        self.on_latch(
                            "order handler failure",
                            f"order stream stopped; cache/venue state divergent: {exc}",
                        )
                    dropped = len(events) - index - 1
                    if dropped:
                        self._log.error(
                            f"{self.name} poller stopped with {dropped} dequeued "
                            "events unhandled; the re-arm drain re-reads venue state"
                        )
                    return None
        if lost:
            if self.on_gap is not None:
                await self._recover_gap()
            else:
                backoff, transient_streak = await self._resync(
                    backoff, transient_streak
                )
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
