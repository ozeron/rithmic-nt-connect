"""Live proof: wire resync restores ticker + book + EXTERNAL bar intent.

Closes STATUS close-out 4 ("live-prove ticker resync"): the code path was
unit-tested (`test_data_client_unit.py`) but never proven against the venue.
This drives the single replay boundary — ``resync_ticker_session`` =
``reset_ticker`` + ``replay_subscription_intent`` — on a live session and
asserts every subscribed surface resumes delivering events.

Markers:
  - ``live`` : needs credentials + network (auto-skip without RITHMIC_USER/PW)
  - ``slow`` : waits up to ~65s for a 1m EXTERNAL bar twice

Usage:
  RITHMIC_TEST_DOTENV=.env uv run pytest tests/e2e/test_reconnect_live.py -v
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

import pytest
from rithmic_nt_connect.data import resync_ticker_session

pytestmark = pytest.mark.live

# NQ front month trades/BBO flow continuously during market hours; the 1m
# bar rolls within 60s of subscribing (proven by TC-D40).
_EVENT_TIMEOUT_SEC = 45
_BAR_POLL_TIMEOUT_SEC = 65


def wait_for_any_event(
    poll: Callable[[], dict | None],
    event_types: tuple[str, ...],
    *,
    timeout_sec: float,
    sleep_sec: float = 0.1,
) -> dict:
    """Poll until an event whose type is in ``event_types`` arrives."""
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        ev = poll()
        if ev and ev.get("type") in event_types:
            return ev
        time.sleep(sleep_sec)
    raise AssertionError(f"none of {event_types!r} arrived within {timeout_sec}s")


class TestResyncRestoresIntent:
    """resync_ticker_session must restore ALL subscription intent."""

    @pytest.mark.slow
    def test_resync_restores_ticker_book_and_bars(self, live_session, live_front_month):
        """Baseline on all three surfaces → reset+replay → all three resume.

        The venue treats duplicate subscribes as refresh, so the replay is
        idempotent; what this pins is that a re-established ticker plant is
        not left with zero subscriptions (the disconnect→zero-subs bug this
        boundary exists for).
        """
        _, symbol, exchange = live_front_month
        ticker_subs = {(symbol, exchange)}
        book_subs = {(symbol, exchange)}
        bar_subs = {(symbol, exchange, 2, 1)}  # 1-MINUTE-LAST-EXTERNAL

        live_session.subscribe(symbol, exchange)
        live_session.subscribe_order_book_summary(symbol, exchange)
        live_session.subscribe_time_bars(symbol, exchange, 2, 1)

        wait_for_any_event(
            live_session.poll_event,
            ("last_trade",),
            timeout_sec=_EVENT_TIMEOUT_SEC,
        )
        wait_for_any_event(
            live_session.poll_event,
            ("bbo", "order_book"),
            timeout_sec=_EVENT_TIMEOUT_SEC,
        )
        wait_for_any_event(
            live_session.poll_history_event,
            ("time_bar",),
            timeout_sec=_BAR_POLL_TIMEOUT_SEC,
        )

        asyncio.run(
            resync_ticker_session(live_session, ticker_subs, book_subs, bar_subs)
        )

        wait_for_any_event(
            live_session.poll_event,
            ("last_trade",),
            timeout_sec=_EVENT_TIMEOUT_SEC,
        )
        wait_for_any_event(
            live_session.poll_event,
            ("bbo", "order_book"),
            timeout_sec=_EVENT_TIMEOUT_SEC,
        )
        wait_for_any_event(
            live_session.poll_history_event,
            ("time_bar",),
            timeout_sec=_BAR_POLL_TIMEOUT_SEC,
        )
