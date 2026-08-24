"""Live proof: ``resync_ticker_session`` restores ticker, book, and EXTERNAL bars.

Markers: ``live`` (creds), ``slow`` (~65s bar poll).

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
    @pytest.mark.slow
    def test_resync_restores_ticker_book_and_bars(self, live_session, live_front_month):
        """Subscribe all three surfaces, resync, assert events resume."""
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
