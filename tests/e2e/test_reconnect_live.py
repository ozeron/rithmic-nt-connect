"""Live proof: ``resync_ticker_session`` restores ticker, book, and EXTERNAL bars.

Markers: ``live`` (creds), ``slow`` (~65s bar poll).

Usage:
  RITHMIC_TEST_DOTENV=.env uv run pytest tests/e2e/test_reconnect_live.py -v
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Protocol

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
        """Subscribe ticker, book, and 1m bars; resync; assert events resume."""
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


@pytest.fixture
def force_live_ticker_failure() -> Callable[[], None]:
    """Integration point for a real, ticker-only fault; never use Session.fake.

    The pre-wave-5 extension exposes only reset_ticker, which explicitly resets
    the plant rather than exercising the host's failure detector. D9 removes
    that method and does not specify a live fault-injection hook. Replace this
    skip with the real hook when one is available; it must affect only this
    test's ticker connection, never a login or another process.
    """
    pytest.skip(
        "needs wave 5: no live ticker-only failure hook (Session.fake is offline)"
    )


@pytest.fixture
def direct_reconnect_session(
    test_env, require_test_plant, force_live_ticker_failure, request
):
    """Check Test/direct/fault-injection prerequisites before opening a login."""
    from rithmic_nt_connect.config import ConnectMode, parse_connect_mode

    if parse_connect_mode(test_env["RITHMIC_CONNECT_MODE"]) != ConnectMode.DIRECT:
        pytest.skip("direct plant recovery needs RITHMIC_CONNECT_MODE=direct")
    return request.getfixturevalue("live_session")


class _GenerationEvent(Protocol):
    generation: int


def _has_session_events() -> bool:
    from rithmic_nt_connect._lib import Session

    return hasattr(Session, "subscribe_session_events")


@pytest.mark.slow
@pytest.mark.skipif(not _has_session_events(), reason="needs wave 5")
def test_direct_ticker_failure_restores_intent(
    direct_reconnect_session, force_live_ticker_failure, request
) -> None:
    """D9: observe Interrupted then Resumed and fresh ticks without re-subscribing.

    This contract is deliberately skipped until a real ticker failure can be
    injected. Session.fake would prove an offline scenario, not live recovery.
    The market-data-only fixture never opens gates or starts order/PnL plants;
    their reconnect counters cannot be observed with today's live hooks.
    """
    from queue import Empty, Queue

    from rithmic_nt_connect import _lib

    session = direct_reconnect_session
    _, symbol, exchange = request.getfixturevalue("live_front_month")
    interrupted_type = getattr(_lib, "Interrupted", None)
    resumed_type = getattr(_lib, "Resumed", None)
    failed_type = getattr(_lib, "RecoveryFailed", None)
    assert interrupted_type and resumed_type and failed_type, "incomplete D9 event API"
    events: Queue[_GenerationEvent] = Queue()
    listener = session.subscribe_session_events(events.put)
    try:
        session.subscribe(symbol, exchange)

        def matching_trade():
            event = session.poll_event(timeout_ms=100)
            if (
                event
                and event.get("symbol") == symbol
                and event.get("exchange") == exchange
            ):
                return event
            return None

        wait_for_any_event(
            matching_trade, ("last_trade",), timeout_sec=_EVENT_TIMEOUT_SEC
        )
        # Fresh venue time prevents a buffered pre-failure tick from passing.
        failure_time_ns = time.time_ns()
        force_live_ticker_failure()

        interrupted = None
        resumed = None
        deadline = time.monotonic() + 60
        while resumed is None and time.monotonic() < deadline:
            try:
                event = events.get(
                    timeout=min(0.2, max(0.001, deadline - time.monotonic()))
                )
            except Empty:
                continue
            assert not isinstance(event, failed_type), "ticker recovery gave up"
            if isinstance(event, interrupted_type):
                interrupted = event
            elif isinstance(event, resumed_type):
                assert interrupted is not None, "Resumed arrived before Interrupted"
                assert event.generation > interrupted.generation
                resumed = event
        assert interrupted is not None, "ticker failure never reported Interrupted"
        assert resumed is not None, "ticker recovery never reported Resumed within 60s"

        def fresh_trade():
            event = matching_trade()
            if event and (event.get("ts_event_ns") or 0) > failure_time_ns:
                return event
            return None

        wait_for_any_event(fresh_trade, ("last_trade",), timeout_sec=_EVENT_TIMEOUT_SEC)
        print(
            "direct ticker recovery: Interrupted -> Resumed; "
            f"ticks resumed: {symbol}/{exchange}"
        )
        print(
            "order/PnL reconnects: unobservable "
            "(data-only session; no live counter hooks)"
        )
    finally:
        try:
            listener.cancel()
        finally:
            session.unsubscribe(symbol, exchange)
