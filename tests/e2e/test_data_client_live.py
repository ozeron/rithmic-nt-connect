"""Live integration tests: Nautilus Data Testing Spec (TC-D*) suite.

Only tests that genuinely need a live LucidTrading session live here.
Pure-conversion assertions (TC-D14, advertised bar types) live in the unit
suites (``test_depth_convert.py``, ``test_history_convert.py``).

Markers:
  - ``live``  : needs credentials + network (auto-skip without RITHMIC_USER/PW)
  - ``slow``  : polls for a 1m EXTERNAL bar (up to 65s); deselect with ``-m "not slow"``

Usage:
  uv run pytest tests/e2e/test_data_client_live.py -v                # full sweep
  uv run pytest tests/e2e/test_data_client_live.py -v -m "not slow"  # fast subset
  uv run pytest tests/e2e/test_data_client_live.py -v -k D30         # single TC
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import ClassVar

import pytest
from nautilus_trader.model.data import BarType
from rithmic_nt_connect.data import payloads_to_bars
from rithmic_nt_connect.historical import (
    load_front_month_instrument,
    load_time_bars,
    load_trade_ticks,
)
from rithmic_nt_connect.providers import RithmicInstrumentProvider

# Whole suite is live (needs credentials + network); the `slow` bar test is
# marked separately on its method.
pytestmark = pytest.mark.live

# 1m bars can take up to 60s to arrive after subscribe.
_BAR_POLL_TIMEOUT_SEC = 65


def wait_for_event(
    poll: Callable[[], dict | None],
    event_type: str,
    *,
    timeout_sec: float,
    sleep_sec: float = 0.1,
) -> dict:
    """Poll until an event of ``event_type`` arrives; raise if timeout hits."""
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        ev = poll()
        if ev and ev.get("type") == event_type:
            return ev
        time.sleep(sleep_sec)
    raise AssertionError(f"no {event_type!r} event within {timeout_sec}s")


# ══════════════════════════════════════════════════════════════════════
# Group 1: Instruments
# ══════════════════════════════════════════════════════════════════════


class TestInstruments:
    """TC-D01, D03 — instrument loading."""

    def test_tc_d01_request_instruments(self, live_session):
        """Load all instruments for the venue — at least one, valid fields."""
        import asyncio

        provider = RithmicInstrumentProvider(live_session, [("NQ", "CME")])
        asyncio.run(provider.load_all_async())
        all_instruments = provider.list_all()
        assert len(all_instruments) >= 1, "at least one instrument loaded"
        for inst in all_instruments:
            assert inst.price_precision >= 1, f"{inst.id}: valid price precision"
            assert inst.price_increment.as_double() > 0, f"{inst.id}: valid tick"
            assert inst.multiplier.as_double() > 0, f"{inst.id}: valid multiplier"

    def test_tc_d03_load_specific_instrument(self, live_session):
        """TC-D03 — load a single NQ front month by ID — valid fields."""
        inst = load_front_month_instrument(live_session, "NQ", "CME")
        assert str(inst.id).endswith(".RITHMIC"), f"instrument_id: {inst.id}"
        assert inst.price_precision >= 1
        assert inst.price_increment.as_double() > 0
        assert inst.multiplier.as_double() > 0
        info = inst.info or {}
        assert info.get("rithmic_symbol"), "rithmic_symbol in info"
        assert info.get("rithmic_exchange"), "rithmic_exchange in info"


# ══════════════════════════════════════════════════════════════════════
# Group 2: Order book
# ══════════════════════════════════════════════════════════════════════


class TestOrderBook:
    """TC-D10 — L2 book subscribe (D14 managed-book lives in unit suite)."""

    def test_tc_d10_subscribe_book_deltas(self, live_session, live_front_month):
        """Subscribe L2 book deltas — receive at least one OrderBookDeltas event.

        LucidTrading demo may deny L2 access (permission denied [13]).
        """
        _, symbol, exchange = live_front_month
        try:
            live_session.subscribe_order_book_summary(symbol, exchange)
        except RuntimeError as exc:
            if "permission denied" in str(exc) or "not entitled" in str(exc):
                pytest.skip(f"L2 book not available on this account: {exc}")
            raise
        ev = wait_for_event(live_session.poll_event, "order_book", timeout_sec=20)
        assert ev.get("bid_price") or ev.get("ask_price"), "bid/ask levels present"


# ══════════════════════════════════════════════════════════════════════
# Group 3: Quotes
# ══════════════════════════════════════════════════════════════════════


class TestQuotes:
    """TC-D20 — live BBO subscribe."""

    def test_tc_d20_subscribe_quotes(self, live_session, live_front_month):
        """Subscribe BBO — receive QuoteTick with bid < ask."""
        _, symbol, exchange = live_front_month
        live_session.subscribe(symbol, exchange)
        ev = wait_for_event(live_session.poll_event, "bbo", timeout_sec=20)
        bid, ask = float(ev["bid_price"]), float(ev["ask_price"])
        assert bid < ask, f"bid {bid} < ask {ask}"
        assert int(ev["bid_size"]) >= 1, "positive bid size"
        assert int(ev["ask_size"]) >= 1, "positive ask size"


# ══════════════════════════════════════════════════════════════════════
# Group 4: Trades
# ══════════════════════════════════════════════════════════════════════


class TestTrades:
    """TC-D30, D31 — live trades subscribe + historical trades request."""

    def test_tc_d30_subscribe_trades(self, live_session, live_front_month):
        """Subscribe last-trade — receive TradeTick with aggressor side."""
        _, symbol, exchange = live_front_month
        live_session.subscribe(symbol, exchange)
        ev = wait_for_event(live_session.poll_event, "last_trade", timeout_sec=20)
        assert float(ev["trade_price"]) > 0, "positive trade price"
        assert int(ev["trade_size"]) >= 1, "positive trade size"
        assert ev.get("aggressor") in (1, 2, None), "valid aggressor"

    def test_tc_d31_request_historical_trades(self, live_session, live_front_month):
        """TC-D31 — request historical trade ticks — valid timestamps, prices, sizes.

        Uses a window ending 1 hour ago (not "now") to avoid the live-indexing
        boundary. The LucidTrading history plant can transiently return empty;
        the test skips rather than fails when that happens.
        """
        inst, *_ = live_front_month
        end = int(datetime.now(UTC).timestamp()) - 3600
        start = end - 900  # 15-minute window, 1h ago — guaranteed past
        ticks = load_trade_ticks(live_session, inst, start, end)
        if not ticks:
            pytest.skip("history plant returned empty (LucidTrading transient)")
        for t in ticks:
            assert float(t.price) > 0
            assert int(t.size) >= 1
            assert t.aggressor_side is not None


# ══════════════════════════════════════════════════════════════════════
# Group 5: Bars
# ══════════════════════════════════════════════════════════════════════


class TestBars:
    """TC-D40, D41 — live bar subscribe + historical bars request."""

    _BAR_PARAMS: ClassVar[list] = [
        pytest.param(2, 1, id="1m"),
        pytest.param(2, 15, id="15m"),
        pytest.param(2, 60, id="1h"),
        pytest.param(3, 1, id="1d"),
    ]

    @pytest.mark.slow
    @pytest.mark.parametrize(("rtype", "period"), _BAR_PARAMS)
    def test_tc_d40_subscribe_external_bars(
        self, live_session, live_front_month, rtype: int, period: int
    ):
        """Subscribe EXTERNAL time bars; skip if venue refuses or times out."""
        _, symbol, exchange = live_front_month
        try:
            live_session.subscribe_time_bars(symbol, exchange, rtype, period)
            ev = wait_for_event(
                live_session.poll_history_event,
                "time_bar",
                timeout_sec=_BAR_POLL_TIMEOUT_SEC,
            )
        except AssertionError as exc:
            pytest.skip(
                f"no {rtype=}/{period=} time_bar payload within "
                f"{_BAR_POLL_TIMEOUT_SEC}s ({exc}); record in skipped-spec"
            )
        finally:
            with contextlib.suppress(Exception):
                live_session.unsubscribe_time_bars(symbol, exchange, rtype, period)
        assert float(ev["open_price"]) > 0, "positive open"
        assert float(ev["high_price"]) >= float(ev["low_price"]), "high >= low"
        assert int(ev["volume"]) >= 0, "valid volume"

    def test_tc_d41_request_historical_bars(self, live_session, live_front_month):
        """TC-D41 — request historical bars — OHLCV, ascending, minute-grid open time.

        The venue ``marker`` is the bar CLOSE time; ``fields_to_bar`` shifts it
        back by the bar duration. A 1m bar's ``ts_event`` must therefore land on
        the minute grid (``ts_event % 60s == 0``). Skips when the history plant
        transiently returns empty (LucidTrading).
        """
        inst, *_ = live_front_month
        bar_type = BarType.from_str(f"{inst.id.symbol}.RITHMIC-1-MINUTE-LAST-EXTERNAL")
        end = int(datetime.now(UTC).timestamp())
        bars = load_time_bars(live_session, inst, end - 7200, end, bar_type)
        if not bars:
            pytest.skip("history plant returned empty (LucidTrading transient)")
        for i, b in enumerate(bars):
            assert b.high.as_double() >= b.low.as_double(), f"bar[{i}]: high >= low"
            assert b.high.as_double() >= b.open.as_double(), f"bar[{i}]: high >= open"
            assert b.high.as_double() >= b.close.as_double(), f"bar[{i}]: high >= close"
            assert int(b.volume) >= 0, f"bar[{i}]: non-negative volume"
            assert b.ts_event % 60_000_000_000 == 0, (
                f"bar[{i}]: open time not minute-aligned after close→open shift: "
                f"{b.ts_event}"
            )
            if i > 0:
                assert b.ts_event >= bars[i - 1].ts_event, "ascending timestamps"

    def test_tc_d42_historical_bars_close_to_open_shift(
        self, live_session, live_front_month
    ):
        """TC-D42 - raw marker is CLOSE; converted ts_event is OPEN (marker - 60s).

        Pins the plant contract that ``ts_event_ns`` (always ``marker*1e9`` for
        intraday bars) is the close time, so the adapter's unconditional
        close→open shift is correct against the real plant. Skips on empty.
        """
        inst, symbol, exchange = live_front_month
        end = int(datetime.now(UTC).timestamp())
        start = end - 7200
        raw = live_session.load_time_bars(symbol, exchange, start, end, 2, 1)
        if not raw:
            pytest.skip("history plant returned empty (LucidTrading transient)")
        bar_type = BarType.from_str(f"{inst.id.symbol}.RITHMIC-1-MINUTE-LAST-EXTERNAL")
        converted = payloads_to_bars(
            list(raw),
            symbol=symbol,
            exchange=exchange,
            bar_type=bar_type,
            price_precision=int(inst.price_precision),
        )
        assert len(converted) == len(raw)
        for i, (r, c) in enumerate(zip(raw, converted, strict=True)):
            close_ns = (
                int(r["ts_event_ns"])
                if r.get("ts_event_ns") is not None
                else int(r["marker"]) * 1_000_000_000
            )
            assert c.ts_event == close_ns - 60_000_000_000, (
                f"bar[{i}]: converted open != raw close - 60s "
                f"({c.ts_event} != {close_ns} - 60s)"
            )


# ══════════════════════════════════════════════════════════════════════
# Group 9: Lifecycle
# ══════════════════════════════════════════════════════════════════════


class TestLifecycle:
    """TC-D70 — subscribe then unsubscribe, clean teardown."""

    def test_tc_d70_unsubscribe_on_stop(self, live_session, live_front_month):
        """Subscribe then unsubscribe — no errors."""
        _, symbol, exchange = live_front_month
        live_session.subscribe(symbol, exchange)
        live_session.unsubscribe(symbol, exchange)
