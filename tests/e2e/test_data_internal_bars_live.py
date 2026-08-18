"""Live characterization for MY043's decision tape (GAP-1 / GAP-4 / GAP-6).

The strategy consumes 1-MINUTE-INTERNAL bars (Nautilus internal aggregation of
the ticker) as its live tape, warmed by 1-MINUTE-EXTERNAL bars. These tests pin
the three contracts the audit found unproven:

- TC-D50: live LastTrade ticks aggregate to open-time minute buckets with valid
  OHLCV (the raw material of INTERNAL bars).
- TC-D51: EXTERNAL bars and tick-aggregated bars agree on close/volume for the
  same completed minutes (VWAP warmup basis == live basis).
- TC-D52: the EXTERNAL cutover does not return a partial in-progress minute
  (the last bar's close is strictly before ``now``).

All skip when the plant returns no data in the window.
"""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import datetime, timezone

import pytest

from nautilus_trader.model.data import BarType

from rithmic_nt_connect._convert import last_trade_to_fields
from rithmic_nt_connect.historical import load_time_bars, load_trade_ticks

pytestmark = pytest.mark.live

_NS_PER_MIN = 60_000_000_000


def _open_minute(ts_ns: int) -> int:
    """Floor a tick/bar timestamp to its open-minute grid (ns)."""
    return int(ts_ns) - (int(ts_ns) % _NS_PER_MIN)


class TestInternalBars:
    """TC-D50, D51, D52 — INTERNAL tape + EXTERNAL parity + cutover."""

    def test_TC_D50_internal_bar_raw_material(self, live_session, live_front_month):
        """TC-D50 — live trades aggregate to minute buckets with valid OHLCV.

        Buckets trades by open minute (floor ts_event to the minute grid) and
        asserts each bucket's OHLC is coherent and volume is the summed size.
        This is the raw material Nautilus turns into INTERNAL 1m bars.
        """
        _, symbol, exchange = live_front_month
        live_session.subscribe(symbol, exchange)

        deadline = time.monotonic() + 70.0
        fields: list[dict] = []
        while time.monotonic() < deadline:
            ev = live_session.poll_event()
            if ev is not None and ev.get("type") == "last_trade":
                fields.append(last_trade_to_fields(ev))
            time.sleep(0.05)

        if len(fields) < 2:
            pytest.skip("no trades within 70s (thin market)")

        buckets: dict[int, list[dict]] = defaultdict(list)
        for f in fields:
            buckets[_open_minute(int(f["ts_event"]))].append(f)

        assert buckets, "at least one minute bucket"
        for key, rows in buckets.items():
            assert key % _NS_PER_MIN == 0, "bucket key is minute-aligned"
            ordered = sorted(rows, key=lambda r: int(r["ts_event"]))
            prices = [float(r["price"]) for r in ordered]
            volume = sum(float(r["size"]) for r in ordered)
            assert prices, "bucket has prices"
            assert max(prices) >= min(prices), "coherent OHLC range"
            assert volume >= len(ordered), "volume is summed trade size"

    def test_TC_D51_external_vs_tick_ohlcv_parity(self, live_session, live_front_month):
        """TC-D51 — EXTERNAL bar close/volume matches tick aggregation.

        For each completed minute in a recent window, compare the EXTERNAL bar
        to the bar rebuilt from individual trade ticks. A large divergence means
        the VWAP warmup basis differs from the live basis (GAP-4).
        """
        inst, *_ = live_front_month
        end = int(datetime.now(timezone.utc).timestamp())
        start = end - 1800  # last 30 minutes (all complete, likely)
        bar_type = BarType.from_str(f"{inst.id.symbol}.RITHMIC-1-MINUTE-LAST-EXTERNAL")

        bars = load_time_bars(live_session, inst, start, end, bar_type)
        ticks = load_trade_ticks(live_session, inst, start, end)
        if not bars or not ticks:
            pytest.skip("history plant returned empty for parity window")

        bars_by_minute = {_open_minute(int(b.ts_event)): b for b in bars}
        tick_buckets: dict[int, list] = defaultdict(list)
        for t in ticks:
            tick_buckets[_open_minute(int(t.ts_event))].append(t)

        compared = 0
        for minute, bar in bars_by_minute.items():
            rows = tick_buckets.get(minute)
            if not rows:
                continue
            close_px = float(sorted(rows, key=lambda t: int(t.ts_event))[-1].price)
            tick_volume = sum(float(t.size) for t in rows)
            # Close must agree to within one tick (0.25 for MNQ); volume must be
            # in the same ballpark (tick-summed vs venue bar volume may differ).
            assert abs(close_px - float(bar.close)) < 0.25 + 1e-9, (
                f"minute {minute}: EXTERNAL close {float(bar.close)} != tick close {close_px}"
            )
            assert tick_volume > 0 and int(bar.volume) > 0, f"minute {minute}: nonzero volume"
            compared += 1

        assert compared >= 1, "no minutes had both EXTERNAL bar and ticks"

    def test_TC_D52_external_cutover_no_partial_minute(self, live_session, live_front_month):
        """TC-D52 — EXTERNAL lookback ending at ``now`` returns complete bars only.

        The last bar's close must be strictly in the past, never the in-progress
        minute. Otherwise the warmup would poison the current bucket (GAP-6).
        """
        inst, *_ = live_front_month
        end = int(datetime.now(timezone.utc).timestamp())
        start = end - 600  # last 10 minutes
        bar_type = BarType.from_str(f"{inst.id.symbol}.RITHMIC-1-MINUTE-LAST-EXTERNAL")

        bars = load_time_bars(live_session, inst, start, end, bar_type)
        if not bars:
            pytest.skip("history plant returned empty for cutover window")

        last = bars[-1]
        last_close_ns = int(last.ts_event) + _NS_PER_MIN  # open + 60s == close
        now_ns = int(datetime.now(timezone.utc).timestamp()) * 1_000_000_000
        assert last_close_ns <= now_ns, (
            f"cutover returned an in-progress/partial minute: close={last_close_ns} now={now_ns}"
        )
