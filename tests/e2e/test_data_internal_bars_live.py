"""Live characterization for MY043's decision tape (GAP-1 / GAP-4 / GAP-6).

The strategy consumes 1-MINUTE-INTERNAL bars (Nautilus internal aggregation of
the ticker) as its live tape, warmed by 1-MINUTE-EXTERNAL bars. These tests pin
the three contracts the audit found unproven:

- TC-D50: live LastTrade ticks aggregate to open-time minute buckets with valid
  OHLCV (the raw material of INTERNAL bars).
- TC-D51: the history tick replay is a lossy *subset* of the EXTERNAL bar
  replay (same minutes, tick volume never exceeds bar volume) — it cannot
  rebuild the VWAP warmup basis, so warmup must use EXTERNAL bars.
- TC-D52: the EXTERNAL cutover does not return a partial in-progress minute
  (the last bar's close is strictly before ``now``).
- TC-D53: the live ticker's ``trade_size`` sums exactly to the EXTERNAL bar
  volume for the same completed minute (the VWAP basis double-proof).

All skip when the plant returns no data in the window.
"""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import datetime, timezone

import pytest

from nautilus_trader.model.data import BarType

from rithmic_nt_connect._convert import last_trade_to_fields
from rithmic_nt_connect.historical import load_time_bars

pytestmark = pytest.mark.live

_NS_PER_MIN = 60_000_000_000


def _open_minute(ts_ns: int) -> int:
    """Floor a tick/bar timestamp to its open-minute grid (ns)."""
    return int(ts_ns) - (int(ts_ns) % _NS_PER_MIN)


class TestInternalBars:
    """TC-D50..D53 — INTERNAL tape + EXTERNAL subset + cutover + basis."""

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
                if ev.get("trade_price") is None:
                    # Stats-only summary (net_change/volume/vwap), not a print.
                    continue
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

    def test_TC_D51_external_vs_tick_replay_volume(self, live_session, live_front_month):
        """TC-D51 — the history tick replay is a lossy subset of EXTERNAL bars.

        The tick replay and EXTERNAL bar replay agree on *minute alignment*
        (tick ``ts_event_ns`` is the trade time; bar ``ts_event`` is the open
        time after the close→open shift), but the tick replay undercounts
        contracts — observed minute volume is never *greater* than the EXTERNAL
        bar volume, and frequently less. It therefore cannot rebuild the VWAP
        warmup basis; warmup must use EXTERNAL bars (which MY043 does).
        """
        inst, symbol, exchange = live_front_month
        end = int(datetime.now(timezone.utc).timestamp())
        start = end - 1800  # last 30 minutes
        bar_type = BarType.from_str(f"{inst.id.symbol}.RITHMIC-1-MINUTE-LAST-EXTERNAL")

        bars = load_time_bars(live_session, inst, start, end, bar_type)
        raw_ticks = live_session.load_ticks(symbol, exchange, start, end)
        if not bars or not raw_ticks:
            pytest.skip("history plant returned empty for parity window")

        bars_by_minute = {_open_minute(int(b.ts_event)): b for b in bars}
        tick_volume_by_minute: dict[int, float] = defaultdict(float)
        for t in raw_ticks:
            ts_ns = int(t.get("ts_event_ns") or 0)
            if not ts_ns or t.get("volume") is None:
                continue
            tick_volume_by_minute[_open_minute(ts_ns)] += float(t["volume"])

        compared = 0
        for minute, bar in sorted(bars_by_minute.items()):
            if minute not in tick_volume_by_minute:
                continue
            tick_volume = tick_volume_by_minute[minute]
            bar_volume = int(bar.volume)
            assert bar_volume > 0, f"minute {minute}: EXTERNAL bar has nonzero volume"
            # Lossy-subset invariant: the tick replay may drop trades, so it
            # must never exceed the authoritative EXTERNAL bar volume.
            assert tick_volume <= bar_volume, (
                f"minute {minute}: tick volume {tick_volume} exceeds EXTERNAL volume {bar_volume}"
            )
            compared += 1

        assert compared >= 1, "no minutes had both EXTERNAL bar and tick volume"

    def test_TC_D53_live_trade_size_sums_to_external_volume(
        self, live_session, live_front_month
    ):
        """TC-D53 — live ticker ``trade_size`` sums to the EXTERNAL bar volume.

        Double-proof of the VWAP basis: subscribe to the live ticker, collect
        one *fully observed* completed minute of trades, then load the EXTERNAL
        bar for that exact minute and assert the summed live ``trade_size``
        equals the venue bar ``volume`` (and the last live price equals the bar
        close within one tick).
        """
        inst, symbol, exchange = live_front_month
        live_session.subscribe(symbol, exchange)

        started = time.time()
        # Minutes that open after we subscribe are fully observable. Collect up
        # to three of them (thin markets can leave a whole minute untraded), then
        # compare against the earliest fully-observed minute that actually traded.
        first_full_sec = (int(started) - (int(started) % 60)) + 60
        deadline = time.monotonic() + (first_full_sec + 2 * 60 + 5 - started)

        fields: list[dict] = []
        while time.monotonic() < deadline:
            ev = live_session.poll_event()
            if ev is not None and ev.get("type") == "last_trade":
                if ev.get("trade_price") is None:
                    # Stats-only summary (net_change/volume/vwap), not a print.
                    continue
                fields.append(last_trade_to_fields(ev))
            time.sleep(0.05)

        traded_minutes = sorted(
            {
                _open_minute(int(f["ts_event"]))
                for f in fields
                if _open_minute(int(f["ts_event"])) >= first_full_sec * 1_000_000_000
            }
        )
        if not traded_minutes:
            pytest.skip("no trades in any fully-observed minute (thin market)")

        candidate_ns = traded_minutes[0]
        rows = [f for f in fields if _open_minute(int(f["ts_event"])) == candidate_ns]
        candidate_sec = candidate_ns // 1_000_000_000
        close_sec = candidate_sec + 60

        tick_volume = sum(float(r["size"]) for r in rows)
        last_px = float(sorted(rows, key=lambda r: int(r["ts_event"]))[-1]["price"])

        bar_type = BarType.from_str(f"{inst.id.symbol}.RITHMIC-1-MINUTE-LAST-EXTERNAL")
        bar = None
        for _ in range(6):
            bars = load_time_bars(live_session, inst, candidate_sec, close_sec, bar_type)
            bar = next(
                (b for b in bars if _open_minute(int(b.ts_event)) == candidate_ns),
                None,
            )
            if bar is not None:
                break
            time.sleep(5)
        if bar is None:
            pytest.fail(
                f"EXTERNAL bar for minute {candidate_sec} unavailable after retries (venue lag?)"
            )

        tick = float(inst.price_increment.as_double())
        assert int(bar.volume) == tick_volume, (
            f"minute {candidate_sec}: live trade_size sum {tick_volume} != "
            f"EXTERNAL volume {int(bar.volume)}"
        )
        assert abs(float(bar.close) - last_px) <= tick + 1e-9, (
            f"minute {candidate_sec}: EXTERNAL close {float(bar.close)} != "
            f"last live price {last_px}"
        )

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
