"""INTERNAL vs EXTERNAL bar seam — deterministic, no venue.

The live full-node parity e2e (TC-D54) proved that a Nautilus ``TradingNode``'s
``1-MINUTE-INTERNAL`` bars equal the venue's ``1-MINUTE-EXTERNAL`` bars. It was
dropped 2026-08-19 because the Rithmic Test ticker plant goes silent (no
incremental trades or BBO, stale synthetic snapshots) while its bar plant keeps
running — a live INTERNAL-vs-EXTERNAL proof is not reliably hostable on Test
(TC-D40 remains the live proof that EXTERNAL bars are delivered). These unit
tests pin the same seam deterministically by driving the *same* engine
aggregator the ``LiveDataEngine`` uses (``TimeBarAggregator`` with
``time_bars_timestamp_on_close=False``) over the *same* venue trade payloads
the adapter converts, and comparing its output against the adapter's EXTERNAL
bar conversion (``payloads_to_bars`` close→open shift):

- INTERNAL (engine-aggregated) and EXTERNAL (venue payload) bars for the same
  minute carry the same open-minute ``ts_event``, identical OHLC, and exact
  volume — the invariant TC-D54 asserted live.
- Same-timestamp multi-price prints (sweeps) are all counted — volume exact
  proves none are dropped.
- The partial subscribe-minute sharp edge is pinned: when the tape starts
  mid-minute, the engine's bar undercounts versus the venue's full-minute bar —
  the exact reason the live e2e's complete-minute guard existed.

Fixtures use the real venue payload shape (``ssboe`` + ``usecs`` history
ticks; ``marker`` = bar CLOSE time for EXTERNAL bars).
"""

from __future__ import annotations

import pytest
from nautilus_trader.common.component import TestClock
from nautilus_trader.data.aggregation import TimeBarAggregator
from nautilus_trader.model.data import Bar, BarType
from rithmic_nt_connect.data import payloads_to_bars, payloads_to_trade_ticks
from rithmic_nt_connect.providers import future_from_reference

# 2026-08-19 16:00:00Z (minute-aligned seconds).
_MINUTE_NS = 1787155200
_NS = 1_000_000_000
_NS_PER_MIN = 60 * _NS


def _open_minute(ts_ns: int) -> int:
    """Floor a tick/bar timestamp to its open-minute grid (ns)."""
    return int(ts_ns) - (int(ts_ns) % _NS_PER_MIN)


_REF = {
    "trading_symbol": "NQU6",
    "trading_exchange": "CME",
    "underlying": "NQ",
    "product_code": "NQ",
    "currency": "USD",
    "tick_size": 0.25,
    "point_value": 20.0,
    "price_precision": 2,
    "expiration_date": "20260918",
    "is_tradable": True,
}


def _instrument():
    return future_from_reference(_REF)


def _ticks(payloads: list[dict]) -> list:
    # Each tick's ts_init must be >= its ts_event; use per-tick event time
    # to keep TimeBarAggregator historical mode correct (clock ~ event).
    ticks = []
    for p in payloads:
        ssboe = int(p.get("ssboe", _MINUTE_NS))
        usecs = int(p.get("usecs", 0))
        ts_event = ssboe * 1_000_000_000 + usecs * 1_000
        ticks.extend(
            payloads_to_trade_ticks(
                [p], symbol="NQU6", exchange="CME", price_precision=2, ts_init=ts_event
            )
        )
    ticks.sort(key=lambda t: t.ts_event)
    return ticks


def _internal_bars(ticks: list, on_close: bool = False) -> list[Bar]:
    """Drive the engine's 1-MINUTE-INTERNAL aggregator over the ticks.

    Mirrors the ``LiveDataEngine`` configuration the dropped live e2e used
    (``time_bars_timestamp_on_close=False``) so INTERNAL bars are stamped with
    the OPEN minute, exactly like the adapter's EXTERNAL conversion.
    """
    instrument = _instrument()
    bars: list[Bar] = []

    def handler(bar: Bar) -> None:
        bars.append(bar)

    aggregator = TimeBarAggregator(
        instrument,
        BarType.from_str("NQU6-CME.RITHMIC-1-MINUTE-LAST-INTERNAL"),
        handler,
        TestClock(),
        timestamp_on_close=on_close,
    )
    # Historical mode advances the clock to each tick's ts_init and closes
    # bars at interval boundaries — deterministic, no real timers.
    aggregator.set_historical_mode(True, handler)
    for tick in ticks:
        aggregator.handle_trade_tick(tick)
    return bars


def _external_bars(payloads: list[dict], on_close: bool = False) -> list[Bar]:
    return payloads_to_bars(
        payloads,
        symbol="NQU6",
        exchange="CME",
        bar_type=BarType.from_str("NQU6-CME.RITHMIC-1-MINUTE-LAST-EXTERNAL"),
        price_precision=2,
        ts_init=1_800_000_000_000_000_000,
        timestamp_on_close=on_close,
    )


def _tick(price: float, size: int, usecs: int, minute_offset: int = 0) -> dict:
    return {
        "type": "history_tick",
        "symbol": "NQU6",
        "exchange": "CME",
        "trade_price": price,
        "trade_size": size,
        "ssboe": _MINUTE_NS + minute_offset,
        "usecs": usecs,
    }


def _boundary_tick(minute_offset: int = 60) -> list:
    """The first tick of the next minute — closes the current minute's bucket."""
    return _ticks([_tick(100.50, 1, 10_000_000, minute_offset=minute_offset)])


def _venue_bar_for_minute(open_, high, low, close, volume: int) -> list[dict]:
    # marker = bar CLOSE time (venue contract); payloads_to_bars shifts it back
    # to the open minute.
    return [
        {
            "type": "history_bar",
            "symbol": "NQU6",
            "exchange": "CME",
            "open_price": open_,
            "high_price": high,
            "low_price": low,
            "close_price": close,
            "volume": volume,
            "marker": _MINUTE_NS + 60,
            "bar_type": 2,
            "period": "60",
        }
    ]


@pytest.mark.parametrize(
    "on_close", [True, False], ids=["close-stamped", "open-stamped"]
)
def test_internal_and_external_bars_agree_on_same_minute(on_close: bool) -> None:
    """Engine-aggregated INTERNAL bar == adapter-converted EXTERNAL bar, under
    either stamping convention (engine and adapter configured alike)."""
    ticks = _ticks(
        [
            _tick(100.00, 2, 45_000_000),
            _tick(100.50, 3, 50_000_000),
            _tick(100.75, 1, 55_000_000),
            _tick(100.25, 4, 58_000_000),
        ]
    )
    # The first tick of the next minute closes the 16:00:00Z bucket.
    internal = _internal_bars([*ticks, *_boundary_tick()], on_close)
    assert len(internal) == 1, "exactly one closed minute from this tape"
    internal_bar = internal[0]

    external_bar = _external_bars(
        _venue_bar_for_minute(
            open_=100.00, high=100.75, low=100.00, close=100.25, volume=10
        ),
        on_close,
    )[0]

    expected = (_MINUTE_NS + 60) * _NS if on_close else _MINUTE_NS * _NS
    assert int(internal_bar.ts_event) == expected, (
        f"INTERNAL bar stamped at the minute {'close' if on_close else 'open'}"
    )
    assert int(internal_bar.ts_event) == int(external_bar.ts_event), (
        "INTERNAL and EXTERNAL bars share one ts_event convention"
    )

    tick = float(_instrument().price_increment.as_double())
    for name, internal_px, external_px in (
        ("open", internal_bar.open, external_bar.open),
        ("high", internal_bar.high, external_bar.high),
        ("low", internal_bar.low, external_bar.low),
        ("close", internal_bar.close, external_bar.close),
    ):
        assert abs(float(internal_px) - float(external_px)) <= tick + 1e-9, (
            f"{name}: INTERNAL {float(internal_px)} != EXTERNAL {float(external_px)}"
        )
    assert int(internal_bar.volume) == int(external_bar.volume) == 10, (
        f"volume: INTERNAL {int(internal_bar.volume)} != EXTERNAL "
        f"{int(external_bar.volume)}"
    )


def test_same_timestamp_sweep_prints_all_counted() -> None:
    """A same-ts multi-price sweep must not drop any print (volume exact)."""
    # Four prints at the identical nanosecond: 1 + 1 + 1 + 7 = 10.
    ticks = _ticks(
        [
            _tick(100.00, 1, 55_123_456),
            _tick(100.50, 1, 55_123_456),
            _tick(100.75, 1, 55_123_456),
            _tick(100.25, 7, 55_123_456),
        ]
    )
    internal = _internal_bars([*ticks, *_boundary_tick()])[0]
    external = _external_bars(
        _venue_bar_for_minute(
            open_=100.00, high=100.75, low=100.00, close=100.25, volume=10
        )
    )[0]

    assert int(internal.volume) == 10, "all same-ts prints counted by the engine"
    assert int(internal.volume) == int(external.volume) == 10


def test_partial_subscribe_minute_undercounts_venue_bar() -> None:
    """Pin the sharp edge that motivated the live e2e's complete-minute guard.

    When the tape starts mid-minute (first tick at 16:00:45), the engine's
    INTERNAL bar for that minute only accumulates post-subscribe ticks, so its
    volume is less than the venue's full-minute EXTERNAL bar. Comparing the
    partial subscribe-minute against the venue bar is therefore invalid — a
    selector must require a minute that opened *after* the first tick.
    """
    ticks = _ticks([_tick(100.25, 5, 45_000_000)])  # tape starts mid-minute
    internal = _internal_bars([*ticks, *_boundary_tick()])[0]
    # The venue's bar for the same minute is full-minute: 5 pre-subscribe + 5
    # post-subscribe contracts.
    external = _external_bars(
        _venue_bar_for_minute(
            open_=100.00, high=100.25, low=100.00, close=100.25, volume=10
        )
    )[0]

    assert int(internal.volume) == 5, "engine counts only post-subscribe prints"
    assert int(internal.volume) != int(external.volume), (
        "partial subscribe-minute must NOT equal the venue's full-minute volume"
    )
