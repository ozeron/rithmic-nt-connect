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
    return payloads_to_trade_ticks(
        payloads, symbol="NQU6", exchange="CME", price_precision=2
    )


def _internal_bars(ticks: list) -> list[Bar]:
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
        BarType.from_str("NQU6.RITHMIC-1-MINUTE-LAST-INTERNAL"),
        handler,
        TestClock(),
        timestamp_on_close=False,
    )
    # Historical mode advances the clock to each tick's ts_init and closes
    # bars at interval boundaries — deterministic, no real timers.
    aggregator.set_historical_mode(True, handler)
    for tick in ticks:
        aggregator.handle_trade_tick(tick)
    return bars


def _external_bars(payloads: list[dict]) -> list[Bar]:
    return payloads_to_bars(
        payloads,
        symbol="NQU6",
        exchange="CME",
        bar_type=BarType.from_str("NQU6.RITHMIC-1-MINUTE-LAST-EXTERNAL"),
        price_precision=2,
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


def _first_complete_traded_minute(
    bars: list[Bar], ticks: list, *, min_ts_ns: int = 0
) -> Bar | None:
    """First INTERNAL bar with volume whose open minute is complete by construction.

    A bar is only comparable against the venue's EXTERNAL bar when it opened
    strictly after the first *recent* tick's minute: the bar containing the
    subscribe moment is partial (its volume counts only post-subscribe
    prints). ``min_ts_ns`` bounds the tape to this run — a stale replay tick
    must never define the cutoff (that would silently accept the partial
    subscribe-minute). Returns ``None`` when the recent tape is empty or no
    complete traded minute has closed yet. This is the pure selector the
    dropped live e2e polled.
    """
    recent = [t for t in ticks if int(t.ts_event) >= min_ts_ns]
    if not recent:
        return None
    cutoff = _open_minute(min(int(t.ts_event) for t in recent))
    for bar in bars:
        if int(bar.volume) > 0 and _open_minute(int(bar.ts_event)) > cutoff:
            return bar
    return None


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


def test_internal_and_external_bars_agree_on_same_minute() -> None:
    """Engine-aggregated INTERNAL bar == adapter-converted EXTERNAL bar."""
    ticks = _ticks(
        [
            _tick(100.00, 2, 45_000_000),
            _tick(100.50, 3, 50_000_000),
            _tick(100.75, 1, 55_000_000),
            _tick(100.25, 4, 58_000_000),
        ]
    )
    # The first tick of the next minute closes the 16:00:00Z bucket.
    internal = _internal_bars([*ticks, *_boundary_tick()])
    assert len(internal) == 1, "exactly one closed minute from this tape"
    internal_bar = internal[0]

    external_bar = _external_bars(
        _venue_bar_for_minute(
            open_=100.00, high=100.75, low=100.00, close=100.25, volume=10
        )
    )[0]

    # Same open-minute grid: both bars are stamped with the OPEN minute.
    assert int(internal_bar.ts_event) == _MINUTE_NS * _NS, (
        "INTERNAL bar is minute-aligned open time (timestamp_on_close=False)"
    )
    assert int(internal_bar.ts_event) == int(external_bar.ts_event), (
        "INTERNAL and EXTERNAL bars share the open-minute ts_event"
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


def test_complete_minute_selector_excludes_partial_subscribe_minute() -> None:
    """The complete-minute selector must pick the first genuinely complete minute.

    Pins ``_first_complete_traded_minute`` — the pure selector the dropped
    live e2e polled. Given a tape that starts mid-minute, the partial
    subscribe-minute (16:00) must never be selected; the first complete minute
    (16:01) is. A late-starting or quiet recent tape must return ``None``
    rather than silently degrading to the partial minute.
    """
    # Tape starts mid-minute 16:00 (partial); minute 16:01 trades fully.
    tape = _ticks(
        [
            _tick(100.00, 2, 45_000_000),  # 16:00:45 — post-subscribe only
            _tick(100.25, 5, 30_000_000, minute_offset=60),  # 16:01:30
        ]
    )
    bars = _internal_bars([*tape, *_boundary_tick(120)])
    assert len(bars) == 2, "one partial (16:00) + one complete (16:01) minute"

    selected = _first_complete_traded_minute(bars, tape, min_ts_ns=0)
    assert selected is not None
    assert int(selected.ts_event) == (_MINUTE_NS + 60) * _NS, (
        "selector must return the first complete minute (16:01), not the "
        "partial subscribe-minute (16:00)"
    )
    assert int(selected.volume) == 5

    # A recent tape that starts late in 16:00 has no complete minute yet: the
    # cutoff becomes 16:01 and the 16:01 bar is still the in-progress one.
    late = _first_complete_traded_minute(bars, tape, min_ts_ns=(_MINUTE_NS + 50) * _NS)
    assert late is None, "no complete minute when the recent tape starts at 16:00:50"

    # A quiet recent tape (nothing recent) must not silently degrade to a
    # partial bar — this is the degradation that previously hid a dead feed.
    quiet = _first_complete_traded_minute(
        bars, tape, min_ts_ns=(_MINUTE_NS + 600) * _NS
    )
    assert quiet is None, "quiet tape -> no selection (no silent degradation)"


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
