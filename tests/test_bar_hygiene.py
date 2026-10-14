"""History is clamped to closed bars; a live bar never repeats one already delivered."""

from __future__ import annotations

import pytest
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.objects import Price, Quantity
from rithmic_nt_connect.data import BarSequenceGuard, closed_bars

M1 = BarType.from_str("MNQZ6.RITHMIC-1-MINUTE-LAST-EXTERNAL")
H1 = BarType.from_str("MNQZ6.RITHMIC-1-HOUR-LAST-EXTERNAL")
MIN = 60_000_000_000


def _bar(bt: BarType, ts_open: int) -> Bar:
    px = Price.from_str("100.00")
    return Bar(bt, px, px, px, px, Quantity.from_int(1), ts_open, ts_open)


def _stamped(bt: BarType, ts_open: int, dur: int, on_close: bool) -> Bar:
    return _bar(bt, ts_open + dur if on_close else ts_open)


@pytest.mark.parametrize(
    "on_close", [True, False], ids=["close-stamped", "open-stamped"]
)
def test_forming_bar_is_cut_from_history(on_close: bool) -> None:
    bars = [_stamped(M1, t, MIN, on_close) for t in (0, MIN, 2 * MIN)]
    # end at 2m30s: the 2m bar closes at 3m, still forming
    kept = closed_bars(bars, M1, 2 * MIN + 30_000_000_000, timestamp_on_close=on_close)
    assert len(kept) == 2
    # end exactly on a close keeps that bar
    assert len(closed_bars(bars, M1, 3 * MIN, timestamp_on_close=on_close)) == 3


@pytest.mark.parametrize(
    "on_close", [True, False], ids=["close-stamped", "open-stamped"]
)
def test_forming_hour_is_cut_from_history(on_close: bool) -> None:
    bars = [_stamped(H1, t, 60 * MIN, on_close) for t in (0, 60 * MIN)]
    assert len(closed_bars(bars, H1, 90 * MIN, timestamp_on_close=on_close)) == 1


def test_live_bar_repeating_history_is_dropped() -> None:
    guard = BarSequenceGuard()
    for b in (_bar(M1, 0), _bar(M1, MIN)):
        guard.note(b)
    assert guard.admit(_bar(M1, MIN)) is False  # boot minute served by both
    assert guard.admit(_bar(M1, 2 * MIN)) is True
    assert guard.admit(_bar(M1, 2 * MIN)) is False  # replay after resubscribe
    assert guard.admit(_bar(H1, 0)) is True  # per bar type


def test_older_history_never_rewinds_the_live_mark() -> None:
    guard = BarSequenceGuard()
    assert guard.admit(_bar(M1, 5 * MIN)) is True
    guard.note(_bar(M1, MIN))  # another sleeve's lookback arrives later
    assert guard.admit(_bar(M1, 6 * MIN)) is True


def test_todays_daily_bar_is_cut_from_history() -> None:
    d1 = BarType.from_str("MNQZ6.RITHMIC-1-DAY-LAST-EXTERNAL")
    day = 86_400_000_000_000
    bars = [_bar(d1, 10 * day), _bar(d1, 11 * day)]
    # end inside day 11: its bar is still forming
    assert [int(b.ts_event) for b in closed_bars(bars, d1, 11 * day + day // 2)] == [
        10 * day
    ]


def test_multi_day_bar_needs_its_whole_span() -> None:
    bt = BarType.from_str("MNQZ6.RITHMIC-2-DAY-LAST-EXTERNAL")
    day = 86_400 * 1_000_000_000
    bar = _bar(bt, 10 * day)
    assert closed_bars([bar], bt, 11 * day) == []  # one day into a two-day bar
    assert closed_bars([bar], bt, 12 * day) == [bar]
