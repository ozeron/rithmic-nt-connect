"""History is clamped to closed bars; a live bar never repeats one already delivered."""

from __future__ import annotations

from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.objects import Price, Quantity
from rithmic_nt_connect.data import BarSequenceGuard, closed_bars

M1 = BarType.from_str("MNQZ6.RITHMIC-1-MINUTE-LAST-EXTERNAL")
H1 = BarType.from_str("MNQZ6.RITHMIC-1-HOUR-LAST-EXTERNAL")
MIN = 60_000_000_000


def _bar(bt: BarType, ts_open: int) -> Bar:
    px = Price.from_str("100.00")
    return Bar(bt, px, px, px, px, Quantity.from_int(1), ts_open, ts_open)


def test_forming_bar_is_cut_from_history() -> None:
    bars = [_bar(M1, 0), _bar(M1, MIN), _bar(M1, 2 * MIN)]
    # end at 2m30s: the 2m bar closes at 3m, still forming
    assert [
        int(b.ts_event) for b in closed_bars(bars, M1, 2 * MIN + 30_000_000_000)
    ] == [0, MIN]
    # end exactly on a close keeps that bar
    assert len(closed_bars(bars, M1, 3 * MIN)) == 3


def test_forming_hour_is_cut_from_history() -> None:
    bars = [_bar(H1, 0), _bar(H1, 60 * MIN)]
    assert len(closed_bars(bars, H1, 90 * MIN)) == 1


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
