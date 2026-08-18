#!/usr/bin/env python3
"""Backtest the 4-bar NQ rule on so-far-today Rithmic history (front month).

Each run reloads the window from the history plant. The same ``--until`` (or a
finished RTH day) is the same tape — sort/dedup live in the adapter.

Never claims a full day while the clock is still inside the window.
No live place. Close MotiveWave first (one Rithmic session).

Usage::

    python examples/backtest_nq_today.py --rth
    python examples/backtest_nq_today.py --rth --until 16:15:00
    python examples/backtest_nq_today.py --rth --check
"""

from __future__ import annotations

import argparse
import contextlib
import re
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

CHI = ZoneInfo("America/Chicago")


def _utc_from_ns(ts_event: int) -> str:
    return datetime.fromtimestamp(ts_event / 1_000_000_000, tz=UTC).isoformat()


_CLOCK = re.compile(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$")


def _parse_until(raw: str, day: datetime) -> datetime:
    """UTC clock ``HH:MM[:SS]`` on ``day``'s UTC date, or a full ISO-8601 stamp."""
    text = raw.strip()
    match = _CLOCK.fullmatch(text)
    if match:
        hour = int(match.group(1))
        minute = int(match.group(2))
        second = int(match.group(3) or 0)
        if hour > 23 or minute > 59 or second > 59:
            raise ValueError(f"invalid --until clock {raw!r}")
        return day.astimezone(UTC).replace(
            hour=hour, minute=minute, second=second, microsecond=0
        )
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(
            f"invalid --until {raw!r}; use HH:MM[:SS] UTC or an ISO-8601 timestamp"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _history_session():
    from rithmic_nt_connect.session import connect_market_data_session

    return connect_market_data_session()


def _window(
    now: datetime, rth: bool, until: datetime | None
) -> tuple[datetime, datetime, str]:
    chi_now = now.astimezone(CHI)
    if rth:
        rth_start = chi_now.replace(
            hour=8, minute=30, second=0, microsecond=0
        ).astimezone(UTC)
        rth_end = chi_now.replace(
            hour=15, minute=15, second=0, microsecond=0
        ).astimezone(UTC)
        start = rth_start
        end = min(now, rth_end)
        if until is not None:
            end = min(end, until)
        rth_hours = (rth_end - rth_start).total_seconds() / 3600.0
        hours = (end - start).total_seconds() / 3600.0
        if now < rth_start:
            raise ValueError(
                f"empty window: now {now.isoformat()} is before RTH open "
                f"{rth_start.isoformat()} UTC (08:30 CT)"
            )
        if end >= rth_end:
            span = f"complete RTH {hours:.2f}h "(
                f"(08:30-15:15 CT = "
                f"{rth_start.strftime('%H:%M')}-{rth_end.strftime('%H:%M')} UTC)"
            )
        else:
            remain = (rth_end - end).total_seconds() / 3600.0
            span = (
                f"partial RTH {hours:.2f}h of {rth_hours:.2f}h — session still open, "
                f"not a full day; RTH ends {rth_end.strftime('%H:%M')} UTC / 15:15 CT "
                f"({remain:.2f}h left)"
            )
        return start, end, span
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = now
    if until is not None:
        end = min(end, until)
    hours = (end - start).total_seconds() / 3600.0
    span = (
        f"partial UTC calendar day {hours:.2f}h of 24h "
        "(00:00 UTC → end; not a full day)"
    )
    return start, end, span


def _fingerprint(engine: Any, venue: Any, ticks: list[Any]) -> dict[str, Any]:
    fills = engine.trader.generate_order_fills_report()
    acct = engine.trader.generate_account_report(venue)
    n_fills = 0 if fills is None or getattr(fills, "empty", True) else len(fills)
    cash = None
    if acct is not None and not getattr(acct, "empty", True):
        cash = str(acct.iloc[-1]["total"])
    fill_sig: tuple[Any, ...] = ()
    if n_fills:
        cols = [
            c
            for c in ("ts_last", "side", "order_side", "last_px", "last_qty")
            if c in fills.columns
        ]
        fill_sig = tuple(fills[cols].astype(str).itertuples(index=False, name=None))
    return {
        "ticks": len(ticks),
        "first": _utc_from_ns(ticks[0].ts_event),
        "last": _utc_from_ns(ticks[-1].ts_event),
        "fills": n_fills,
        "cash": cash,
        "fill_sig": fill_sig,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Backtest the 4-bar NQ rule on so-far-today Rithmic history.",
        epilog="""examples:
  python examples/backtest_nq_today.py --rth
  python examples/backtest_nq_today.py --rth --until 16:15:00
  python examples/backtest_nq_today.py --rth --check

Pin --until (or wait until RTH is closed) to reload the same tape.
""",
    )
    parser.add_argument("--root", default="NQ", help="futures root (default NQ)")
    parser.add_argument("--exchange", default="CME", help="exchange code (default CME)")
    parser.add_argument(
        "--rth",
        action="store_true",
        help="only CME RTH (08:30-15:15 CT), printed and clamped in UTC",
    )
    parser.add_argument(
        "--until",
        metavar="HH:MM[:SS]",
        help="UTC end: 16:14:55 today, or a full ISO-8601 stamp",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="run the engine twice on the same ticks; fail if fills/cash differ",
    )
    args = parser.parse_args(argv)

    from rithmic_nt_connect import load_dotenv_files

    load_dotenv_files(ROOT / ".env")

    from nautilus_trader.backtest.engine import BacktestEngine
    from nautilus_trader.config import BacktestEngineConfig, LoggingConfig
    from nautilus_trader.model.data import BarType
    from nautilus_trader.model.enums import AccountType, BookType, OmsType
    from nautilus_trader.model.identifiers import TraderId, Venue
    from nautilus_trader.model.objects import Currency, Money
    from nq_four_bar import NqFourBarConfig, NqFourBarStrategy
    from rithmic_nt_connect import VENUE
    from rithmic_nt_connect.historical import (
        load_front_month_instrument,
        load_time_bars,
        load_trade_ticks,
    )

    now = datetime.now(UTC)
    try:
        until = _parse_until(args.until, now) if args.until else None
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 2
    try:
        start, end, span = _window(now, args.rth, until)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 2
    if end <= start:
        print(
            f"empty window {start.isoformat()} → {end.isoformat()} UTC", file=sys.stderr
        )
        return 2
    print(f"requested UTC {start.isoformat()} → {end.isoformat()}  {span}")

    session = _history_session()
    try:
        instrument = load_front_month_instrument(session, args.root, args.exchange)
        print(f"loading ticks {instrument.id}  {start.isoformat()} → {end.isoformat()}")
        try:
            ticks = load_trade_ticks(session, instrument, start, end)
        except Exception as exc:
            print(f"history error: {exc}", file=sys.stderr)
            return 1

        daily_type = BarType.from_str(f"{instrument.id}-1-DAY-LAST-EXTERNAL")
        daily_bars: list[Any] = []
        try:
            daily_from = start - timedelta(days=40)
            print(f"loading daily bars {daily_from.date()} → {end.date()} UTC")
            daily_bars = load_time_bars(
                session, instrument, daily_from, end, daily_type
            )
        except Exception as exc:
            print(f"history bars failed (SMA will be cold): {exc}", file=sys.stderr)
        print(f"lookback  daily={len(daily_bars)}  (VWAP from INTERNAL 1m on ticks)")
    finally:
        session.disconnect()

    print(f"instrument {instrument.id} prec={instrument.price_precision}")
    if not ticks:
        print(
            f"no usable ticks in {start.isoformat()} → {end.isoformat()}",
            file=sys.stderr,
        )
        print(
            "Close MotiveWave/R|Trader (one Rithmic session) and retry.",
            file=sys.stderr,
        )
        return 2
    print(
        f"ticks={len(ticks)}  "(
            f"first={_utc_from_ns(ticks[0].ts_event)} "
            f"last={_utc_from_ns(ticks[-1].ts_event)}"
        )
    )

    def run_once(*, log_strategy: bool) -> tuple[Any, dict[str, Any]]:
        engine = BacktestEngine(
            config=BacktestEngineConfig(
                trader_id=TraderId("BACK-001"),
                logging=LoggingConfig(
                    log_level="WARNING",
                    print_config=False,
                    log_component_levels={"NqFourBar-001": "INFO"}
                    if log_strategy
                    else {},
                ),
            )
        )
        engine.add_venue(
            venue=Venue(VENUE),
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            starting_balances=[Money.from_str("100000 USD")],
            base_currency=Currency.from_str("USD"),
            default_leverage=Decimal(20),
            book_type=BookType.L1_MBP,
            trade_execution=True,
            bar_execution=True,
        )
        engine.add_instrument(instrument)
        if daily_bars:
            engine.add_data(list(daily_bars))
        engine.add_data(list(ticks))
        engine.add_strategy(NqFourBarStrategy(NqFourBarConfig()))
        engine.run()
        return engine, _fingerprint(engine, Venue(VENUE), ticks)

    _, result = run_once(log_strategy=True)
    if args.check:
        _, again = run_once(log_strategy=False)
        same = (
            again["ticks"] == result["ticks"]
            and again["fills"] == result["fills"]
            and again["cash"] == result["cash"]
            and again["fill_sig"] == result["fill_sig"]
        )
        print(
            f"CHECK {'PASS' if same else 'FAIL'}  "
            f"fills={again['fills']} cash={again['cash']}"
        )
        if not same:
            print("same ticks produced different fills/cash", file=sys.stderr)
            return 3

    start_cash = Decimal(100000)
    end_cash = None
    with contextlib.suppress(Exception):
        end_cash = Decimal(str(result["cash"]).split()[0])
    pnl = None if end_cash is None else end_cash - start_cash
    pnl_s = "n/a" if pnl is None else f"{pnl:+.2f} USD"
    print(
        f"RESULT  ticks={result['ticks']} fills={result['fills']}  "
        f"cash={result['cash']}  pnl={pnl_s}  "
        f"{result['first']} → {result['last']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
