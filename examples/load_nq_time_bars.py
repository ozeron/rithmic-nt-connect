#!/usr/bin/env python3
"""Fetch venue time bars only (async-rithmic get_historical_time_bars analog).

Rithmic TimeBarType is four units + a period:

    SECOND_BAR=1  MINUTE_BAR=2  DAILY_BAR=3  WEEKLY_BAR=4

There is no native hour type. Nautilus 1-HOUR maps to MINUTE_BAR period=60.
Daily/weekly windows are calendar YYYYMMDD on the wire (handled in Rust).

    python examples/load_nq_time_bars.py
    python examples/load_nq_time_bars.py --specs 1-DAY,1-HOUR

Close MotiveWave / R|Trader first. Market-data plants only (no PnL / orders).
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))


# Lookbacks stay small: do not pull 1s/1m when checking 1h/1d.
_SPEC_WINDOWS = {
    "1-DAY": timedelta(days=40),
    "1-WEEK": timedelta(days=90),
    "1-HOUR": timedelta(days=2),
    "15-MINUTE": timedelta(days=1),
    "5-MINUTE": timedelta(hours=6),
    "1-MINUTE": timedelta(hours=2),
}

_RITHMIC_NAME = {1: "SECOND_BAR", 2: "MINUTE_BAR", 3: "DAILY_BAR", 4: "WEEKLY_BAR"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--root", default="NQ")
    parser.add_argument("--exchange", default="CME")
    parser.add_argument(
        "--specs",
        default="1-DAY,1-HOUR,15-MINUTE",
        help="Comma-separated Nautilus specs (default: 1-DAY,1-HOUR,15-MINUTE)",
    )
    return parser.parse_args()


def main() -> int:
    from rithmic_nt_connect import (
        load_dotenv_files,
        load_front_month_instrument,
        load_time_bars,
    )
    from rithmic_nt_connect.config import SessionConfig
    from rithmic_nt_connect.data import bar_type_to_rithmic, external_bar_advertised
    from rithmic_nt_connect.session import connect_market_data_session

    load_dotenv_files(ROOT / ".env")
    args = _parse_args()

    from nautilus_trader.model.data import BarType

    try:
        cfg = SessionConfig.from_env()
    except Exception as exc:
        print(f"SKIP (no credentials): {exc}")
        return 2

    specs = [part.strip().upper() for part in args.specs.split(",") if part.strip()]
    unknown = [spec for spec in specs if spec not in _SPEC_WINDOWS]
    if unknown:
        print(
            f"unknown spec {unknown}; known: {sorted(_SPEC_WINDOWS)}", file=sys.stderr
        )
        return 2

    print(
        f"connecting system={cfg.system_name!r} user={cfg.user[:2]}*** "
        "plants=market_data (no pnl)"
    )
    print("NOTE: close MotiveWave / R|Trader first (one session per login).")

    session = connect_market_data_session(cfg)
    try:
        instrument = load_front_month_instrument(session, args.root, args.exchange)
        end = datetime.now(UTC)
        print(f"instrument {instrument.id}")
        print()
        print(f"{'spec':<12} {'rithmic':<22} {'live':<6} {'n':>5}  first → last (UTC)")
        for spec in specs:
            bar_type = BarType.from_str(f"{instrument.id}-{spec}-LAST-EXTERNAL")
            rtype, period = bar_type_to_rithmic(bar_type)
            start = end - _SPEC_WINDOWS[spec]
            bars = load_time_bars(session, instrument, start, end, bar_type)
            first = last = "-"
            if bars:
                first = datetime.fromtimestamp(bars[0].ts_event / 1e9, UTC).isoformat()
                last = datetime.fromtimestamp(bars[-1].ts_event / 1e9, UTC).isoformat()
            live = "yes" if external_bar_advertised(bar_type) else "no"
            wire = f"{_RITHMIC_NAME[rtype]} x{period}"
            print(f"{spec:<12} {wire:<22} {live:<6} {len(bars):>5}  {first} → {last}")
            if bars:
                last_bar = bars[-1]
                print(
                    f"             close={last_bar.close} vol={last_bar.volume} "
                    f"ohlc={last_bar.open}/{last_bar.high}/{last_bar.low}/{last_bar.close}"
                )
        return 0
    finally:
        session.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
