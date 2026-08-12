#!/usr/bin/env python3
"""Minimal live NQ ticks example against LucidTrading (credentials required).

Close MotiveWave / R|Trader first (one Rithmic session per login).

Usage:
  maturin develop
  cp .env.example .env   # fill credentials
  python examples/live_nq_ticks.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))


def _load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


def main() -> int:
    _load_dotenv(ROOT / ".env")
    from rithmic_connect.config import SessionConfig
    from rithmic_connect.session import create_rust_session

    cfg = SessionConfig.from_env()
    session = create_rust_session(cfg)
    session.connect()
    try:
        root = cfg.symbol or "NQ"
        exchange = cfg.exchange or "CME"
        front = session.get_front_month(root, exchange)
        trading = (
            (front.get("trading_symbol") if isinstance(front, dict) else None) or root
        )
        print(f"subscribing {trading}/{exchange}")
        try:
            ref = session.get_reference_data(str(trading), exchange)
            print(f"reference: tick_size={ref.get('tick_size')} underlying={ref.get('underlying')}")
        except Exception as exc:  # noqa: BLE001
            print(f"reference soft-fail: {exc}")
        session.subscribe(str(trading), exchange)
        deadline = time.time() + 20
        while time.time() < deadline:
            ev = session.poll_event()
            if ev is None:
                time.sleep(0.02)
                continue
            etype = ev.get("type")
            if etype == "last_trade":
                print(f"trade {ev.get('trade_price')} x {ev.get('trade_size')}")
            elif etype == "bbo":
                print(
                    f"bbo {ev.get('bid_price')}x{ev.get('bid_size')} / "
                    f"{ev.get('ask_price')}x{ev.get('ask_size')}"
                )
            else:
                print(f"event {etype}")
    finally:
        session.disconnect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
