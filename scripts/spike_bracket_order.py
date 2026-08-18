#!/usr/bin/env python3
"""Plant-bracket spike harness — no place unless --place.

  uv run python scripts/spike_bracket_order.py
  uv run python scripts/spike_bracket_order.py --place --stop-ticks 40 --qty 1

--place requires RITHMIC_BRACKETS=1 and RITHMIC_ENABLE_TRADING=1.
RITHMIC_CONNECT_MODE is required by SessionConfig.from_env (direct|gateway).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--place", action="store_true")
    p.add_argument("--root", default="NQ", help="root symbol for front-month resolve")
    p.add_argument("--exchange", default="CME")
    p.add_argument("--qty", type=int, default=1)
    p.add_argument("--stop-ticks", type=int, default=40)
    p.add_argument("--target-ticks", type=int, default=None)
    p.add_argument("--side", default="Buy", choices=("Buy", "Sell"))
    p.add_argument("--seconds", type=float, default=8.0, help="poll window after place")
    args = p.parse_args(argv)

    from rithmic_nt_connect import env_truthy, load_dotenv_files
    from rithmic_nt_connect.config import SessionConfig
    from rithmic_nt_connect.front_month import resolve_front_month
    from rithmic_nt_connect.session import create_session

    load_dotenv_files(ROOT / ".env")

    if not args.place:
        print(
            "DRY: subscribe_bracket_updates / place_bracket_order / "
            "adjust_bracket_stop / adjust_bracket_target (Lucid proof open)"
        )
        return 0

    if not env_truthy(os.environ.get("RITHMIC_BRACKETS")) or not env_truthy(
        os.environ.get("RITHMIC_ENABLE_TRADING")
    ):
        print(
            "REFUSE --place: need RITHMIC_BRACKETS=1 and RITHMIC_ENABLE_TRADING=1",
            file=sys.stderr,
        )
        return 2

    cfg = SessionConfig.from_env()
    session = create_session(cfg)
    session.connect()
    smoke_localid = f"spike-bracket-{uuid.uuid4().hex[:8]}"
    saw_ack = False
    saw_reject = False
    basket_id = None
    try:
        front = resolve_front_month(session, args.root, args.exchange)
        session.subscribe_order_updates()
        session.subscribe_bracket_updates()
        session.place_bracket_order(
            symbol=front["trading_symbol"],
            exchange=front["trading_exchange"],
            side=args.side,
            price_type="Market",
            quantity=int(args.qty),
            localid=smoke_localid,
            duration="DAY",
            stop_ticks=int(args.stop_ticks),
            target_ticks=None if args.target_ticks is None else int(args.target_ticks),
        )
        print(
            f"PLACE sent front={front['trading_symbol']}.{front['trading_exchange']} "
            f"localid={smoke_localid}; polling {args.seconds}s for notify…"
        )
        deadline = time.monotonic() + max(0.0, args.seconds)
        while time.monotonic() < deadline:
            ev = session.poll_order_event()
            if ev is None:
                time.sleep(0.05)
                continue
            tag = str(ev.get("user_tag") or ev.get("localid") or "")
            if tag != smoke_localid:
                continue
            status = str(ev.get("status") or "")
            text = str(ev.get("text") or ev.get("report_text") or "")
            print(
                f"order_event: type={ev.get('type')} status={status!r} "
                f"basket_id={ev.get('basket_id')} tag={tag!r} text={text!r}"
            )
            if ev.get("basket_id"):
                basket_id = ev.get("basket_id")
            low = f"{status} {text}".lower()
            if any(tok in low for tok in ("reject", "denied", "fail", "error")):
                saw_reject = True
                break
            if basket_id and any(
                tok in low for tok in ("open", "accept", "fill", "submit", "new")
            ):
                saw_ack = True
                break
            if basket_id:
                saw_ack = True
                break
    finally:
        try:
            session.disconnect()
        except Exception as exc:
            print(f"disconnect warning: {exc}", file=sys.stderr)

    if saw_reject:
        print("PLACE rejected by venue/plant", file=sys.stderr)
        return 1
    if saw_ack or basket_id:
        print(f"PLACE observed basket_id={basket_id}")
        return 0
    print(
        "PLACE inconclusive: no basket_id / ack within poll window",
        file=sys.stderr,
    )
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
