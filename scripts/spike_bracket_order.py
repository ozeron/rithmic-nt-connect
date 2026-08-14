#!/usr/bin/env python3
"""Plant-bracket spike harness (Demo/Lucid) — no place unless --place.

Records accept/refuse for place_bracket_order. Does not prove disconnect survival
unless --place is used with operator follow-up.

Usage (from rithmic-connect, after maturin develop):

  uv run python scripts/spike_bracket_order.py
  uv run python scripts/spike_bracket_order.py --place --stop-ticks 40 --qty 1

Requires repo .env credentials. Refuses unless RITHMIC_BRACKETS=1 for --place.
connect_mode may be direct or gateway for --place.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--place", action="store_true", help="Actually submit a DAY StopOnly bracket")
    p.add_argument("--symbol", default="NQ")
    p.add_argument("--exchange", default="CME")
    p.add_argument("--qty", type=int, default=1)
    p.add_argument("--stop-ticks", type=int, default=40)
    p.add_argument("--target-ticks", type=int, default=None)
    p.add_argument("--side", default="Buy", choices=("Buy", "Sell"))
    args = p.parse_args(argv)

    print("spike_bracket_order: capability check")
    brackets = os.environ.get("RITHMIC_BRACKETS", "").strip() in {"1", "true", "TRUE", "yes"}
    mode = (os.environ.get("RITHMIC_CONNECT_MODE", "direct").strip() or "direct").lower()
    print(f"  RITHMIC_BRACKETS={brackets} connect_mode={mode!r}")
    if args.place and (not brackets or mode not in {"direct", "gateway"}):
        print(
            "REFUSE --place: need RITHMIC_BRACKETS=1 and "
            "RITHMIC_CONNECT_MODE=direct|gateway",
            file=sys.stderr,
        )
        return 2

    try:
        from rithmic_nt_connect.config import SessionConfig
        from rithmic_nt_connect.session import create_session
    except ImportError as exc:
        print(f"REFUSE: import failed ({exc}); run maturin develop", file=sys.stderr)
        return 2

    # Prefer loading sibling quant-guild-work .env if present
    root = Path(__file__).resolve().parents[1]
    for env_path in (root / ".env", root.parent / "algotrading" / "quant-guild-work" / ".env"):
        if env_path.is_file():
            try:
                from dotenv import load_dotenv

                load_dotenv(env_path, override=False)
                print(f"  loaded env {env_path}")
            except ImportError:
                pass
            break

    if not args.place:
        print(
            "DRY: API surface expected on WireSession: "
            "subscribe_bracket_updates, place_bracket_order, "
            "adjust_bracket_stop, adjust_bracket_target"
        )
        print(
            "STATUS note: brackets wired on direct + gateway; "
            "Lucid/Demo accept + basket-id + disconnect survival = NOT YET PROVEN"
        )
        return 0

    cfg = SessionConfig.from_env()  # type: ignore[attr-defined]
    session = create_session(cfg, plants="execution")
    session.connect()
    try:
        session.subscribe_order_updates()
        if hasattr(session, "subscribe_bracket_updates"):
            session.subscribe_bracket_updates()
        else:
            print("REFUSE: session missing subscribe_bracket_updates", file=sys.stderr)
            return 2
        session.place_bracket_order(
            symbol=args.symbol,
            exchange=args.exchange,
            side=args.side,
            price_type="Market",
            quantity=int(args.qty),
            localid="spike-bracket-1",
            duration="DAY",
            stop_ticks=int(args.stop_ticks),
            target_ticks=None if args.target_ticks is None else int(args.target_ticks),
        )
        print("PLACE submitted — check notifications for basket_id / rp_code")
        print("Operator: disconnect client and confirm legs still working (survival probe)")
    finally:
        try:
            session.disconnect()
        except Exception as exc:  # noqa: BLE001
            print(f"disconnect warning: {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
