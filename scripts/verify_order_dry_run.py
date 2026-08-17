"""Phase 2 order-plant dry-run / gated live verify harness.

Default mode is **dry-run**: connect, login order plant, subscribe to order
updates, optionally poll for a few seconds, and exit **without placing orders**.

Live place is intentionally gated behind ``--live-place`` **and**
``RITHMIC_ENABLE_TRADING=1`` (plus an explicit far ``--price``). Test-plant order
routing with ``DEFAULT_APP_NAME`` is confirmed authorized (proven 2026-08-17); the
script cancels only its own basket, never ``cancel_all``.

Examples::

    # Dry-run only (safe; default)
    python scripts/verify_order_dry_run.py --seconds 5

    # Live far-limit smoke (requires explicit --price; BUY below / SELL above market)
    RITHMIC_ENABLE_TRADING=1 python scripts/verify_order_dry_run.py \\
        --live-place --side BUY --price 28000 --seconds 5

    # Explicitly refuse live place even if env is set
    python scripts/verify_order_dry_run.py --no-live-place
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

SMOKE_TAG_PREFIX = "rithmic-nt-connect-dryrun"


def _slim_event(ev: dict) -> dict:
    return {
        "type": ev.get("type"),
        "source": ev.get("source"),
        "notify_type_name": ev.get("notify_type_name"),
        "status": ev.get("status"),
        "basket_id": ev.get("basket_id"),
        "user_tag": ev.get("user_tag"),
        "symbol": ev.get("symbol"),
        "text": ev.get("text"),
        "report_text": ev.get("report_text"),
        "price": ev.get("price"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=3.0, help="poll window")
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="optional JSON report path under artifacts/",
    )
    parser.add_argument(
        "--live-place",
        action="store_true",
        help="DANGEROUS: place a 1-lot DAY limit (requires --price + env gate)",
    )
    parser.add_argument(
        "--no-live-place",
        action="store_true",
        help="force dry-run even if RITHMIC_ENABLE_TRADING is set",
    )
    parser.add_argument("--root", default="NQ", help="root symbol for front-month resolve")
    parser.add_argument("--exchange", default="CME")
    parser.add_argument(
        "--price",
        type=float,
        default=None,
        help="required with --live-place: BUY far below market, SELL far above",
    )
    parser.add_argument("--side", default="BUY", choices=["BUY", "SELL"])
    args = parser.parse_args(argv)

    from rithmic_nt_connect import env_truthy
    from rithmic_nt_connect import load_dotenv_files
    from rithmic_nt_connect.config import SessionConfig
    from rithmic_nt_connect.front_month import resolve_front_month
    from rithmic_nt_connect.session import create_session

    load_dotenv_files(ROOT / ".env")

    session_cfg = SessionConfig.from_env()

    env_trading = env_truthy(os.environ.get("RITHMIC_ENABLE_TRADING"))
    allow_live = bool(args.live_place) and env_trading and not args.no_live_place
    if args.live_place and not allow_live:
        print(
            "REFUSING --live-place: set RITHMIC_ENABLE_TRADING=1 and omit --no-live-place",
            file=sys.stderr,
        )
        return 3
    if allow_live and args.price is None:
        print(
            "REFUSING --live-place: pass an explicit --price "
            "(BUY far below market, SELL far above; no default)",
            file=sys.stderr,
        )
        return 3

    session = create_session(session_cfg)
    smoke_tag = f"{SMOKE_TAG_PREFIX}-{uuid.uuid4().hex[:8]}"
    report: dict = {
        "mode": "live_place" if allow_live else "dry_run",
        "system_name": session_cfg.system_name,
        "app_name": session_cfg.app_name,
        "connect_mode": session_cfg.connect_mode.value,
        "events": [],
        "placed": False,
        "smoke_tag": smoke_tag if allow_live else None,
    }
    try:
        session.connect()
        front = resolve_front_month(session, args.root, args.exchange)
        report["front"] = {
            "root": args.root,
            "trading_symbol": front["trading_symbol"],
            "trading_exchange": front["trading_exchange"],
        }
        session.subscribe_order_updates()
        getter = getattr(session, "resolved_account", None)
        if callable(getter):
            resolved = getter()
            if isinstance(resolved, dict):
                report["resolved_account"] = {
                    "account_id": resolved.get("account_id"),
                    "fcm_id": resolved.get("fcm_id"),
                    "ib_id": resolved.get("ib_id"),
                }
        print(
            f"order plant subscribed; front={front['trading_symbol']}.{front['trading_exchange']}; "
            f"mode={report['mode']}; account={report.get('resolved_account')}"
        )

        if allow_live:
            assert args.price is not None  # gated above
            print(
                f"WARNING: placing live {args.side} LIMIT @ {args.price} "
                f"tag={smoke_tag}",
                file=sys.stderr,
            )
            session.place_order(
                front["trading_symbol"],
                front["trading_exchange"],
                args.side,
                "LIMIT",
                1,
                smoke_tag,
                price=args.price,
                duration="DAY",
            )
            report["placed"] = True
            report["place"] = {
                "side": args.side,
                "price_type": "LIMIT",
                "price": args.price,
                "qty": 1,
                "user_tag": smoke_tag,
            }
            print("place_order sent; waiting for notifications…")

        deadline = time.monotonic() + max(0.0, args.seconds)
        while time.monotonic() < deadline:
            ev = session.poll_order_event()
            if ev is None:
                time.sleep(0.05)
                continue
            slim = _slim_event(ev)
            report["events"].append(slim)
            print(f"order_event: {slim}")

        report["cancelled"] = False
        if allow_live:
            # Cancel only the smoke order we placed (never cancel_all / other baskets).
            smoke_baskets = [
                e["basket_id"]
                for e in report["events"]
                if e.get("basket_id") and e.get("user_tag") == smoke_tag
            ]
            # Fallback: basket_ids from events that also carry our place price
            # when user_tag is missing on some notification shapes.
            if not smoke_baskets:
                smoke_baskets = [
                    e["basket_id"]
                    for e in report["events"]
                    if e.get("basket_id")
                    and e.get("price") is not None
                    and float(e["price"]) == float(args.price)
                    and e.get("symbol") == front["trading_symbol"]
                ]
            try:
                if smoke_baskets:
                    for bid in dict.fromkeys(smoke_baskets):
                        session.cancel_order(bid)
                        print(f"cancel_order sent basket_id={bid}")
                    report["cancelled"] = True
                    end_cancel = time.monotonic() + 2.0
                    while time.monotonic() < end_cancel:
                        ev = session.poll_order_event()
                        if ev is None:
                            time.sleep(0.05)
                            continue
                        slim = _slim_event(ev)
                        report["events"].append(slim)
                        print(f"order_event: {slim}")
                else:
                    print(
                        "WARN: no smoke basket_id found; skipping cancel "
                        "(will not cancel_all)",
                        file=sys.stderr,
                    )
            except Exception as cancel_exc:  # noqa: BLE001
                print(f"WARN: cancel after place failed: {cancel_exc}", file=sys.stderr)

        report["event_count"] = len(report["events"])
        if report["placed"]:
            if report["event_count"] == 0:
                print(
                    "FAIL: place_order sent but no order events received",
                    file=sys.stderr,
                )
                return 1
            print(
                f"LIVE PLACE OK event_count={report['event_count']} "
                f"placed={report['placed']} cancelled={report['cancelled']}"
            )
        else:
            print(f"DRY-RUN OK event_count={report['event_count']} placed={report['placed']}")
        if args.out is not None:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(report, indent=2) + "\n")
            print(f"wrote {args.out}")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            session.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
