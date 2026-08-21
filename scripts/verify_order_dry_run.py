"""Phase 2 order-plant dry-run / gated live verify harness.

Default mode is **dry-run**: connect, login order plant, subscribe to order
updates, optionally poll for a few seconds, and exit **without placing orders**.

    Live place is intentionally gated behind ``--live-place`` **and**
    ``RITHMIC_ENABLE_TRADING=1`` (plus an explicit far ``--price`` / stop
    trigger). Test-plant order routing with ``DEFAULT_APP_NAME`` is confirmed
    authorized (proven 2026-08-17); the script cancels only its own basket,
    never ``cancel_all``.

Examples::

    # Dry-run only (safe; default)
    python scripts/verify_order_dry_run.py --seconds 5

    # Live far-limit smoke (requires explicit --price; BUY below / SELL above market)
    RITHMIC_ENABLE_TRADING=1 python scripts/verify_order_dry_run.py \\
        --live-place --side BUY --price 28000 --seconds 5

    # Live far stop (never marketable: SELL trigger below / BUY above market;
    # --auto-trigger-offset derives it from the polled last trade)
    RITHMIC_ENABLE_TRADING=1 python scripts/verify_order_dry_run.py \\
        --live-place --order-type STOP_MARKET --side SELL \\
        --auto-trigger-offset 500 --seconds 12

    # Explicitly refuse live place even if env is set
    python scripts/verify_order_dry_run.py --no-live-place
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

SMOKE_TAG_PREFIX = "rithmic-nt-connect-dryrun"

# Single source of truth for the CME resting-stop rule (live-proven on
# Rithmic Test 2026-08-21: a wrong-side stop is exchange-rejected, a
# resting one is not). Help texts, examples, and _derive_trigger all use
# these phrases — never restate the rule inline.
_RESTING_SELL_RULE = "SELL stop trigger BELOW the last trade"
_RESTING_BUY_RULE = "BUY stop trigger ABOVE the last trade"


def _valid_offset(offset: float) -> bool:
    """A derived stop must rest strictly away from the market: an offset of
    zero sits at the market and negative/non-finite values land through it —
    any of those makes the stop marketable and executes immediately."""
    return math.isfinite(offset) and offset > 0


def _derive_trigger(side: str, market_px: float, offset: float) -> float:
    """Resting-side stop trigger from the last trade per the rule above:
    SELL derives below, BUY derives above. Raises ValueError for any offset
    that could derive a marketable stop (the invariant lives here, so every
    caller is safe — argparse gating alone would not be)."""
    if side not in ("SELL", "BUY"):
        raise ValueError(f"unsupported side: {side!r}")
    if not _valid_offset(offset):
        raise ValueError(
            f"offset must be finite and > 0 (got {offset!r}); "
            f"{_RESTING_SELL_RULE} / {_RESTING_BUY_RULE}"
        )
    return market_px - offset if side == "SELL" else market_px + offset


def _our_baskets(events: list[dict], tag: str) -> set[str]:
    """Basket ids attributed to our smoke order by IDENTITY only.

    Primary evidence: rows carrying our ``user_tag``. Later rows for the
    same basket may omit the tag, so they inherit attribution from their
    basket id. Price equality is deliberately NOT used — it assumes a LIMIT
    price exists and can misattribute orders we do not own.
    """
    tagged = {
        e["basket_id"]
        for e in events
        if e.get("basket_id") and e.get("user_tag") == tag
    }
    if not tagged:
        return set()
    return {e["basket_id"] for e in events if e.get("basket_id") in tagged}


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


def _poll_market_px(
    session: Any, symbol: str, exchange: str, window: float
) -> float | None:
    """Subscribe the ticker and return the first last-trade price (or None)."""
    session.subscribe(symbol, exchange)
    deadline = time.monotonic() + max(0.0, window)
    while time.monotonic() < deadline:
        ev = session.poll_event()
        if ev is not None and ev.get("type") == "last_trade":
            px = ev.get("trade_price")
            if px is not None:
                return float(px)
        time.sleep(0.05)
    return None


def _build_parser() -> argparse.ArgumentParser:
    """Arg definitions; stop-rule help strings compose the module constants
    so the documented rule can never drift from _derive_trigger."""
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
        help="DANGEROUS: place a 1-lot DAY order (needs --price/trigger + gate)",
    )
    parser.add_argument(
        "--no-live-place",
        action="store_true",
        help="force dry-run even if RITHMIC_ENABLE_TRADING is set",
    )
    parser.add_argument(
        "--root", default="NQ", help="root symbol for front-month resolve"
    )
    parser.add_argument("--exchange", default="CME")
    parser.add_argument(
        "--price",
        type=float,
        default=None,
        help="required with --live-place: BUY far below market, SELL far above",
    )
    parser.add_argument("--side", default="BUY", choices=["BUY", "SELL"])
    parser.add_argument(
        "--order-type",
        default="LIMIT",
        choices=["LIMIT", "STOP_MARKET"],
        help="order type to place when --live-place is set",
    )
    parser.add_argument(
        "--trigger-price",
        type=float,
        default=None,
        help=f"STOP_MARKET trigger ({_RESTING_SELL_RULE} / {_RESTING_BUY_RULE})",
    )
    parser.add_argument(
        "--auto-trigger-offset",
        type=float,
        default=None,
        help="derive the STOP_MARKET trigger from the polled last trade "
        f"({_RESTING_SELL_RULE} / {_RESTING_BUY_RULE}); finite and > 0",
    )
    parser.add_argument(
        "--market-window",
        type=float,
        default=6.0,
        help="seconds to poll the ticker for a last trade (auto trigger)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    # Safety gates evaluate FIRST, before any dotenv/credential loading or
    # session construction: a mis-gated live place must fail fast and must
    # not depend on a repo .env existing at all.
    from rithmic_nt_connect import env_truthy

    env_trading = env_truthy(os.environ.get("RITHMIC_ENABLE_TRADING"))
    allow_live = bool(args.live_place) and env_trading and not args.no_live_place
    refusal: str | None = None
    if args.live_place and not allow_live:
        refusal = (
            "REFUSING --live-place: set RITHMIC_ENABLE_TRADING=1 and omit "
            "--no-live-place"
        )
    elif allow_live and args.order_type == "LIMIT" and args.price is None:
        refusal = (
            "REFUSING --live-place: pass an explicit --price "
            "(BUY far below market, SELL far above; no default)"
        )
    elif allow_live and args.order_type == "STOP_MARKET":
        offset = args.auto_trigger_offset
        if args.trigger_price is None and offset is None:
            refusal = (
                "REFUSING --live-place STOP_MARKET: pass --trigger-price "
                f"({_RESTING_SELL_RULE} / {_RESTING_BUY_RULE}) "
                "or --auto-trigger-offset"
            )
        elif offset is not None and not _valid_offset(offset):
            refusal = (
                "REFUSING --live-place STOP_MARKET: --auto-trigger-offset "
                "must be finite and > 0 (a non-positive offset derives a "
                "marketable stop that executes immediately)"
            )
    if refusal is not None:
        print(refusal, file=sys.stderr)
        return 3

    from rithmic_nt_connect import load_dotenv_files
    from rithmic_nt_connect.config import SessionConfig
    from rithmic_nt_connect.front_month import resolve_front_month
    from rithmic_nt_connect.session import create_session

    load_dotenv_files(ROOT / ".env")

    session_cfg = SessionConfig.from_env()

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
            f"order plant subscribed; front={front['trading_symbol']}."
            f"{front['trading_exchange']}; "
            f"mode={report['mode']}; account={report.get('resolved_account')}"
        )

        if allow_live:
            trigger_price: float | None = None
            if args.order_type == "STOP_MARKET":
                trigger_price = args.trigger_price
                if trigger_price is None:
                    market_px = _poll_market_px(
                        session,
                        front["trading_symbol"],
                        front["trading_exchange"],
                        args.market_window,
                    )
                    if market_px is None:
                        print(
                            "no last trade observed in the market window; "
                            "refusing to guess a stop trigger",
                            file=sys.stderr,
                        )
                        return 4
                    offset = float(args.auto_trigger_offset or 0.0)
                    trigger_price = _derive_trigger(args.side, market_px, offset)
                print(
                    f"WARNING: placing live {args.side} STOP_MARKET "
                    f"trigger @ {trigger_price} tag={smoke_tag}",
                    file=sys.stderr,
                )
            else:
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
                args.order_type,
                1,
                smoke_tag,
                price=args.price if args.order_type == "LIMIT" else None,
                trigger_price=trigger_price,
                duration="DAY",
            )
            report["placed"] = True
            report["place"] = {
                "side": args.side,
                "price_type": args.order_type,
                "price": args.price,
                "trigger_price": trigger_price,
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
            # Cancel only the smoke order we placed (never cancel_all /
            # other baskets): identity attribution via user_tag + basket
            # inheritance, never price equality.
            smoke_baskets = sorted(_our_baskets(report["events"], smoke_tag))
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
                    # A placed order we cannot attribute must fail the run —
                    # it is still live at the venue and needs manual cleanup.
                    print(
                        "FAIL: no basket attributed to smoke tag "
                        f"{smoke_tag}; the order may still be live — "
                        "cancel it manually in R|Trader / the venue UI",
                        file=sys.stderr,
                    )
                    return 1
            except Exception as cancel_exc:
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
            print(
                f"DRY-RUN OK event_count={report['event_count']} "
                f"placed={report['placed']}"
            )
        if args.out is not None:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(report, indent=2) + "\n")
            print(f"wrote {args.out}")
        return 0
    except Exception as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    finally:
        with contextlib.suppress(Exception):
            session.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
