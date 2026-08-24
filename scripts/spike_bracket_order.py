#!/usr/bin/env python3
"""Plant-bracket spike harness — no place unless --place.

  uv run python scripts/spike_bracket_order.py
  uv run python scripts/spike_bracket_order.py --place --stop-ticks 40 --qty 1

--place requires RITHMIC_BRACKETS=1 and RITHMIC_ENABLE_TRADING=1.
RITHMIC_CONNECT_MODE is required by SessionConfig.from_env (direct|gateway).

P2 proof flow (gap-closure plan):

1. ACCEPT  — far LIMIT entry (``--limit-price``, never marketable) or the
   legacy market entry; parent + bracket legs must come back acknowledged.
2. SURVIVE — drop the order plant, re-subscribe both intents (order updates
   + bracket updates), then require bracket notifications for our basket to
   RESUME and the legs to still be working in a ``load_orders`` drain.
3. CLEANUP — cancel by basket id (identity, never cancel_all) and verify
   the drain stops reporting the basket working.

Exit codes: 0 ok · 1 place rejected · 2 gate refusal · 3 inconclusive ·
4 survival failed · 5 cleanup incomplete.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

_RC_OK = 0
_RC_REJECTED = 1
_RC_REFUSED = 2
_RC_INCONCLUSIVE = 3
_RC_SURVIVAL = 4
_RC_CLEANUP = 5

_TERMINAL_STATUSES = {"complete", "canceled", "cancelled", "expired", "filled"}
_REJECT_TOKENS = ("reject", "denied", "fail", "error")


def _poll_for(
    session,
    *,
    seconds: float,
    want_basket: str | None,
    localid: str,
):
    """Return ``(last_matching_event, basket_id, saw_rejection)``."""
    deadline = time.monotonic() + max(0.0, seconds)
    basket: str | None = want_basket
    last = None
    saw_rejection = False
    while time.monotonic() < deadline:
        ev = session.poll_order_event()
        if ev is None:
            time.sleep(0.05)
            continue
        tag = str(ev.get("user_tag") or ev.get("localid") or "")
        ev_basket = ev.get("basket_id") or None
        ours = tag == localid or (basket is not None and ev_basket == basket)
        if not ours:
            continue
        last = ev
        if _event_is_rejection(ev):
            saw_rejection = True
        if ev_basket and basket is None:
            basket = str(ev_basket)
            print(f"basket identified: {basket}")
        status = str(ev.get("status") or "")
        text = str(ev.get("text") or ev.get("report_text") or "")
        print(
            f"order_event: status={status!r} basket={ev_basket} "
            f"tag={tag!r} text={text!r}"
        )
    return last, basket, saw_rejection


def _event_is_rejection(ev: dict) -> bool:
    low = f"{ev.get('status') or ''} {ev.get('text') or ev.get('report_text') or ''}"
    return any(tok in low.lower() for tok in _REJECT_TOKENS)


def _event_is_bracket_path(ev: dict) -> bool:
    """MODIFY_* / bracket adjust ack — plain OPEN rows must not satisfy survival."""
    notify = str(ev.get("notify_type_name") or "").upper()
    status = str(ev.get("status") or "").lower()
    text = str(ev.get("text") or ev.get("report_text") or "").lower()
    if "MODIFY" in notify or "BRACKET" in notify:
        return True
    blob = f"{status} {text}"
    return "modif" in blob or "bracket" in blob


def _wait_bbo(
    session, symbol: str, exchange: str, *, seconds: float = 8.0
) -> tuple[float, float] | None:
    """Subscribe ticker and wait for a two-sided BBO; None if timed out."""
    session.subscribe(symbol, exchange)
    deadline = time.monotonic() + max(0.0, seconds)
    bid = ask = None
    try:
        while time.monotonic() < deadline:
            ev = session.poll_event()
            if ev is None:
                time.sleep(0.05)
                continue
            if ev.get("type") != "bbo":
                continue
            if ev.get("bid_price") is not None:
                bid = float(ev["bid_price"])
            if ev.get("ask_price") is not None:
                ask = float(ev["ask_price"])
            if bid is not None and ask is not None:
                return bid, ask
    finally:
        with contextlib.suppress(Exception):
            session.unsubscribe(symbol, exchange)
    return None


def _limit_is_far(side: str, limit_price: float, bid: float, ask: float) -> bool:
    """BUY must sit below ask; SELL above bid — never marketable."""
    if side == "Buy":
        return limit_price < ask
    return limit_price > bid


def _latest_basket_row(session, basket: str):
    end = int(time.time())
    rows = session.load_orders(end - 3600, end) or ()
    latest = None
    latest_key = None
    for idx, row in enumerate(rows):
        if str(row.get("basket_id") or "") != basket:
            continue
        key = row.get("ts_event_ns") or row.get("ssboe") or idx
        if latest_key is None or key >= latest_key:
            latest, latest_key = row, key
    return latest


def _latest_basket_drain_status(session, basket: str) -> str | None:
    latest = _latest_basket_row(session, basket)
    if latest is None:
        return None
    status = str(latest.get("status") or "").strip().lower()
    text = str(latest.get("text") or "").strip()
    print(f"drain: basket {basket} latest row status={status!r} text={text!r}")
    return status


def _drain_basket_working(session, basket: str) -> bool:
    status = _latest_basket_drain_status(session, basket)
    if status is None:
        print(f"drain: basket {basket} not present (no rows)")
        return False
    return status not in _TERMINAL_STATUSES


def _drain_basket_terminal(session, basket: str) -> bool:
    status = _latest_basket_drain_status(session, basket)
    if status is None:
        print(
            f"drain: basket {basket} not present (no rows) — "
            "not proof of terminal cleanup"
        )
        return False
    return status in _TERMINAL_STATUSES


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--place", action="store_true")
    p.add_argument("--root", default="NQ", help="root symbol for front-month resolve")
    p.add_argument("--exchange", default="CME")
    p.add_argument("--qty", type=int, default=1)
    p.add_argument("--stop-ticks", type=int, default=40)
    p.add_argument("--target-ticks", type=int, default=None)
    p.add_argument("--side", default="Buy", choices=("Buy", "Sell"))
    p.add_argument(
        "--limit-price",
        type=float,
        default=None,
        help="far LIMIT entry price (BUY below / SELL above market; resting, "
        "never marketable). Omit for the legacy MARKET entry.",
    )
    p.add_argument("--seconds", type=float, default=8.0, help="poll window per phase")
    args = p.parse_args(argv)

    from rithmic_nt_connect import env_truthy, load_dotenv_files
    from rithmic_nt_connect.config import SessionConfig
    from rithmic_nt_connect.front_month import resolve_front_month
    from rithmic_nt_connect.session import create_session

    load_dotenv_files(ROOT / ".env")

    if not args.place:
        print(
            "DRY: subscribe_bracket_updates / place_bracket_order / "
            "adjust_bracket_stop / adjust_bracket_target (+ survive + cleanup)"
        )
        return _RC_OK

    if not env_truthy(os.environ.get("RITHMIC_BRACKETS")) or not env_truthy(
        os.environ.get("RITHMIC_ENABLE_TRADING")
    ):
        print(
            "REFUSE --place: need RITHMIC_BRACKETS=1 and RITHMIC_ENABLE_TRADING=1",
            file=sys.stderr,
        )
        return _RC_REFUSED

    cfg = SessionConfig.from_env()
    session = create_session(cfg)
    session.connect()
    smoke_localid = f"spike-bracket-{uuid.uuid4().hex[:8]}"
    basket_id: str | None = None
    try:
        front = resolve_front_month(session, args.root, args.exchange)
        session.subscribe_order_updates()
        session.subscribe_bracket_updates()
        entry_desc = "MARKET"
        kwargs = {}
        if args.limit_price is not None:
            limit_px = float(args.limit_price)
            bbo = _wait_bbo(
                session,
                front["trading_symbol"],
                front["trading_exchange"],
                seconds=max(8.0, args.seconds),
            )
            if bbo is None:
                print(
                    "INCONCLUSIVE: no BBO to validate --limit-price is far",
                    file=sys.stderr,
                )
                return _RC_INCONCLUSIVE
            bid, ask = bbo
            if not _limit_is_far(args.side, limit_px, bid, ask):
                print(
                    f"REFUSE --limit-price {limit_px}: marketable vs "
                    f"bid={bid} ask={ask} (BUY must be < ask, SELL > bid)",
                    file=sys.stderr,
                )
                return _RC_REFUSED
            print(f"far-limit ok: {args.side} {limit_px} vs bid={bid} ask={ask}")
            entry_desc = f"LIMIT {limit_px}"
            kwargs["price"] = limit_px
            price_type = "Limit"
        else:
            price_type = "Market"
        session.place_bracket_order(
            symbol=front["trading_symbol"],
            exchange=front["trading_exchange"],
            side=args.side,
            price_type=price_type,
            quantity=int(args.qty),
            localid=smoke_localid,
            duration="DAY",
            stop_ticks=int(args.stop_ticks),
            target_ticks=None if args.target_ticks is None else int(args.target_ticks),
            **kwargs,
        )
        print(
            f"PLACE sent front={front['trading_symbol']}.{front['trading_exchange']} "
            f"entry={entry_desc} localid={smoke_localid}; polling {args.seconds}s…"
        )
        _, basket_id, place_rejected = _poll_for(
            session, seconds=args.seconds, want_basket=None, localid=smoke_localid
        )
        if place_rejected:
            print("PLACE rejected by venue/plant", file=sys.stderr)
            return _RC_REJECTED

        if not basket_id:
            print(
                "INCONCLUSIVE: no basket identified; skipping survival/cleanup",
                file=sys.stderr,
            )
            return _RC_INCONCLUSIVE

        print("SURVIVAL: dropping order plant and re-subscribing both intents…")
        session.disconnect_order_plant()
        session.subscribe_order_updates()
        session.subscribe_bracket_updates()
        survived_event = None
        nudge_ticks = int(args.stop_ticks) + 1
        try:
            session.adjust_bracket_stop(basket_id, nudge_ticks, 0)
        except Exception as exc:
            print(
                f"SURVIVAL FAILED: adjust_bracket_stop rejected: {exc}",
                file=sys.stderr,
            )
        else:
            print(
                f"SURVIVAL: adjust_bracket_stop ticks={nudge_ticks} level=0 "
                "(post-redial bracket nudge)"
            )
            survived_event, _, _ = _poll_for(
                session,
                seconds=args.seconds,
                want_basket=basket_id,
                localid=smoke_localid,
            )
        still_working = _drain_basket_working(session, basket_id)
        survival_ok = (
            survived_event is not None
            and _event_is_bracket_path(survived_event)
            and still_working
        )
        if not survival_ok:
            missing = []
            if survived_event is None:
                missing.append("no notification after redial")
            elif not _event_is_bracket_path(survived_event):
                missing.append("post-redial event was not a bracket/modify path ack")
            if not still_working:
                missing.append("legs not working in drain")
            print(
                f"SURVIVAL FAILED: {' and '.join(missing) or 'unknown'}",
                file=sys.stderr,
            )
        else:
            print("SURVIVAL OK: bracket-path notifications resumed; legs working")

        try:
            session.cancel_order(basket_id)
        except Exception as exc:
            print(
                f"CLEANUP INCOMPLETE: cancel_order failed: {exc} — "
                "close the basket manually at the venue",
                file=sys.stderr,
            )
            return _RC_CLEANUP
        print(f"cancel_order sent basket_id={basket_id}")
        _poll_for(
            session,
            seconds=args.seconds,
            want_basket=basket_id,
            localid=smoke_localid,
        )
        if not _drain_basket_terminal(session, basket_id):
            print(
                f"CLEANUP INCOMPLETE: basket {basket_id} has no explicit "
                "terminal drain row after identity cancel — close it out "
                "manually (empty drain is not proof)",
                file=sys.stderr,
            )
            return _RC_CLEANUP
        print("CLEANUP OK: basket terminal at venue")
        return _RC_OK if survival_ok else _RC_SURVIVAL
    finally:
        try:
            session.disconnect()
        except Exception as exc:
            print(f"disconnect warning: {exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
