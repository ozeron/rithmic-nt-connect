#!/usr/bin/env python3
"""Plant-bracket spike harness — no place unless --place.

  uv run python scripts/spike_bracket_order.py
  uv run python scripts/spike_bracket_order.py --place --far-ticks 20 --qty 1

--place requires RITHMIC_BRACKETS=1 and RITHMIC_ENABLE_TRADING=1.
RITHMIC_CONNECT_MODE is required by SessionConfig.from_env (direct|gateway).

Proof policy (fail-closed):
  • Each phase needs *positive* evidence — missing / weak signal is not OK.
  • Illegal states are unrepresentable (``Accepted`` needs a basket id;
    ``FarLimit`` only constructs when far + not marketable; ``Cleaned``
    needs an explicit terminal drain status).
  • Cleanup always runs once a basket id is known (identity cancel, never
    cancel_all), independent of survival.

P2 phases:

1. ACCEPT  — far LIMIT from live sized BBO (BUY = bid - N ticks), or
   explicit ``--market-entry``. Optional ``--limit-price`` must still clear
   the far rule.
2. SURVIVE — plant redial + ``adjust_bracket_stop``; require bracket-path
   ack *and* working drain row.
3. CLEANUP — cancel by basket id; require explicit terminal drain row.

Exit codes: 0 ok · 1 place rejected · 2 gate refusal · 3 inconclusive ·
4 survival failed · 5 cleanup incomplete.
"""

from __future__ import annotations

import argparse
import contextlib
import math
import os
import sys
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

# Exact status tokens only — ``cancel_rejected`` / ``modify_rejected`` stay
# non-terminal (order still live). Venue ``text`` is never used for this.
_TERMINAL_STATUSES = frozenset(
    {
        "complete",
        "canceled",
        "cancelled",
        "expired",
        "filled",
        "rejected",
    }
)
_WORKING_STATUSES = frozenset(
    {
        "open",
        "working",
        "accepted",
        "submitted",
        "partial",
        "partially filled",
        "triggered",
        "new",
    }
)
_REJECT_TOKENS = ("reject", "denied", "fail", "error")
_KNOWN_TICK_SIZE: dict[str, float] = {
    "NQ": 0.25,
    "MNQ": 0.25,
    "ES": 0.25,
    "MES": 0.25,
}
_DEFAULT_FAR_TICKS = 20


class Outcome(Enum):
    """Closed-set phase / script result — never a bare bool."""

    OK = 0
    REJECTED = 1
    REFUSED = 2
    INCONCLUSIVE = 3
    SURVIVAL = 4
    CLEANUP = 5


class ProofError(Exception):
    """Abort the current phase with a typed outcome (not a silent skip)."""

    def __init__(self, outcome: Outcome, message: str) -> None:
        super().__init__(message)
        self.outcome = outcome
        self.message = message


# --- classifiers (closed fields; venue text is diagnostic only) -------------


def is_rejection(ev: dict) -> bool:
    low = f"{ev.get('status') or ''} {ev.get('text') or ev.get('report_text') or ''}"
    return any(tok in low.lower() for tok in _REJECT_TOKENS)


def is_bracket_path(ev: dict) -> bool:
    """MODIFY_* / bracket adjust ack — plain OPEN must not satisfy survival."""
    notify = str(ev.get("notify_type_name") or "").upper()
    if "MODIFY" in notify or "BRACKET" in notify:
        return True
    blob = f"{ev.get('status') or ''} {ev.get('text') or ev.get('report_text') or ''}"
    low = blob.lower()
    return "modif" in low or "bracket" in low


def is_terminal_status(status: str | None) -> bool:
    return status is not None and status.strip().lower() in _TERMINAL_STATUSES


def is_working_status(status: str | None) -> bool:
    return status is not None and status.strip().lower() in _WORKING_STATUSES


def finite_positive(value: float) -> bool:
    return math.isfinite(value) and value > 0


def size_ok(size: object) -> bool:
    if isinstance(size, bool):
        return False
    if isinstance(size, (int, float)):
        return int(size) >= 1
    if isinstance(size, str):
        try:
            return int(size) >= 1
        except ValueError:
            return False
    return False


def resolve_tick_size(
    root: str, *, tick_size: float | None, front_raw: dict | None
) -> float | None:
    if tick_size is not None:
        try:
            t = float(tick_size)
        except (TypeError, ValueError):
            return None
        if finite_positive(t):
            return t
        return None
    raw_tick = (front_raw or {}).get("tick_size")
    if raw_tick is not None:
        try:
            t = float(raw_tick)
            if finite_positive(t):
                return t
        except (TypeError, ValueError):
            pass
    known = _KNOWN_TICK_SIZE.get(root.upper())
    return known if known is not None and finite_positive(known) else None


# --- constructed evidence ---------------------------------------------------


@dataclass(frozen=True)
class SizedBbo:
    """Two-sided quote with size ≥ 1 on both sides."""

    bid: float
    ask: float

    @classmethod
    def from_event(cls, ev: dict) -> SizedBbo | None:
        if ev.get("type") != "bbo":
            return None
        if not (
            ev.get("bid_price") is not None
            and size_ok(ev.get("bid_size"))
            and ev.get("ask_price") is not None
            and size_ok(ev.get("ask_size"))
        ):
            return None
        bid, ask = float(ev["bid_price"]), float(ev["ask_price"])
        if not (math.isfinite(bid) and math.isfinite(ask)):
            return None
        if bid <= 0 or ask <= 0 or bid > ask:
            return None
        return cls(bid=bid, ask=ask)


@dataclass(frozen=True)
class FarLimit:
    """LIMIT that is ≥ N ticks outside the near side and not marketable."""

    side: str
    price: float
    bid: float
    ask: float
    tick: float
    far_ticks: int
    source: str

    @staticmethod
    def _derive_price(
        side: str, bid: float, ask: float, tick: float, far_ticks: int
    ) -> float:
        t = Decimal(str(tick))
        n = Decimal(int(far_ticks))
        if side == "Buy":
            return float(Decimal(str(bid)) - t * n)
        return float(Decimal(str(ask)) + t * n)

    @staticmethod
    def _far_enough(
        side: str,
        price: float,
        bid: float,
        ask: float,
        tick: float,
        far_ticks: int,
    ) -> bool:
        t = Decimal(str(tick))
        n = Decimal(int(far_ticks))
        limit = Decimal(str(price))
        if side == "Buy":
            return limit <= Decimal(str(bid)) - t * n
        return limit >= Decimal(str(ask)) + t * n

    @staticmethod
    def _not_marketable(side: str, price: float, bid: float, ask: float) -> bool:
        return price < ask if side == "Buy" else price > bid

    @classmethod
    def _checked(
        cls,
        *,
        side: str,
        price: float,
        bbo: SizedBbo,
        tick: float,
        far_ticks: int,
        source: str,
    ) -> FarLimit:
        if not finite_positive(tick):
            raise ProofError(Outcome.REFUSED, f"non-finite or non-positive tick={tick}")
        if not math.isfinite(price):
            raise ProofError(Outcome.REFUSED, f"non-finite --limit-price {price}")
        if not (
            math.isfinite(bbo.bid)
            and math.isfinite(bbo.ask)
            and bbo.bid > 0
            and bbo.ask > 0
        ):
            raise ProofError(
                Outcome.REFUSED,
                f"non-finite or non-positive BBO bid={bbo.bid} ask={bbo.ask}",
            )
        if not cls._far_enough(side, price, bbo.bid, bbo.ask, tick, far_ticks):
            raise ProofError(
                Outcome.REFUSED,
                f"--limit-price {price}: not >= {far_ticks} ticks outside "
                f"bid={bbo.bid} ask={bbo.ask} tick={tick}",
            )
        if not cls._not_marketable(side, price, bbo.bid, bbo.ask):
            raise ProofError(
                Outcome.REFUSED,
                f"--limit-price {price}: marketable vs bid={bbo.bid} ask={bbo.ask}",
            )
        return cls(
            side=side,
            price=price,
            bid=bbo.bid,
            ask=bbo.ask,
            tick=tick,
            far_ticks=far_ticks,
            source=source,
        )

    @classmethod
    def derive(cls, side: str, bbo: SizedBbo, tick: float, far_ticks: int) -> FarLimit:
        price = cls._derive_price(side, bbo.bid, bbo.ask, tick, far_ticks)
        return cls._checked(
            side=side,
            price=price,
            bbo=bbo,
            tick=tick,
            far_ticks=far_ticks,
            source="derived",
        )

    @classmethod
    def override(
        cls,
        side: str,
        price: float,
        bbo: SizedBbo,
        tick: float,
        far_ticks: int,
    ) -> FarLimit:
        return cls._checked(
            side=side,
            price=float(price),
            bbo=bbo,
            tick=tick,
            far_ticks=far_ticks,
            source="override",
        )


@dataclass(frozen=True)
class Entry:
    price_type: str  # "Limit" | "Market"
    price: float | None
    desc: str

    @classmethod
    def market(cls) -> Entry:
        return cls(price_type="Market", price=None, desc="MARKET")

    @classmethod
    def from_far_limit(cls, far: FarLimit) -> Entry:
        return cls(
            price_type="Limit",
            price=far.price,
            desc=f"LIMIT {far.price}",
        )

    def place_kwargs(self) -> dict[str, float]:
        return {} if self.price is None else {"price": self.price}


@dataclass(frozen=True)
class Accepted:
    """Place phase succeeded only with a venue basket id."""

    basket_id: str
    localid: str


@dataclass(frozen=True)
class Survived:
    """Survival needs bracket-path ack *and* a non-terminal drain row."""

    ack: dict
    drain_status: str


@dataclass(frozen=True)
class Cleaned:
    """Cleanup needs an explicit terminal drain status (empty ≠ clean)."""

    drain_status: str


@dataclass(frozen=True)
class PollSnapshot:
    last: dict | None
    basket_id: str | None
    rejected: bool


@dataclass(frozen=True)
class DrainRow:
    status: str
    text: str


# --- proof I/O façade -------------------------------------------------------


class ProofIO:
    """Session ops with sized-BBO / ours-poll / latest-drain rules in one place."""

    def __init__(self, session: Any, *, seconds: float, localid: str) -> None:
        self._session = session
        self.seconds = max(0.0, seconds)
        self.localid = localid

    def wait_sized_bbo(
        self, symbol: str, exchange: str, *, seconds: float | None = None
    ) -> SizedBbo | None:
        window = self.seconds if seconds is None else max(0.0, seconds)
        self._session.subscribe(symbol, exchange)
        deadline = time.monotonic() + window
        try:
            while time.monotonic() < deadline:
                ev = self._session.poll_event()
                if ev is None:
                    time.sleep(0.05)
                    continue
                bbo = SizedBbo.from_event(ev)
                if bbo is not None:
                    return bbo
        finally:
            with contextlib.suppress(Exception):
                self._session.unsubscribe(symbol, exchange)
        return None

    def poll_ours(self, *, want_basket: str | None = None) -> PollSnapshot:
        deadline = time.monotonic() + self.seconds
        basket = want_basket
        last = None
        rejected = False
        while time.monotonic() < deadline:
            ev = self._session.poll_order_event()
            if ev is None:
                time.sleep(0.05)
                continue
            tag = str(ev.get("user_tag") or ev.get("localid") or "")
            ev_basket = ev.get("basket_id") or None
            ours = tag == self.localid or (basket is not None and ev_basket == basket)
            if not ours:
                continue
            last = ev
            rejected = rejected or is_rejection(ev)
            if ev_basket and basket is None:
                basket = str(ev_basket)
                print(f"basket identified: {basket}")
            status = str(ev.get("status") or "")
            text = str(ev.get("text") or ev.get("report_text") or "")
            print(
                f"order_event: status={status!r} basket={ev_basket} "
                f"tag={tag!r} text={text!r}"
            )
        return PollSnapshot(last=last, basket_id=basket, rejected=rejected)

    def latest_drain(self, basket: str) -> DrainRow | None:
        end = int(time.time())
        rows = self._session.load_orders(end - 3600, end) or ()
        latest = None
        latest_key = None
        for idx, row in enumerate(rows):
            if str(row.get("basket_id") or "") != basket:
                continue
            key = row.get("ts_event_ns") or row.get("ssboe") or idx
            if latest_key is None or key >= latest_key:
                latest, latest_key = row, key
        if latest is None:
            return None
        status = str(latest.get("status") or "").strip().lower()
        text = str(latest.get("text") or "").strip()
        print(f"drain: basket {basket} latest row status={status!r} text={text!r}")
        return DrainRow(status=status, text=text)

    def require_working(self, basket: str) -> str:
        row = self.latest_drain(basket)
        if row is None:
            print(f"drain: basket {basket} not present (no rows)")
            raise ProofError(
                Outcome.SURVIVAL, f"legs not working in drain (no rows for {basket})"
            )
        if not is_working_status(row.status):
            raise ProofError(
                Outcome.SURVIVAL,
                f"legs not working in drain (status={row.status!r})",
            )
        return row.status

    def require_terminal(self, basket: str) -> Cleaned:
        row = self.latest_drain(basket)
        if row is None:
            raise ProofError(
                Outcome.CLEANUP,
                f"basket {basket} has no drain row after identity cancel — "
                "empty drain is not proof; close it out manually",
            )
        if not is_terminal_status(row.status):
            raise ProofError(
                Outcome.CLEANUP,
                f"basket {basket} not terminal after cancel (status={row.status!r})",
            )
        return Cleaned(drain_status=row.status)


# --- phases -----------------------------------------------------------------


def build_entry(
    *,
    io: ProofIO,
    side: str,
    root: str,
    front: dict,
    far_ticks: int,
    tick_size: float | None,
    limit_price: float | None,
    market_entry: bool,
) -> Entry:
    if market_entry:
        return Entry.market()
    front_raw = front.get("raw")
    tick = resolve_tick_size(
        root,
        tick_size=tick_size,
        front_raw=front_raw if isinstance(front_raw, dict) else None,
    )
    if tick is None:
        raise ProofError(
            Outcome.REFUSED,
            f"unknown tick for root={root!r}; pass --tick-size",
        )
    bbo = io.wait_sized_bbo(
        front["trading_symbol"],
        front["trading_exchange"],
        seconds=max(8.0, io.seconds),
    )
    if bbo is None:
        raise ProofError(
            Outcome.INCONCLUSIVE,
            "no usable two-sided BBO (size>=1) for far LIMIT",
        )
    far = (
        FarLimit.override(side, limit_price, bbo, tick, far_ticks)
        if limit_price is not None
        else FarLimit.derive(side, bbo, tick, far_ticks)
    )
    print(
        f"far-limit {far.source}: {far.side} {far.price} "
        f"(bid={far.bid} ask={far.ask} tick={far.tick} "
        f"far_ticks={far.far_ticks})"
    )
    return Entry.from_far_limit(far)


def phase_accept(
    *,
    session: Any,
    io: ProofIO,
    front: dict,
    side: str,
    qty: int,
    stop_ticks: int,
    target_ticks: int | None,
    entry: Entry,
    state: dict[str, str | None],
) -> Accepted:
    session.place_bracket_order(
        symbol=front["trading_symbol"],
        exchange=front["trading_exchange"],
        side=side,
        price_type=entry.price_type,
        quantity=int(qty),
        localid=io.localid,
        duration="DAY",
        stop_ticks=int(stop_ticks),
        target_ticks=None if target_ticks is None else int(target_ticks),
        **entry.place_kwargs(),
    )
    print(
        f"PLACE sent front={front['trading_symbol']}.{front['trading_exchange']} "
        f"entry={entry.desc} localid={io.localid}; polling {io.seconds}s…"
    )
    snap = io.poll_ours()
    if snap.basket_id:
        state["basket_id"] = snap.basket_id
    if snap.rejected:
        raise ProofError(Outcome.REJECTED, "PLACE rejected by venue/plant")
    if not snap.basket_id:
        raise ProofError(
            Outcome.INCONCLUSIVE,
            "no basket identified after place",
        )
    return Accepted(basket_id=snap.basket_id, localid=io.localid)


def phase_survive(
    *,
    session: Any,
    io: ProofIO,
    accepted: Accepted,
    stop_ticks: int,
) -> Survived:
    print("SURVIVAL: dropping order plant and re-subscribing both intents…")
    session.disconnect_order_plant()
    session.subscribe_order_updates()
    session.subscribe_bracket_updates()
    nudge_ticks = int(stop_ticks) + 1
    try:
        session.adjust_bracket_stop(accepted.basket_id, nudge_ticks, 0)
    except Exception as exc:
        raise ProofError(
            Outcome.SURVIVAL, f"adjust_bracket_stop rejected: {exc}"
        ) from exc
    print(
        f"SURVIVAL: adjust_bracket_stop ticks={nudge_ticks} level=0 "
        "(post-redial bracket nudge)"
    )
    snap = io.poll_ours(want_basket=accepted.basket_id)
    if snap.last is None:
        raise ProofError(Outcome.SURVIVAL, "no notification after redial")
    if not is_bracket_path(snap.last):
        raise ProofError(
            Outcome.SURVIVAL,
            "post-redial event was not a bracket/modify path ack",
        )
    drain_status = io.require_working(accepted.basket_id)
    print("SURVIVAL OK: bracket-path notifications resumed; legs working")
    return Survived(ack=snap.last, drain_status=drain_status)


def phase_cleanup(*, session: Any, io: ProofIO, basket_id: str) -> Cleaned:
    try:
        session.cancel_order(basket_id)
    except Exception as exc:
        raise ProofError(
            Outcome.CLEANUP,
            f"cancel_order failed: {exc} — close the basket manually at the venue",
        ) from exc
    print(f"cancel_order sent basket_id={basket_id}")
    io.poll_ours(want_basket=basket_id)
    cleaned = io.require_terminal(basket_id)
    print("CLEANUP OK: basket terminal at venue")
    return cleaned


def run_place(args: argparse.Namespace) -> Outcome:
    from rithmic_nt_connect import env_truthy, load_dotenv_files
    from rithmic_nt_connect.config import SessionConfig
    from rithmic_nt_connect.front_month import resolve_front_month
    from rithmic_nt_connect.session import create_session

    load_dotenv_files(ROOT / ".env")

    if not env_truthy(os.environ.get("RITHMIC_BRACKETS")) or not env_truthy(
        os.environ.get("RITHMIC_ENABLE_TRADING")
    ):
        print(
            "REFUSE --place: need RITHMIC_BRACKETS=1 and RITHMIC_ENABLE_TRADING=1",
            file=sys.stderr,
        )
        return Outcome.REFUSED

    if args.market_entry and args.limit_price is not None:
        print(
            "REFUSE: --market-entry and --limit-price are mutually exclusive",
            file=sys.stderr,
        )
        return Outcome.REFUSED

    if args.far_ticks < 1:
        print("REFUSE: --far-ticks must be >= 1", file=sys.stderr)
        return Outcome.REFUSED

    cfg = SessionConfig.from_env()
    session = create_session(cfg)
    session.connect()
    localid = f"spike-bracket-{uuid.uuid4().hex[:8]}"
    io = ProofIO(session, seconds=args.seconds, localid=localid)
    state: dict[str, str | None] = {"basket_id": None}
    outcome = Outcome.OK

    try:
        front = resolve_front_month(session, args.root, args.exchange)
        session.subscribe_order_updates()
        session.subscribe_bracket_updates()
        try:
            entry = build_entry(
                io=io,
                side=args.side,
                root=args.root,
                front=front,
                far_ticks=int(args.far_ticks),
                tick_size=args.tick_size,
                limit_price=args.limit_price,
                market_entry=bool(args.market_entry),
            )
            accepted = phase_accept(
                session=session,
                io=io,
                front=front,
                side=args.side,
                qty=int(args.qty),
                stop_ticks=int(args.stop_ticks),
                target_ticks=args.target_ticks,
                entry=entry,
                state=state,
            )
            try:
                phase_survive(
                    session=session,
                    io=io,
                    accepted=accepted,
                    stop_ticks=int(args.stop_ticks),
                )
            except ProofError as exc:
                if exc.outcome is Outcome.SURVIVAL:
                    print(f"SURVIVAL FAILED: {exc.message}", file=sys.stderr)
                    outcome = Outcome.SURVIVAL
                else:
                    raise
        except ProofError as exc:
            print(f"{exc.outcome.name}: {exc.message}", file=sys.stderr)
            outcome = exc.outcome
    finally:
        basket = state.get("basket_id")
        if basket is not None:
            try:
                phase_cleanup(session=session, io=io, basket_id=basket)
            except ProofError as exc:
                print(f"CLEANUP INCOMPLETE: {exc.message}", file=sys.stderr)
                outcome = Outcome.CLEANUP
        try:
            session.disconnect()
        except Exception as exc:
            print(f"disconnect warning: {exc}", file=sys.stderr)

    return outcome


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
        "--far-ticks",
        type=int,
        default=_DEFAULT_FAR_TICKS,
        help="ticks outside bid/ask for derived far LIMIT (default %(default)s)",
    )
    p.add_argument(
        "--tick-size",
        type=float,
        default=None,
        help="instrument tick; default from known root map (NQ/MNQ/ES/MES=0.25)",
    )
    p.add_argument(
        "--limit-price",
        type=float,
        default=None,
        help="optional exact LIMIT override; must still be >= --far-ticks outside BBO",
    )
    p.add_argument(
        "--market-entry",
        action="store_true",
        help="explicit MARKET entry (not for P2 resting-bracket evidence)",
    )
    p.add_argument("--seconds", type=float, default=8.0, help="poll window per phase")
    args = p.parse_args(argv)

    if not args.place:
        print(
            "DRY: subscribe_bracket_updates / place_bracket_order / "
            "adjust_bracket_stop / adjust_bracket_target (+ survive + cleanup)"
        )
        return Outcome.OK.value

    return run_place(args).value


# --- test aliases (stable names for unit pins) ------------------------------

_event_is_rejection = is_rejection
_event_is_bracket_path = is_bracket_path
_size_ok = size_ok
_resolve_tick_size = resolve_tick_size
_derive_far_limit = FarLimit._derive_price
_limit_is_far_enough = FarLimit._far_enough
_limit_not_marketable = FarLimit._not_marketable


def _wait_bbo(session, symbol: str, exchange: str, *, seconds: float = 8.0):
    bbo = ProofIO(session, seconds=seconds, localid="").wait_sized_bbo(
        symbol, exchange, seconds=seconds
    )
    return None if bbo is None else (bbo.bid, bbo.ask)


def _drain_basket_working(session, basket: str) -> bool:
    io = ProofIO(session, seconds=0.0, localid="")
    row = io.latest_drain(basket)
    if row is None:
        print(f"drain: basket {basket} not present (no rows)")
        return False
    return is_working_status(row.status)


def _drain_basket_terminal(session, basket: str) -> bool:
    io = ProofIO(session, seconds=0.0, localid="")
    try:
        io.require_terminal(basket)
    except ProofError:
        return False
    return True


if __name__ == "__main__":
    raise SystemExit(main())
