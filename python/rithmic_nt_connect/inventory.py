"""Venue inventory without a node: working orders + net positions per contract.

For out-of-process tools (emergency flatten, plant status) that dial their own session.
A node reads the same facts from its exec client (``exposure.ExposureSnapshot``).

Positions come only from PnL-plant ``instrument_pnl`` rows. ``pnl_live`` is False until
the PnL stream has been observed for ``PNL_SETTLE_NS`` (account rows land before the
instrument burst): without it positions are unknown, never flat. Order state uses the
exec client's own status mapping (``order_status_from_fields`` +
``TERMINAL_ORDER_STATUSES``), so the two can never disagree on what is terminal.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Protocol, cast

from nautilus_trader.model.enums import OrderStatus

from rithmic_nt_connect import _lib
from rithmic_nt_connect._orders import order_notification_to_fields
from rithmic_nt_connect.exposure import PNL_SETTLE_NS
from rithmic_nt_connect.order_rows import order_status_from_fields
from rithmic_nt_connect.recon import TERMINAL_ORDER_STATUSES

PNL_SETTLE_S = PNL_SETTLE_NS / 1e9


class InventorySession(Protocol):
    def subscribe_order_updates(self) -> None: ...
    def subscribe_pnl(self) -> None: ...
    def load_orders(self, start_ssboe: int, end_ssboe: int) -> list[dict[str, Any]]: ...
    def poll_order_event(self) -> dict[str, Any] | None: ...
    def poll_pnl_event(self) -> dict[str, Any] | None: ...


def _as_int(raw: object) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        return int(str(raw))
    except (TypeError, ValueError):
        try:
            return int(float(str(raw)))
        except (TypeError, ValueError):
            return None


def _as_float(raw: object) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        return float(str(raw))
    except (TypeError, ValueError):
        return None


def _side_from_txn(raw: object) -> str | None:
    return _lib.side_from_txn(_as_int(raw), str(raw or ""))


@dataclass
class WorkingOrder:
    basket_id: str
    symbol: str
    exchange: str = "CME"
    side: str | None = None
    qty: int | None = None
    unfilled: int | None = None
    status: str | None = None
    kind: str | None = None
    user_tag: str | None = None
    price: float | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "basket_id": self.basket_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "side": self.side,
            "qty": self.qty,
            "unfilled": self.unfilled,
            "status": self.status,
            "kind": self.kind,
            "user_tag": self.user_tag,
            "price": self.price,
        }


@dataclass
class OpenPosition:
    symbol: str
    exchange: str = "CME"
    net_qty: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "net_qty": self.net_qty,
            "flatten_side": _lib.flatten_side(self.net_qty),
            "flatten_qty": abs(self.net_qty),
        }


class Inventory:
    """Reduce order + PnL events into working baskets and net positions."""

    def __init__(self) -> None:
        self._orders: dict[str, WorkingOrder] = {}
        self._positions: dict[str, OpenPosition] = {}
        self.pnl_events = 0
        self._pnl_first_s: float | None = None
        self._observed_until_s: float | None = None
        # Baskets the venue already finished: Rithmic never reuses a basket id, so a
        # late row (e.g. a bare COMPLETE) must not resurrect a phantom working order.
        self._done_baskets: set[str] = set()
        # Contracts whose last PnL row carried no readable net: exposure unknown.
        self._unknown_positions: set[str] = set()

    @property
    def pnl_live(self) -> bool:
        """True once the PnL stream was observed for ``PNL_SETTLE_S`` and every
        position row read cleanly.

        The first row is often an account row that lands before the instrument burst;
        positions read before the settle window may still be missing (never flat).
        """
        # A position row we could not read is not flat (rule in ``rithmic-exec``).
        return _lib.pnl_live(
            self._pnl_first_s,
            self._observed_until_s,
            bool(self._unknown_positions),
            PNL_SETTLE_S,
        )

    def mark_observed(self, now_s: float) -> None:
        """The stream was watched until ``now_s`` (monotonic seconds)."""
        self._observed_until_s = now_s

    def ingest(self, event: dict[str, Any], now_s: float | None = None) -> None:
        now = time.monotonic() if now_s is None else now_s
        etype = str(event.get("type") or "")
        basket = event.get("basket_id")
        from_order_stream = etype == "order_notification" and event.get("source") in {
            "rithmic",
            "exchange",
        }
        if etype in {"", "order_notification"} and (basket or from_order_stream):
            self._ingest_order(event)
            return
        if etype in {"account_pnl", "instrument_pnl"}:
            self.pnl_events += 1
            if self._pnl_first_s is None:
                self._pnl_first_s = now
            self.mark_observed(max(now, self._observed_until_s or now))
        if etype == "instrument_pnl":
            self._ingest_position(event)

    def _ingest_order(self, event: dict[str, Any]) -> None:
        basket = str(event.get("basket_id") or "").strip()
        if not basket or basket in self._done_baskets:
            return
        try:
            fields = order_notification_to_fields(event)
        except Exception:  # unreadable row: keep the order (fail closed)
            fields = None
        kind = fields.get("kind") if fields is not None else event.get("kind")
        status = str(event.get("status") or "").upper() or None
        unfilled = _as_int(event.get("total_unfilled_size"))
        if fields is not None and _is_gone(fields, unfilled):
            self._orders.pop(basket, None)
            self._done_baskets.add(basket)
            return
        prev = self._orders.get(basket)
        symbol = str(event.get("symbol") or (prev.symbol if prev else "")).strip()
        if not symbol:
            return
        exchange = str(
            event.get("exchange") or (prev.exchange if prev else "CME") or "CME"
        )
        side = _side_from_txn(event.get("transaction_type")) or (
            prev.side if prev else None
        )
        qty = _as_int(event.get("quantity"))
        if qty is None and prev is not None:
            qty = prev.qty
        if unfilled is None and prev is not None:
            unfilled = prev.unfilled
        self._orders[basket] = WorkingOrder(
            basket_id=basket,
            symbol=symbol,
            exchange=exchange,
            side=side,
            qty=qty,
            unfilled=unfilled,
            status=status,
            kind=str(kind) if kind is not None else (prev.kind if prev else None),
            user_tag=(
                str(event["user_tag"])
                if event.get("user_tag") is not None
                else (prev.user_tag if prev else None)
            ),
            price=_as_float(event.get("price"))
            if event.get("price") is not None
            else (prev.price if prev else None),
        )

    def _ingest_position(self, event: dict[str, Any]) -> None:
        symbol = str(event.get("symbol") or "").strip()
        if not symbol:
            return
        exchange = str(event.get("exchange") or "CME")
        key = f"{symbol}.{exchange}"
        change = _lib.position_change(_as_int(event.get("net_quantity")))
        if change == "unknown":
            self._positions.pop(key, None)
            self._unknown_positions.add(key)
            return
        self._unknown_positions.discard(key)
        if change == "flat":
            self._positions.pop(key, None)
            return
        self._positions[key] = OpenPosition(
            symbol=symbol,
            exchange=exchange,
            net_qty=cast(int, change),  # non-zero net
        )

    def working(self) -> list[WorkingOrder]:
        return [self._orders[k] for k in sorted(self._orders)]

    def positions(self) -> list[OpenPosition]:
        return [self._positions[k] for k in sorted(self._positions)]


def _is_gone(fields: dict[str, Any], unfilled: int | None) -> bool:
    """Terminal per the exec client's mapping; a fill only when provably complete.

    ``order_status_from_fields`` maps every ``filled`` row to FILLED, partial or not;
    an order whose unfilled size is unknown stays working (unknown is not absent).
    """
    status = order_status_from_fields(fields)
    if status not in TERMINAL_ORDER_STATUSES:
        status_class = "working"
    elif status is OrderStatus.FILLED:
        status_class = "filled"
    else:
        status_class = "other_terminal"
    return _lib.order_is_gone(
        status_class,
        unfilled,
        _as_int(fields.get("quantity")),
        _as_int(fields.get("total_fill_size")),
    )


def poll_into(session: InventorySession, inventory: Inventory, seconds: float) -> int:
    """Feed order + PnL events into ``inventory`` for ``seconds``; returns count."""
    seen = 0
    deadline = time.monotonic() + max(0.0, seconds)
    while time.monotonic() < deadline:
        progressed = False
        for getter in (session.poll_order_event, session.poll_pnl_event):
            ev = getter()
            if ev is None:
                continue
            progressed = True
            seen += 1
            inventory.ingest(ev)
        if not progressed:
            time.sleep(0.05)
        inventory.mark_observed(time.monotonic())
    return seen


def venue_inventory_snapshot(session: InventorySession, *, seconds: float) -> Inventory:
    """Replay working orders, subscribe order + PnL updates, drain for ``seconds``.

    Raises when either subscription fails; check ``pnl_live`` before trusting positions.
    The caller owns ``connect``/``disconnect``.
    """
    inventory = Inventory()
    session.subscribe_order_updates()
    for event in session.load_orders(0, 0):
        inventory.ingest(event)
    session.subscribe_pnl()
    poll_into(session, inventory, seconds)
    return inventory


__all__ = [
    "Inventory",
    "InventorySession",
    "OpenPosition",
    "WorkingOrder",
    "poll_into",
    "venue_inventory_snapshot",
]
