"""Venue inventory without a node: working orders + net positions per contract.

For out-of-process tools (emergency flatten, plant status) that dial their own session.
A node reads the same facts from its exec client (``exposure.ExposureSnapshot``).

Positions come only from PnL-plant ``instrument_pnl`` rows. ``pnl_live`` is False until
any PnL row arrived: without it positions are unknown, never flat.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Protocol

from rithmic_nt_connect._orders import kind_from_notify

_TERMINAL_KINDS = frozenset({"canceled", "rejected"})
_DEAD_STATUS = frozenset({"CANCELLED", "CANCELED", "COMPLETE", "FILLED", "REJECTED"})


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
    value = _as_int(raw)
    if value == 1:
        return "BUY"
    if value == 2:
        return "SELL"
    text = str(raw or "").strip().upper()
    if text in {"BUY", "B", "1"}:
        return "BUY"
    if text in {"SELL", "S", "2"}:
        return "SELL"
    return None


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
            "flatten_side": "SELL" if self.net_qty > 0 else "BUY",
            "flatten_qty": abs(self.net_qty),
        }


class Inventory:
    """Reduce order + PnL events into working baskets and net positions."""

    def __init__(self) -> None:
        self._orders: dict[str, WorkingOrder] = {}
        self._positions: dict[str, OpenPosition] = {}
        self.pnl_events = 0

    @property
    def pnl_live(self) -> bool:
        """True once any PnL row arrived; before that positions are unknown."""
        return self.pnl_events > 0

    def ingest(self, event: dict[str, Any]) -> None:
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
        if etype == "instrument_pnl":
            self._ingest_position(event)

    def _ingest_order(self, event: dict[str, Any]) -> None:
        basket = str(event.get("basket_id") or "").strip()
        if not basket:
            return
        kind = event.get("kind")
        if kind is None and event.get("notify_type_name") is not None:
            kind = kind_from_notify(
                str(event.get("source") or ""),
                str(event.get("notify_type_name")),
                event.get("status"),
            )
        status = str(event.get("status") or "").upper() or None
        unfilled = _as_int(event.get("total_unfilled_size"))
        if kind in _TERMINAL_KINDS or (
            status is not None and status in _DEAD_STATUS and kind != "filled"
        ):
            self._orders.pop(basket, None)
            return
        if kind == "filled" and (unfilled is None or unfilled <= 0):
            self._orders.pop(basket, None)
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
        net = _as_int(event.get("net_quantity"))
        if net is None:
            return
        exchange = str(event.get("exchange") or "CME")
        key = f"{symbol}.{exchange}"
        if net == 0:
            self._positions.pop(key, None)
            return
        self._positions[key] = OpenPosition(
            symbol=symbol, exchange=exchange, net_qty=net
        )

    def adopt_positions(self, other: Inventory) -> None:
        """Start from ``other``'s positions; later PnL rows overwrite them."""
        self._positions = dict(other._positions)

    def working(self) -> list[WorkingOrder]:
        return [self._orders[k] for k in sorted(self._orders)]

    def positions(self) -> list[OpenPosition]:
        return [self._positions[k] for k in sorted(self._positions)]


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
