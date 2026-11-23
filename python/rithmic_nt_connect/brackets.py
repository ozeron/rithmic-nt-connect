"""Native bracket translation and cache-backed linkage recovery.

The public book contract is documented in :mod:`rithmic_nt_connect.execution`.
Only venue notifications/report rows change order state. Template 353 carries
bracket levels and release counters, not an OCO cancellation or a fill; it
requests a coalesced working-order refresh rather than inventing either.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from nautilus_trader.model import (
    ClientOrderId,
    OrderSide,
    OrderType,
    TimeInForce,
    TriggerType,
)

from rithmic_nt_connect import _lib

REFERENCE_TAG = "rithmic:bracket_reference="


def ticks_from_prices(
    reference: Any, price: Any, tick: Any, *, buy: bool, stop: bool, initial: bool
) -> int:
    """Coerce Python prices to exact fixed-point inputs for the Rust rule."""
    try:
        values = [Decimal(str(x)) for x in (reference, price, tick)]
    except InvalidOperation as exc:
        raise ValueError("bracket prices must be valid decimal values") from exc
    if any(not v.is_finite() for v in values):
        raise ValueError("bracket prices must be finite")
    places = max(0, *[-int(v.as_tuple().exponent) for v in values])
    if places > 9:
        raise ValueError("bracket prices exceed supported precision (9 decimals)")
    scale = 10**places
    fixed = [int(v * scale) for v in values]
    result = _lib.bracket_ticks(fixed[0], fixed[1], fixed[2], buy, stop, initial)
    if result is None:
        raise ValueError("bracket price is not an exact signed whole-tick distance")
    return result


def submission_ticks(host: Any, entry: Any, stop: Any, target: Any) -> tuple[int, int]:
    """Validate every leg before any submit event or plant command."""
    orders = (entry, stop, target)
    if {str(cid) for cid in (getattr(entry, "linked_order_ids", None) or [])} != {
        str(stop.client_order_id),
        str(target.client_order_id),
    }:
        raise ValueError("bracket entry must link both exit ids for restart recovery")
    if len({str(o.client_order_id) for o in orders}) != 3:
        raise ValueError("bracket client ids must be distinct")
    if entry.order_type not in (OrderType.MARKET, OrderType.LIMIT):
        raise ValueError("bracket entry must be MARKET or LIMIT")
    if stop.order_type != OrderType.STOP_MARKET or target.order_type != OrderType.LIMIT:
        raise ValueError("bracket exits must be STOP_MARKET and LIMIT")
    if entry.side not in (OrderSide.BUY, OrderSide.SELL):
        raise ValueError("bracket entry side must be BUY or SELL")
    for o in orders:
        if o.instrument_id != entry.instrument_id or o.strategy_id != entry.strategy_id:
            raise ValueError("bracket legs must share instrument and strategy")
        if Decimal(str(o.quantity)) != int(entry.quantity) or int(entry.quantity) <= 0:
            raise ValueError("bracket legs require equal positive integral quantities")
        if int(entry.quantity) > 2**31 - 1:
            raise ValueError("bracket quantity exceeds plant int32")
        if o.time_in_force != entry.time_in_force or o.time_in_force not in (
            TimeInForce.DAY,
            TimeInForce.GTC,
        ):
            raise ValueError("bracket legs require matching DAY or GTC duration")
        if (
            getattr(o, "is_post_only", False)
            or getattr(o, "display_qty", None) is not None
        ):
            raise ValueError("post-only / iceberg bracket legs are unsupported")
    opposite = OrderSide.SELL if entry.side == OrderSide.BUY else OrderSide.BUY
    if stop.side != opposite or target.side != opposite:
        raise ValueError("bracket exits must oppose entry")
    if getattr(entry, "is_reduce_only", False) or any(
        getattr(o, "is_quote_quantity", False) or getattr(o, "is_emulated", False)
        for o in orders
    ):
        raise ValueError(
            "reduce-only entry, quote quantity or emulated brackets unsupported"
        )
    if getattr(stop, "trigger_type", None) not in (None, TriggerType.DEFAULT):
        raise ValueError("bracket stop requires DEFAULT trigger")
    tags = [
        str(t)[len(REFERENCE_TAG) :]
        for t in (getattr(entry, "tags", None) or [])
        if str(t).startswith(REFERENCE_TAG)
    ]
    if len(tags) > 1:
        raise ValueError("bracket entry has multiple reference tags")
    reference = tags[0] if tags else getattr(entry, "price", None)
    if reference is None:
        raise ValueError(f"MARKET bracket requires entry tag {REFERENCE_TAG}<decimal>")
    instrument = host._cache.instrument(entry.instrument_id)
    if instrument is None:
        raise ValueError("bracket instrument missing from cache")
    tick = instrument.price_increment
    if entry.order_type == OrderType.LIMIT:
        # The reference tag changes exit geometry, never the actual entry price.
        ticks_from_prices(0, entry.price, tick, buy=True, stop=False, initial=False)
    buy = entry.side == OrderSide.BUY
    return (
        ticks_from_prices(
            reference, stop.trigger_price, tick, buy=buy, stop=True, initial=True
        ),
        ticks_from_prices(
            reference, target.price, tick, buy=buy, stop=False, initial=True
        ),
    )


@dataclass
class Bracket:
    entry: Any
    stop: Any
    target: Any
    basket: str | None = None


class NativeBrackets:
    """Nautilus boundary effects, with ownership recovered from cached linkage."""

    def __init__(self, host: Any) -> None:
        self.host = host
        self.groups: dict[str, Bracket] = {}
        self.refresh_task: asyncio.Task[None] | None = None
        self.refresh_pending = False

    def register(self, entry: Any, stop: Any, target: Any) -> Bracket:
        key = str(entry.client_order_id)
        if key not in self.groups:
            self.groups[key] = Bracket(entry, stop, target)
        return self.groups[key]

    def group_for_order(self, order: Any) -> Bracket | None:
        parent = getattr(order, "parent_order_id", None)
        if parent is None and not getattr(order, "linked_order_ids", None):
            return None
        entry = self.host._cache.order(parent) if parent is not None else order
        if entry is None:
            return None
        key = str(entry.client_order_id)
        if key in self.groups:
            group = self.groups[key]
            # rc9 cache views return fresh Python wrappers; identity and old
            # snapshots cannot identify a leg or supply a current fill.
            group.entry = (
                self.host._cache.order(group.entry.client_order_id) or group.entry
            )
            group.stop = (
                self.host._cache.order(group.stop.client_order_id) or group.stop
            )
            group.target = (
                self.host._cache.order(group.target.client_order_id) or group.target
            )
            return group
        linked = getattr(entry, "linked_order_ids", None) or []
        orders = [entry] + [self.host._cache.order(cid) for cid in linked]
        if len(orders) != 3 or any(o is None for o in orders):
            return None
        legs = self.host._bracket_legs(orders)
        if legs is None:
            return None
        return self.register(*legs)

    def resolve(self, fields: dict[str, Any]) -> ClientOrderId | None:
        """Inherited entry tags on children must never bind children to entry."""
        host = self.host
        basket = str(fields.get("basket_id") or "")
        original = str(fields.get("original_basket_id") or "")
        cid = host._client_order_id_for_venue(basket) if basket else None
        if cid is not None:
            cached = host._cache.order(cid)
            if cached is not None and self.group_for_order(cached) is not None:
                return cid
            return None
        parent_cid = host._client_order_id_for_venue(original) if original else None
        tag = fields.get("user_tag")
        candidate = host._cache.order(parent_cid) if parent_cid else None
        if candidate is None and tag:
            candidate = host._cache.order(ClientOrderId(str(tag)))
        if candidate is None:
            return None
        group = self.group_for_order(candidate)
        if group is None:
            return None
        entry_basket = host._venue_id_for_order(group.entry)
        if entry_basket:
            group.basket = entry_basket
        if original and original != basket:
            if group.basket is not None and original != group.basket:
                return None
            group.basket = original
            price_type = fields.get("price_type")
            side = fields.get("transaction_type")
            expected_side = 2 if group.entry.side == OrderSide.BUY else 1
            if side != expected_side:
                return None
            leg = (
                group.stop
                if price_type == 4
                else group.target
                if price_type == 1
                else None
            )
            if leg is None:
                return None
            existing = host._venue_id_for_order(leg)
            if existing and existing != basket:
                host._latch_order_plant(
                    "bracket linkage", "multiple venue baskets per exit are unsupported"
                )
                return None
            return leg.client_order_id
        # An entry must agree on side; an exit with an inherited tag and no
        # original basket is unresolved, never silently attributed to entry.
        if fields.get("transaction_type") == (
            1 if group.entry.side == OrderSide.BUY else 2
        ):
            if basket:
                group.basket = basket
            return group.entry.client_order_id
        return None

    def report_linkage(self, fields: dict[str, Any]) -> dict[str, Any]:
        if getattr(self.host, "_cache", None) is None:
            return {}
        cid = self.resolve(fields)
        order = self.host._cache.order(cid) if cid else None
        if order is None:
            tag = fields.get("user_tag")
            tagged = self.host._cache.order(ClientOrderId(str(tag))) if tag else None
            if tagged is not None and self.group_for_order(tagged) is not None:
                # An unresolved inherited entry tag is not an exit identity.
                return {"client_order_id": None}
            return {}
        if self.group_for_order(order) is None:
            return {}
        return {
            "client_order_id": cid,
            "parent_order_id": getattr(order, "parent_order_id", None),
            "linked_order_ids": getattr(order, "linked_order_ids", None),
            "contingency_type": getattr(order, "contingency_type", None),
        }

    async def modify(self, command: Any, order: Any) -> bool:
        group = self.group_for_order(order)
        host = self.host
        venue = host._venue_id_for_order(order)
        if group is None:
            if getattr(order, "parent_order_id", None) is not None:
                host._modify_rejected(
                    command, venue, "bracket linkage unavailable; cannot modify exit"
                )
                return True
            return False
        try:
            if order.client_order_id == group.entry.client_order_id:
                raise ValueError("native bracket entry modification is unsupported")
            if command.quantity is not None and command.quantity != order.quantity:
                raise ValueError("native bracket quantity modification is unsupported")
            stop = order.client_order_id == group.stop.client_order_id
            if (stop and command.price is not None) or (
                not stop and command.trigger_price is not None
            ):
                raise ValueError(
                    "bracket modify supports stop trigger or target price only"
                )
            price = command.trigger_price if stop else command.price
            if price is None:
                raise ValueError(
                    "bracket modify needs a new stop trigger or target price"
                )
            fill = getattr(group.entry, "avg_px", None)
            if fill is None or not getattr(group.entry, "filled_qty", 0):
                raise ValueError("bracket entry fill unknown; cannot adjust exit")
            basket = host._venue_id_for_order(group.entry) or group.basket
            if not basket:
                raise ValueError("bracket parent basket unknown; cannot adjust exit")
            instrument = host._cache.instrument(order.instrument_id)
            ticks = ticks_from_prices(
                fill,
                price,
                instrument.price_increment,
                buy=group.entry.side == OrderSide.BUY,
                stop=stop,
                initial=False,
            )
        except (ValueError, TypeError, AttributeError, OverflowError) as exc:
            host._modify_rejected(command, venue, str(exc))
            return True
        operation = (
            host._session.adjust_bracket_stop
            if stop
            else host._session.adjust_bracket_target
        )
        try:
            await asyncio.to_thread(operation, basket, ticks, None)
        except Exception as exc:
            host._order_command_failed(
                "adjust_bracket",
                exc,
                reject=lambda reason: host._modify_rejected(command, venue, reason),
            )
        return True

    def update(self, event: dict[str, Any]) -> None:
        """353 is a refresh hint: release counters are not remaining leaves."""
        self.refresh_pending = True
        if self.refresh_task is None or self.refresh_task.done():
            self.refresh_task = asyncio.get_running_loop().create_task(self.refresh())

    async def refresh(self) -> None:
        try:
            while self.refresh_pending:
                self.refresh_pending = False
                events = await self.host._load_orders_events(
                    0, int(self.host._clock.timestamp_ns() // 1_000_000_000)
                )
                self.host._apply_drain_rows(events)
        except Exception as exc:
            self.host._latch_order_plant("bracket refresh", str(exc))

    async def close(self) -> None:
        if self.refresh_task is not None:
            self.refresh_task.cancel()
            await asyncio.gather(self.refresh_task, return_exceptions=True)
            self.refresh_task = None


def brackets_for(host: Any) -> NativeBrackets:
    value = getattr(host, "_native_brackets", None)
    if value is None:
        value = NativeBrackets(host)
        host._native_brackets = value
    return value
