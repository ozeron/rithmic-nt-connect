"""Order-row interpretation: venue closed-set values -> Nautilus meaning.

One place decides what a normalized Rithmic order row (live notification or
working-orders drain row) means: order type / time-in-force / trigger type /
status, and whether a drain row is trustworthy enough to bind a venue id.
Builders are deliberately permissive for advisory reports (unknown closed-set
values fall back to MARKET / GTC / ACCEPTED so one malformed row cannot abort
recon); ``row_is_trustworthy`` is the strict gate for anything that binds
identity.
"""

from __future__ import annotations

from typing import Any

from nautilus_trader.model.enums import (
    OrderStatus,
    OrderType,
    TimeInForce,
    TriggerType,
)

from rithmic_nt_connect import _lib
from rithmic_nt_connect._orders import (
    RITHMIC_DURATION_TO_TIF,
    RITHMIC_PRICE_TYPE_TO_ORDER_TYPE,
    enum_int,
    order_side_from_notification,
)

# Rithmic price types that carry a trigger price (StopLimit / StopMarket).
_STOP_PRICE_TYPES = frozenset(
    price_type
    for price_type, order_type in RITHMIC_PRICE_TYPE_TO_ORDER_TYPE.items()
    if order_type in (OrderType.STOP_LIMIT, OrderType.STOP_MARKET)
)

# Order types that support the TRIGGERED order status (Nautilus #3812).
# Market-style stops execute immediately on trigger and have no intermediate
# TRIGGERED state.
TRIGGERABLE_ORDER_TYPES = frozenset(
    {
        OrderType.STOP_LIMIT,
        OrderType.TRAILING_STOP_LIMIT,
        OrderType.LIMIT_IF_TOUCHED,
    }
)


def order_type_from_fields(fields: dict[str, Any]) -> OrderType:
    """Nautilus order type for a normalized notification row (pure)."""
    raw = fields.get("price_type")
    if raw is None:
        return OrderType.MARKET
    try:
        return RITHMIC_PRICE_TYPE_TO_ORDER_TYPE.get(int(raw), OrderType.MARKET)
    except (TypeError, ValueError):
        return OrderType.MARKET


def tif_from_fields(fields: dict[str, Any]) -> TimeInForce:
    """Nautilus time-in-force for a normalized notification row (pure)."""
    raw = fields.get("duration")
    if raw is None:
        return TimeInForce.GTC
    try:
        return RITHMIC_DURATION_TO_TIF.get(int(raw), TimeInForce.GTC)
    except (TypeError, ValueError):
        return TimeInForce.GTC


def trigger_type_from_fields(fields: dict[str, Any]) -> TriggerType:
    """Nautilus trigger type for a normalized notification row (pure)."""
    raw = fields.get("price_type")
    if raw is None:
        return TriggerType.NO_TRIGGER
    try:
        if int(raw) in _STOP_PRICE_TYPES:
            return TriggerType.DEFAULT
    except (TypeError, ValueError):
        pass
    return TriggerType.NO_TRIGGER


def order_status_from_fields(fields: dict[str, Any]) -> OrderStatus:
    """Venue drain/notification row -> Nautilus order status (pure).

    Operation-rejection kinds leave the order working (never terminal
    REJECTED for a live order); everything not recognizably terminal maps to
    ACCEPTED so recon never fabricates a terminal outcome.
    """
    kind = fields.get("kind")
    status_u = str(fields.get("status") or "").upper()
    try:
        qty = int(fields.get("quantity") or 0)
        filled = int(fields.get("total_fill_size") or 0)
    except (TypeError, ValueError):
        qty = filled = 0
    if kind == "filled" or (qty > 0 and filled >= qty):
        return OrderStatus.FILLED
    # PARTIALLY_FILLED when partial, else ACCEPTED (not PENDING_CANCEL:
    # recon would leave the OMS believing a cancel is in flight forever).
    if kind in ("modify_rejected", "cancel_rejected"):
        if qty > 0 and 0 < filled < qty:
            return OrderStatus.PARTIALLY_FILLED
        return OrderStatus.ACCEPTED
    # Check genuine reject kinds/status before the CANCEL substring so a
    # rejected cancel (e.g. "CANCELLATION_FAILED") is not CANCELED.
    if kind == "rejected" or "REJECT" in status_u:
        return OrderStatus.REJECTED
    if kind == "canceled" or status_u in ("CANCELLED", "CANCELED"):
        return OrderStatus.CANCELED
    if kind == "expired" or "EXPIRED" in status_u:
        return OrderStatus.EXPIRED
    if qty > 0 and 0 < filled < qty:
        return OrderStatus.PARTIALLY_FILLED
    if kind == "triggered":
        # Only limit-style stops expose TRIGGERED (#3812); market-style
        # stops execute straight through on trigger and stay ACCEPTED
        # (working) until the fill arrives.
        if order_type_from_fields(fields) in TRIGGERABLE_ORDER_TYPES:
            return OrderStatus.TRIGGERED
        return OrderStatus.ACCEPTED
    # Everything else defaults to working (ACCEPTED): a row that is not
    # recognizably filled/rejected/canceled/expired is an open order.
    return OrderStatus.ACCEPTED


def row_is_trustworthy(fields: dict[str, Any]) -> bool:
    """A drain row binds a venue id only when its closed-set execution terms
    are real: side present, ``price_type``/``duration`` present and mappable,
    and a recognizable order state. Never fabricates terms (a row with
    missing/unknown closed-set values is advisory-only).

    The decision lives in the ``rithmic-exec`` crate; here the Python-object
    coercions (``enum_int``, the side) are applied and the result passed on.
    The closed-set values are exact integers or None, so a bool/non-integral
    value can never coerce into a valid enum.
    """
    kind = fields.get("kind")
    return _lib.row_is_trustworthy(
        order_side_from_notification(fields) is not None,
        enum_int(fields.get("price_type")),
        enum_int(fields.get("duration")),
        kind if isinstance(kind, str) else None,
        str(fields.get("status") or ""),
    )
