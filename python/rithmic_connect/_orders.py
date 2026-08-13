"""Order mapping helpers and notification → action classification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Mapping

from rithmic_connect._convert import ConvertError
from rithmic_connect._convert import _ts_ns
from rithmic_connect._convert import instrument_id_from_symbol
from rithmic_connect.constants import VENUE

OrderActionKind = Literal[
    "accepted",
    "rejected",
    "updated",
    "canceled",
    "triggered",
    "filled",
    "modify_rejected",
    "cancel_rejected",
]


class OrderMapError(ValueError):
    """Raised when an order type/side/TIF cannot be mapped to Rithmic."""


_ORDER_TYPE_TO_RITHMIC: dict[str, str] = {
    "MARKET": "MARKET",
    "LIMIT": "LIMIT",
    "STOP_MARKET": "STOP_MARKET",
    "STOP_LIMIT": "STOP_LIMIT",
    "MARKET_IF_TOUCHED": "MARKET_IF_TOUCHED",
    "LIMIT_IF_TOUCHED": "LIMIT_IF_TOUCHED",
}

_TIF_TO_RITHMIC: dict[str, str] = {
    "DAY": "DAY",
    "GTC": "GTC",
    "IOC": "IOC",
    "FOK": "FOK",
}


@dataclass(frozen=True)
class OrderAction:
    kind: OrderActionKind
    reason: str | None = None
    quantity: Any | None = None
    price: Any | None = None
    trigger: Any | None = None
    fill_qty: Any | None = None
    fill_px: Any | None = None
    trade_id: str | None = None


def nautilus_side_to_rithmic(side: Any) -> str:
    name = getattr(side, "name", None) or str(side)
    name = name.upper().removeprefix("ORDERSIDE.")
    if name in {"BUY", "B"}:
        return "BUY"
    if name in {"SELL", "S"}:
        return "SELL"
    raise OrderMapError(f"unsupported order side: {side!r}")


def nautilus_order_type_to_rithmic(order_type: Any) -> str:
    name = getattr(order_type, "name", None) or str(order_type)
    name = name.upper().removeprefix("ORDERTYPE.")
    mapped = _ORDER_TYPE_TO_RITHMIC.get(name)
    if mapped is None:
        raise OrderMapError(f"unsupported order type for Rithmic: {order_type!r}")
    return mapped


def nautilus_tif_to_rithmic(tif: Any) -> str:
    name = getattr(tif, "name", None) or str(tif)
    name = name.upper().removeprefix("TIMEINFORCE.")
    mapped = _TIF_TO_RITHMIC.get(name)
    if mapped is None:
        raise OrderMapError(f"unsupported time in force for Rithmic: {tif!r}")
    return mapped


def order_notification_to_fields(d: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize a wire order_notification dict for the exec client."""
    if d.get("type") not in (None, "order_notification"):
        raise ConvertError(f"unexpected order notification type: {d.get('type')!r}")
    source = d.get("source")
    if source not in ("rithmic", "exchange"):
        raise ConvertError(f"order notification missing source: {source!r}")
    symbol = d.get("symbol")
    if not symbol:
        raise ConvertError("order notification missing symbol")
    ts = _ts_ns(d)
    kind = d.get("kind")
    if kind is None and d.get("notify_type_name") is not None:
        kind = kind_from_notify(str(source), str(d.get("notify_type_name")), d.get("status"))
    return {
        "type": "order_notification",
        "source": source,
        "kind": kind,
        "notify_type": d.get("notify_type"),
        "notify_type_name": d.get("notify_type_name"),
        "status": d.get("status"),
        "basket_id": d.get("basket_id"),
        "exchange_order_id": d.get("exchange_order_id"),
        "user_tag": d.get("user_tag"),
        "account_id": d.get("account_id"),
        "symbol": str(symbol),
        "exchange": d.get("exchange"),
        "instrument_id": instrument_id_from_symbol(str(symbol), d.get("exchange")),
        "quantity": d.get("quantity"),
        "total_fill_size": d.get("total_fill_size"),
        "total_unfilled_size": d.get("total_unfilled_size"),
        "fill_size": d.get("fill_size"),
        "price": d.get("price"),
        "trigger_price": d.get("trigger_price"),
        "avg_fill_price": d.get("avg_fill_price"),
        "fill_price": d.get("fill_price"),
        "transaction_type": d.get("transaction_type"),
        "price_type": d.get("price_type"),
        "fill_id": d.get("fill_id"),
        "text": d.get("text"),
        "report_text": d.get("report_text"),
        "completion_reason": d.get("completion_reason"),
        "ssboe": d.get("ssboe"),
        "usecs": d.get("usecs"),
        "ts_event": ts,
        "is_snapshot": d.get("is_snapshot"),
        "venue": VENUE,
    }


def kind_from_notify(
    source: str,
    notify_type_name: str,
    status: Any | None = None,
) -> str | None:
    """Map plant notify_type_name to a canonical action kind (also set in Rust)."""
    name = notify_type_name.upper()
    if source == "rithmic":
        if name == "OPEN":
            return "accepted"
        if name == "MODIFIED":
            return "updated"
        if name == "MODIFICATION_FAILED":
            return "modify_rejected"
        if name == "CANCELLATION_FAILED":
            return "cancel_rejected"
        if name == "COMPLETE":
            status_u = str(status or "").upper()
            if status_u in {"CANCELLED", "CANCELED"}:
                return "canceled"
            return None
        return None
    if source == "exchange":
        if name == "FILL":
            return "filled"
        if name == "REJECT":
            return "rejected"
        if name == "CANCEL":
            return "canceled"
        if name == "TRIGGER":
            return "triggered"
        if name == "NOT_MODIFIED":
            return "modify_rejected"
        if name in {"NOT_CANCELLED", "NOT_CANCELED"}:
            return "cancel_rejected"
        return None
    return None


def trade_id_from_fill_fields(fields: Mapping[str, Any], ts_event: int) -> str:
    fill_id = fields.get("fill_id")
    if fill_id:
        return str(fill_id)
    basket = fields.get("basket_id") or ""
    exch = fields.get("exchange_order_id") or ""
    fill_sz = fields.get("fill_size")
    fill_px = fields.get("fill_price")
    return f"{basket}:{exch}:{ts_event}:{fill_sz}:{fill_px}"


def notification_action(fields: Mapping[str, Any], order: Any) -> OrderAction | None:
    """Classify a normalized notification into one emit action (or None to ignore)."""
    kind = fields.get("kind")
    if not kind:
        return None
    reason = fields.get("text") or fields.get("report_text") or fields.get("status")

    if kind == "accepted":
        return OrderAction(kind="accepted")
    if kind == "rejected":
        return OrderAction(kind="rejected", reason=str(reason or "REJECT"))
    if kind == "modify_rejected":
        return OrderAction(kind="modify_rejected", reason=str(reason or "NOT_MODIFIED"))
    if kind == "cancel_rejected":
        return OrderAction(kind="cancel_rejected", reason=str(reason or "NOT_CANCELLED"))
    if kind == "updated":
        qty_raw = fields.get("quantity")
        qty = int(qty_raw) if qty_raw is not None else int(order.quantity)
        price = fields.get("price")
        trigger = fields.get("trigger_price")
        return OrderAction(
            kind="updated",
            quantity=qty,
            price=price,
            trigger=trigger,
        )
    if kind == "canceled":
        return OrderAction(kind="canceled")
    if kind == "triggered":
        return OrderAction(kind="triggered")
    if kind == "filled":
        fill_px = fields.get("fill_price")
        fill_sz = fields.get("fill_size")
        if fill_px is None or fill_sz is None:
            return None
        ts_event = int(fields.get("ts_event") or 0)
        return OrderAction(
            kind="filled",
            fill_qty=int(fill_sz),
            fill_px=fill_px,
            trade_id=trade_id_from_fill_fields(fields, ts_event),
        )
    return None
