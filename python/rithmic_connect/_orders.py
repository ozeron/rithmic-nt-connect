"""Order-side mapping helpers for Phase 2 (Nautilus ↔ Rithmic wire strings)."""

from __future__ import annotations

from typing import Any, Mapping

from rithmic_connect._convert import ConvertError
from rithmic_connect._convert import _ts_ns
from rithmic_connect._convert import instrument_id_from_symbol
from rithmic_connect.constants import VENUE


class OrderMapError(ValueError):
    """Raised when an order type/side/TIF cannot be mapped to Rithmic."""


# Nautilus OrderType int / name → rithmic-rs OrderType as_str_name
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

# Rithmic notify_type for exchange fills
_EXCHANGE_FILL = 5
_EXCHANGE_REJECT = 6
_EXCHANGE_CANCEL = 3

# Rithmic plant notify types of interest
_RITHMIC_OPEN = 13
_RITHMIC_COMPLETE = 15
_RITHMIC_MODIFICATION_FAILED = 16
_RITHMIC_CANCELLATION_FAILED = 17


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
        # allow raw dicts missing type when source is set
        if d.get("source") not in ("rithmic", "exchange"):
            raise ConvertError(f"unexpected order notification type: {d.get('type')!r}")
    source = d.get("source")
    if source not in ("rithmic", "exchange"):
        raise ConvertError(f"order notification missing source: {source!r}")
    symbol = d.get("symbol")
    if not symbol:
        raise ConvertError("order notification missing symbol")
    ts = _ts_ns(d)
    return {
        "type": "order_notification",
        "source": source,
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


def is_exchange_fill(fields: Mapping[str, Any]) -> bool:
    if fields.get("source") != "exchange":
        return False
    name = fields.get("notify_type_name")
    if name == "FILL":
        return True
    return fields.get("notify_type") == _EXCHANGE_FILL


def is_exchange_reject(fields: Mapping[str, Any]) -> bool:
    if fields.get("source") != "exchange":
        return False
    name = fields.get("notify_type_name")
    if name == "REJECT":
        return True
    return fields.get("notify_type") == _EXCHANGE_REJECT


def is_exchange_cancel(fields: Mapping[str, Any]) -> bool:
    if fields.get("source") != "exchange":
        return False
    name = fields.get("notify_type_name")
    if name == "CANCEL":
        return True
    return fields.get("notify_type") == _EXCHANGE_CANCEL


def is_rithmic_open(fields: Mapping[str, Any]) -> bool:
    if fields.get("source") != "rithmic":
        return False
    name = fields.get("notify_type_name")
    if name == "OPEN":
        return True
    return fields.get("notify_type") == _RITHMIC_OPEN


def is_rithmic_complete(fields: Mapping[str, Any]) -> bool:
    if fields.get("source") != "rithmic":
        return False
    name = fields.get("notify_type_name")
    if name == "COMPLETE":
        return True
    return fields.get("notify_type") == _RITHMIC_COMPLETE


def is_rithmic_modify_failed(fields: Mapping[str, Any]) -> bool:
    if fields.get("source") != "rithmic":
        return False
    name = fields.get("notify_type_name")
    if name == "MODIFICATION_FAILED":
        return True
    return fields.get("notify_type") == _RITHMIC_MODIFICATION_FAILED


def is_rithmic_cancel_failed(fields: Mapping[str, Any]) -> bool:
    if fields.get("source") != "rithmic":
        return False
    name = fields.get("notify_type_name")
    if name == "CANCELLATION_FAILED":
        return True
    return fields.get("notify_type") == _RITHMIC_CANCELLATION_FAILED
