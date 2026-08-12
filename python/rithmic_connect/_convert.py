"""Convert venue DTO dicts into validated field maps for later Nautilus types.

Phase 1 keeps these as plain dicts so unit tests run without the PyO3 extension
or ``nautilus_trader`` installed. U4+ will construct ``TradeTick`` / ``QuoteTick``
from these fields.
"""

from __future__ import annotations

from typing import Any, Mapping

from rithmic_connect.constants import VENUE


class ConvertError(ValueError):
    """Raised when a venue DTO dict is missing required fields."""


def _require(d: Mapping[str, Any], *keys: str) -> None:
    missing = [k for k in keys if k not in d or d[k] is None]
    if missing:
        raise ConvertError(f"missing required fields: {', '.join(missing)}")


def _ts_ns(d: Mapping[str, Any]) -> int | None:
    if "ts_event_ns" in d and d["ts_event_ns"] is not None:
        return int(d["ts_event_ns"])
    ssboe = d.get("ssboe")
    usecs = d.get("usecs")
    if ssboe is None:
        return None
    usecs_i = int(usecs or 0)
    return int(ssboe) * 1_000_000_000 + usecs_i * 1_000


def instrument_id_from_symbol(symbol: str, exchange: str | None = None) -> str:
    """Build a Nautilus-style instrument id string (symbol.VENUE)."""
    _ = exchange  # exchange retained for callers; venue is constant for Phase 1
    return f"{symbol}.{VENUE}"


def last_trade_to_fields(d: Mapping[str, Any]) -> dict[str, Any]:
    """Map a LastTrade venue dict to TradeTick-oriented fields."""
    _require(d, "symbol", "trade_price", "trade_size")
    symbol = str(d["symbol"])
    exchange = d.get("exchange")
    ts = _ts_ns(d)
    if ts is None:
        raise ConvertError("missing timestamp fields: ts_event_ns or ssboe")
    return {
        "type": "trade",
        "instrument_id": instrument_id_from_symbol(symbol, exchange),
        "symbol": symbol,
        "exchange": exchange,
        "price": float(d["trade_price"]),
        "size": float(d["trade_size"]),
        "aggressor": d.get("aggressor"),
        "ts_event": ts,
        "venue": VENUE,
    }


def bbo_to_fields(d: Mapping[str, Any]) -> dict[str, Any]:
    """Map a BestBidOffer venue dict to QuoteTick-oriented fields."""
    _require(d, "symbol", "bid_price", "ask_price", "bid_size", "ask_size")
    symbol = str(d["symbol"])
    exchange = d.get("exchange")
    ts = _ts_ns(d)
    if ts is None:
        raise ConvertError("missing timestamp fields: ts_event_ns or ssboe")
    return {
        "type": "quote",
        "instrument_id": instrument_id_from_symbol(symbol, exchange),
        "symbol": symbol,
        "exchange": exchange,
        "bid_price": float(d["bid_price"]),
        "ask_price": float(d["ask_price"]),
        "bid_size": float(d["bid_size"]),
        "ask_size": float(d["ask_size"]),
        "ts_event": ts,
        "venue": VENUE,
    }


def account_pnl_to_fields(d: Mapping[str, Any]) -> dict[str, Any]:
    """Map an AccountPnL venue dict to AccountState-oriented fields."""
    _require(d, "account_id")
    return {
        "type": "account_pnl",
        "account_id": str(d["account_id"]),
        "fcm_id": d.get("fcm_id"),
        "ib_id": d.get("ib_id"),
        "account_balance": d.get("account_balance"),
        "cash_on_hand": d.get("cash_on_hand"),
        "margin_balance": d.get("margin_balance"),
        "day_pnl": d.get("day_pnl"),
        "open_position_pnl": d.get("open_position_pnl"),
        "closed_position_pnl": d.get("closed_position_pnl"),
        "available_buying_power": d.get("available_buying_power"),
        "used_buying_power": d.get("used_buying_power"),
        "currency": d.get("currency", "USD"),
        "is_snapshot": d.get("is_snapshot"),
        "venue": VENUE,
    }


def order_book_to_fields(d: Mapping[str, Any]) -> dict[str, Any]:
    """Map an OrderBook venue dict to OrderBookDeltas-oriented fields.

    Produces a CLEAR + ADD snapshot for the provided bid/ask level arrays.
    """
    _require(d, "symbol")
    symbol = str(d["symbol"])
    exchange = d.get("exchange")
    ts = _ts_ns(d)
    if ts is None:
        raise ConvertError("missing timestamp fields: ts_event_ns or ssboe")

    bid_price = list(d.get("bid_price") or [])
    bid_size = list(d.get("bid_size") or [])
    ask_price = list(d.get("ask_price") or [])
    ask_size = list(d.get("ask_size") or [])
    if not bid_price and not ask_price:
        raise ConvertError("order book has no bid/ask levels")

    levels: list[dict[str, Any]] = []
    for i, price in enumerate(bid_price):
        size = bid_size[i] if i < len(bid_size) else 0
        if price is None or size is None:
            continue
        levels.append(
            {
                "side": "BUY",
                "price": float(price),
                "size": float(size),
                "order_id": i + 1,
            }
        )
    for i, price in enumerate(ask_price):
        size = ask_size[i] if i < len(ask_size) else 0
        if price is None or size is None:
            continue
        levels.append(
            {
                "side": "SELL",
                "price": float(price),
                "size": float(size),
                "order_id": 1_000_000 + i + 1,
            }
        )
    if not levels:
        raise ConvertError("order book levels empty after filtering")

    return {
        "type": "order_book",
        "instrument_id": instrument_id_from_symbol(symbol, exchange),
        "symbol": symbol,
        "exchange": exchange,
        "update_type": d.get("update_type"),
        "levels": levels,
        "ts_event": ts,
        "venue": VENUE,
    }


def instrument_pnl_to_fields(d: Mapping[str, Any]) -> dict[str, Any]:
    """Map an InstrumentPnL venue dict to position-oriented fields."""
    _require(d, "symbol")
    symbol = str(d["symbol"])
    exchange = d.get("exchange")
    net = d.get("net_quantity")
    if net is None:
        open_qty = d.get("open_position_quantity")
        net = open_qty if open_qty is not None else 0
    net_i = int(net)
    if net_i > 0:
        side = "LONG"
        qty = net_i
    elif net_i < 0:
        side = "SHORT"
        qty = abs(net_i)
    else:
        side = "FLAT"
        qty = 0
    return {
        "type": "instrument_pnl",
        "account_id": d.get("account_id"),
        "instrument_id": instrument_id_from_symbol(symbol, exchange),
        "symbol": symbol,
        "exchange": exchange,
        "position_side": side,
        "quantity": qty,
        "avg_px_open": d.get("avg_open_fill_price"),
        "open_position_pnl": d.get("open_position_pnl"),
        "closed_position_pnl": d.get("closed_position_pnl"),
        "is_snapshot": d.get("is_snapshot"),
        "venue": VENUE,
    }
