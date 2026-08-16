"""Convert venue DTO dicts into validated field maps for later Nautilus types.

Phase 1 keeps these as plain dicts so unit tests run without the PyO3 extension
or ``nautilus_trader`` installed. U4+ will construct ``TradeTick`` / ``QuoteTick``
from these fields.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from rithmic_nt_connect.constants import VENUE


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


def format_price_str(value: float | Decimal | str) -> str:
    """Normalize a numeric price to a Nautilus ``Price.from_str``-ready text."""
    text = f"{float(value):.8f}".rstrip("0").rstrip(".")
    if "." not in text:
        text = f"{text}.0"
    return text


def rithmic_route_from_info(
    info: Mapping[str, Any],
    *,
    instrument_id: str | None = None,
) -> tuple[str, str]:
    symbol = info.get("rithmic_symbol")
    exchange = info.get("rithmic_exchange")
    if not symbol or not exchange:
        label = instrument_id if instrument_id is not None else "instrument"
        raise ValueError(f"instrument {label} missing rithmic_symbol/rithmic_exchange in info")
    return str(symbol), str(exchange)


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
    from rithmic_nt_connect.constants import DEFAULT_ACCOUNT_CURRENCY

    _require(d, "account_id")
    cash = d.get("cash_on_hand")
    balance = d.get("account_balance")
    if cash is None and balance is None:
        cash = "0"
        balance = "0"
    currency = d.get("currency") or DEFAULT_ACCOUNT_CURRENCY
    return {
        "type": "account_pnl",
        "account_id": str(d["account_id"]),
        "fcm_id": d.get("fcm_id"),
        "ib_id": d.get("ib_id"),
        "account_balance": balance,
        "cash_on_hand": cash,
        "margin_balance": d.get("margin_balance"),
        "day_pnl": d.get("day_pnl"),
        "open_position_pnl": d.get("open_position_pnl"),
        "closed_position_pnl": d.get("closed_position_pnl"),
        "available_buying_power": d.get("available_buying_power"),
        "used_buying_power": d.get("used_buying_power"),
        "currency": currency,
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

    if "bid_price" not in d or "ask_price" not in d:
        raise ConvertError("missing required fields: bid_price, ask_price")
    bid_price = list(d["bid_price"] if d["bid_price"] is not None else [])
    bid_size = list(d["bid_size"] if d.get("bid_size") is not None else [])
    ask_price = list(d["ask_price"] if d["ask_price"] is not None else [])
    ask_size = list(d["ask_size"] if d.get("ask_size") is not None else [])
    if len(bid_price) != len(bid_size):
        raise ConvertError(
            f"bid_price/bid_size length mismatch: {len(bid_price)} vs {len(bid_size)}"
        )
    if len(ask_price) != len(ask_size):
        raise ConvertError(
            f"ask_price/ask_size length mismatch: {len(ask_price)} vs {len(ask_size)}"
        )
    if not bid_price and not ask_price:
        raise ConvertError("order book has no bid/ask levels")

    levels: list[dict[str, Any]] = []
    for i, price in enumerate(bid_price):
        size = bid_size[i]
        if price is None or size is None:
            raise ConvertError(f"bid level {i} has null price/size")
        levels.append(
            {
                "side": "BUY",
                "price": float(price),
                "size": float(size),
                "order_id": i + 1,
            }
        )
    for i, price in enumerate(ask_price):
        size = ask_size[i]
        if price is None or size is None:
            raise ConvertError(f"ask level {i} has null price/size")
        levels.append(
            {
                "side": "SELL",
                "price": float(price),
                "size": float(size),
                "order_id": 1_000_000 + i + 1,
            }
        )

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
    _require(d, "symbol", "net_quantity")
    symbol = str(d["symbol"])
    exchange = d.get("exchange")
    net_i = int(d["net_quantity"])
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


def time_bar_to_fields(d: Mapping[str, Any]) -> dict[str, Any]:
    """Map a history_bar venue dict to Bar-oriented fields.

    ``ts_event`` is the venue **close** time. The close→open shift is applied by
    ``fields_to_bar`` (which knows the authoritative Nautilus ``BarType``
    duration) — NOT here, because the wire ``period`` unit (native vs seconds)
    is not reliable.
    """
    _require(d, "symbol", "open_price", "high_price", "low_price", "close_price", "volume")
    symbol = str(d["symbol"])
    exchange = d.get("exchange")
    ts = _ts_ns(d)
    if ts is None and d.get("marker") is not None:
        ts = int(d["marker"]) * 1_000_000_000
    if ts is None:
        raise ConvertError("missing timestamp fields: ts_event_ns, ssboe, or marker")
    volume = float(d["volume"])
    if volume < 0:
        raise ConvertError(f"bar volume must be >= 0, got {volume}")
    return {
        "type": "bar",
        "instrument_id": instrument_id_from_symbol(symbol, exchange),
        "symbol": symbol,
        "exchange": exchange,
        "open": float(d["open_price"]),
        "high": float(d["high_price"]),
        "low": float(d["low_price"]),
        "close": float(d["close_price"]),
        "volume": volume,
        "num_trades": d.get("num_trades"),
        "bar_type": d.get("bar_type"),
        "period": d.get("period"),
        "marker": d.get("marker"),
        "ts_event": ts,
        "venue": VENUE,
    }
