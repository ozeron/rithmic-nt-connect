"""Front-month resolution helpers."""

from __future__ import annotations

from typing import Any, Mapping

from rithmic_nt_connect.session import WireSession


class FrontMonthError(ValueError):
    """Raised when front-month resolution fails."""


def resolve_front_month(
    session: WireSession,
    root: str,
    exchange: str,
) -> dict[str, Any]:
    """Resolve the listed front contract for a product root.

    Returns a stable dict suitable for APIs / verify reports::

        {
          "root": "NQ",
          "exchange": "CME",
          "trading_symbol": "NQU6",
          "trading_exchange": "CME",
          "symbol_name": "...",
          "is_front_month_symbol": True,
          "raw": {...},
        }
    """
    raw = session.get_front_month(root, exchange)
    if not isinstance(raw, Mapping):
        raise FrontMonthError(f"unexpected front-month payload type: {type(raw)!r}")
    trading_symbol = raw.get("trading_symbol")
    if not trading_symbol:
        raise FrontMonthError(f"front-month response missing trading_symbol for {root}/{exchange}")
    trading_exchange = raw.get("trading_exchange")
    if not trading_exchange:
        raise FrontMonthError(
            f"front-month response missing trading_exchange for {root}/{exchange}"
        )
    return {
        "root": root,
        "exchange": exchange,
        "trading_symbol": str(trading_symbol),
        "trading_exchange": str(trading_exchange),
        "symbol_name": raw.get("symbol_name"),
        "is_front_month_symbol": raw.get("is_front_month_symbol"),
        "raw": dict(raw),
    }
