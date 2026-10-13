"""Single wire-float → Nautilus ``Price`` conversion for data and execution."""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.model.objects import Price

from rithmic_nt_connect._convert import ConvertError, format_price_str


def price_from_wire(
    value: float | Decimal | str, precision: int | None = None
) -> Price:
    """Build a ``Price`` from a venue number.

    Without ``precision`` the shortest faithful decimal string is used; with it
    the value is fixed to that many places (instrument ``price_precision``).
    """
    if precision is None:
        return Price.from_str(format_price_str(value))
    if precision < 0:
        raise ConvertError(f"price_precision must be >= 0, got {precision}")
    return Price.from_str(f"{float(value):.{int(precision)}f}")
