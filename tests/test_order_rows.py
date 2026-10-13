"""Order-row interpretation: venue closed-set values -> Nautilus meaning.

Fixtures use the real normalized payload shape (``order_notification_to_fields``
output: integer ``price_type`` / ``duration``, string ``kind``/``status``).
"""

from __future__ import annotations

from typing import Any

import pytest
from nautilus_trader.model.enums import OrderStatus, OrderType, TimeInForce, TriggerType
from rithmic_nt_connect._orders import enum_int, sentinel_none
from rithmic_nt_connect.order_rows import (
    order_status_from_fields,
    order_type_from_fields,
    row_is_trustworthy,
    tif_from_fields,
    trigger_type_from_fields,
)


@pytest.mark.parametrize(
    ("price_type", "expected"),
    [
        (1, OrderType.LIMIT),
        (2, OrderType.MARKET),
        (3, OrderType.STOP_LIMIT),
        (4, OrderType.STOP_MARKET),
        (None, OrderType.MARKET),  # absent: advisory default
        (9, OrderType.MARKET),  # unknown closed-set value: advisory default
        ("not-a-number", OrderType.MARKET),
    ],
)
def test_order_type_from_price_type(price_type: Any, expected: OrderType) -> None:
    assert order_type_from_fields({"price_type": price_type}) is expected


@pytest.mark.parametrize(
    ("duration", "expected"),
    [
        (1, TimeInForce.DAY),
        (2, TimeInForce.GTC),
        (3, TimeInForce.IOC),
        (4, TimeInForce.FOK),
        (None, TimeInForce.GTC),
        (9, TimeInForce.GTC),
        ("x", TimeInForce.GTC),
    ],
)
def test_tif_from_duration(duration: Any, expected: TimeInForce) -> None:
    assert tif_from_fields({"duration": duration}) is expected


@pytest.mark.parametrize(
    ("price_type", "expected"),
    [
        (3, TriggerType.DEFAULT),  # Rithmic StopLimit
        (4, TriggerType.DEFAULT),  # Rithmic StopMarket
        (1, TriggerType.NO_TRIGGER),
        (2, TriggerType.NO_TRIGGER),
        (None, TriggerType.NO_TRIGGER),
        ("x", TriggerType.NO_TRIGGER),
    ],
)
def test_trigger_type_from_price_type(price_type: Any, expected: TriggerType) -> None:
    assert trigger_type_from_fields({"price_type": price_type}) is expected


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        ({"kind": "filled"}, OrderStatus.FILLED),
        ({"quantity": 2, "total_fill_size": 2}, OrderStatus.FILLED),
        ({"quantity": 2, "total_fill_size": 1}, OrderStatus.PARTIALLY_FILLED),
        ({"kind": "rejected"}, OrderStatus.REJECTED),
        ({"status": "order rejected"}, OrderStatus.REJECTED),
        ({"kind": "canceled"}, OrderStatus.CANCELED),
        ({"status": "CANCELLED"}, OrderStatus.CANCELED),
        # A rejected cancel is still working, never CANCELED/REJECTED.
        ({"kind": "cancel_rejected"}, OrderStatus.ACCEPTED),
        (
            {"kind": "modify_rejected", "quantity": 4, "total_fill_size": 1},
            OrderStatus.PARTIALLY_FILLED,
        ),
        ({"kind": "expired"}, OrderStatus.EXPIRED),
        # Limit-style stops expose TRIGGERED; market-style stay working.
        ({"kind": "triggered", "price_type": 3}, OrderStatus.TRIGGERED),
        ({"kind": "triggered", "price_type": 4}, OrderStatus.ACCEPTED),
        ({"kind": "accepted"}, OrderStatus.ACCEPTED),
        ({"kind": "unknown", "status": "UNKNOWN"}, OrderStatus.ACCEPTED),
        ({}, OrderStatus.ACCEPTED),
    ],
)
def test_order_status_from_fields(
    fields: dict[str, Any], expected: OrderStatus
) -> None:
    assert order_status_from_fields(fields) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1, 1),
        ("1", 1),
        (2.0, 2),
        (1.5, None),  # non-integral truncation would fabricate a valid enum
        (True, None),  # bool is an int subclass
        (None, None),
        ("abc", None),
    ],
)
def test_enum_int_is_exact(value: Any, expected: int | None) -> None:
    assert enum_int(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, None), (-1.0, None), ("-1.0", None), (0.0, 0.0), (21000.25, 21000.25)],
)
def test_sentinel_none_maps_only_the_pending_price(value: Any, expected: Any) -> None:
    assert sentinel_none(value) == expected


def _row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "kind": "accepted",
        "status": "open",
        "transaction_type": 1,
        "price_type": 1,
        "duration": 2,
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"kind": None, "status": "TRIGGER PENDING"},  # resting stops carry only this
        {"kind": None, "status": "working"},
        {"kind": "expired", "status": None},
    ],
)
def test_trustworthy_rows(overrides: dict[str, Any]) -> None:
    assert row_is_trustworthy(_row(**overrides))


@pytest.mark.parametrize(
    "overrides",
    [
        {"transaction_type": None},  # no side
        {"price_type": None},
        {"duration": None},
        {"price_type": 9},  # outside the closed set
        {"duration": 9},
        {"price_type": True},  # bool must not coerce into an enum
        {"duration": 2.5},
        {"kind": None, "status": "garbled"},  # no recognizable order state
    ],
)
def test_untrustworthy_rows(overrides: dict[str, Any]) -> None:
    assert not row_is_trustworthy(_row(**overrides))
