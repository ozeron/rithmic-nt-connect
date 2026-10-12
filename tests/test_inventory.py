"""Node-less venue inventory (emergency flatten / plant status tools)."""

from __future__ import annotations

from collections import deque
from typing import Any

import pytest
from rithmic_nt_connect.inventory import (
    PNL_SETTLE_S,
    Inventory,
    venue_inventory_snapshot,
)


def _open(basket: str, symbol: str = "MNQU6") -> dict[str, Any]:
    return {
        "type": "order_notification",
        "source": "rithmic",
        "kind": "accepted",
        "notify_type_name": "OPEN",
        "basket_id": basket,
        "symbol": symbol,
        "exchange": "CME",
        "transaction_type": 1,
        "quantity": 1,
        "total_unfilled_size": 1,
    }


def test_open_then_cancel_clears_working() -> None:
    inv = Inventory()
    inv.ingest(_open("B1"))
    assert [w.basket_id for w in inv.working()] == ["B1"]
    assert inv.working()[0].side == "BUY"
    inv.ingest(
        {
            "type": "order_notification",
            "source": "exchange",
            "kind": "canceled",
            "basket_id": "B1",
            "symbol": "MNQU6",
            "status": "CANCELLED",
        }
    )
    assert inv.working() == []


def test_kind_is_derived_from_notify_type_when_missing() -> None:
    inv = Inventory()
    row = _open("B1")
    del row["kind"]
    inv.ingest(row)
    assert inv.working()[0].kind == "accepted"


def test_positions_need_a_settled_pnl_stream_and_flat_rows_clear() -> None:
    inv = Inventory()
    assert inv.pnl_live is False
    inv.ingest({"type": "account_pnl", "account_balance": 1}, now_s=100.0)
    # An account row lands before the instrument burst: not yet proof of flat.
    assert inv.pnl_live is False and inv.positions() == []
    inv.mark_observed(100.0 + PNL_SETTLE_S)
    assert inv.pnl_live is True
    inv.ingest({"type": "instrument_pnl", "symbol": "MNQU6", "net_quantity": -2})
    assert inv.positions()[0].net_qty == -2
    inv.ingest({"type": "instrument_pnl", "symbol": "MNQU6", "net_quantity": 0})
    assert inv.positions() == []


def test_terminal_status_follows_the_exec_client_mapping() -> None:
    inv = Inventory()
    inv.ingest(_open("B1"))
    expired = _open("B1")
    expired.update(kind="expired", status="EXPIRED")
    inv.ingest(expired)
    assert inv.working() == []  # EXPIRED is terminal (was missing before)


def test_a_fill_without_an_unfilled_size_keeps_the_order() -> None:
    inv = Inventory()
    inv.ingest(_open("B1"))
    fill = _open("B1")
    fill.update(kind="filled", quantity=2, total_fill_size=1)
    del fill["total_unfilled_size"]
    inv.ingest(fill)
    assert [w.basket_id for w in inv.working()] == ["B1"]  # unknown is not absent
    done = _open("B1")
    done.update(kind="filled", total_unfilled_size=0)
    inv.ingest(done)
    assert inv.working() == []


class _Session:
    def __init__(self, *, pnl_fails: bool = False) -> None:
        self._pnl: deque[dict[str, Any]] = deque()
        self._pnl_fails = pnl_fails

    def subscribe_order_updates(self) -> None: ...

    def subscribe_pnl(self) -> None:
        if self._pnl_fails:
            raise ConnectionError("pnl plant down")
        self._pnl.append(
            {"type": "instrument_pnl", "symbol": "MNQU6", "net_quantity": 1}
        )

    def load_orders(self, start_ssboe: int, end_ssboe: int) -> list[dict[str, Any]]:
        return [_open("B1")]

    def poll_order_event(self) -> dict[str, Any] | None:
        return None

    def poll_pnl_event(self) -> dict[str, Any] | None:
        return self._pnl.popleft() if self._pnl else None


def test_snapshot_replays_orders_and_reads_pnl() -> None:
    inv = venue_inventory_snapshot(_Session(), seconds=0.05)
    assert [w.basket_id for w in inv.working()] == ["B1"]
    assert inv.positions()[0].net_qty == 1
    assert inv.pnl_live is False  # a 50 ms window is shorter than the settle gate


def test_snapshot_raises_when_pnl_cannot_subscribe() -> None:
    with pytest.raises(ConnectionError):
        venue_inventory_snapshot(_Session(pnl_fails=True), seconds=0.01)
