"""cancel_all_orders honors the command's scope; batch cancel loops per order."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, cast

from _stubs import FaultInjectingSession, _Log, _TestClient
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    BatchCancelOrders,
    CancelAllOrders,
    CancelOrder,
)
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import (
    ClientOrderId,
    InstrumentId,
    StrategyId,
    TraderId,
    VenueOrderId,
)
from rithmic_nt_connect._order_plant import OrderPlantPolicy, OrderPlantState
from rithmic_nt_connect.session import WireSession

IID = InstrumentId.from_str("MNQZ6.RITHMIC")
SID = StrategyId("S-1")
TID = TraderId("TRADER-001")


def _order(coid: str, basket: str) -> Any:
    return SimpleNamespace(
        client_order_id=ClientOrderId(coid),
        venue_order_id=VenueOrderId(basket),
        strategy_id=SID,
        instrument_id=IID,
    )


class _Cache:
    def __init__(self, orders: list[Any]) -> None:
        self.orders = orders
        self.queries: list[dict[str, object]] = []

    def orders_open(self, **filters: object) -> list[Any]:
        self.queries.append(filters)
        return list(self.orders)

    def venue_order_id(self, coid: ClientOrderId) -> VenueOrderId | None:
        return None


def _client(cache: _Cache) -> tuple[_TestClient, FaultInjectingSession]:
    session = FaultInjectingSession()
    client = _TestClient.__new__(_TestClient)
    client._log = _Log()
    client._clock = SimpleNamespace(timestamp_ns=lambda: 2)
    client._cache = cache
    client._config_local = cast(
        Any, SimpleNamespace(enable_trading=True, soft_fail_pnl=False)
    )
    client._order_plant = OrderPlantPolicy(OrderPlantState.LIVE)
    client._session = cast(WireSession, session)
    return client, session


def test_cancel_all_is_scoped_and_never_plant_wide() -> None:
    cache = _Cache([_order("O-1", "B1"), _order("O-2", "B2")])
    client, session = _client(cache)
    cmd = CancelAllOrders(TID, SID, IID, OrderSide.SELL, UUID4(), 0)
    asyncio.run(client._cancel_all_orders(cmd))
    # Scoped to this client's venue too: the node cache is shared across venues.
    assert cache.queries == [
        {
            "venue": client.venue,
            "instrument_id": IID,
            "strategy_id": SID,
            "side": OrderSide.SELL,
        }
    ]
    assert session.calls == ["cancel_order:B1", "cancel_order:B2"]
    assert "cancel_all_orders" not in session.calls


def test_batch_cancel_cancels_each_order() -> None:
    cache = _Cache([])
    client, session = _client(cache)
    cancels = [
        CancelOrder(
            TID, SID, IID, ClientOrderId(f"O-{i}"), VenueOrderId(f"B{i}"), UUID4(), 0
        )
        for i in (1, 2)
    ]
    cmd = BatchCancelOrders(TID, SID, IID, cancels, UUID4(), 0)
    asyncio.run(client._batch_cancel_orders(cmd))
    assert session.calls == ["cancel_order:B1", "cancel_order:B2"]
