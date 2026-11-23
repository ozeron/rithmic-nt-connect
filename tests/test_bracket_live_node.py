"""Real rc9 LiveNode + built PyO3 FakeVenue bracket lifecycle, no network."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from _fake_venue import (
    HAS_FAKE,
    INSTRUMENT_ID,
    TS,
    Venues,
    build_node,
    notification,
    pnl_pushes,
    until,
)
from nautilus_trader.model import OrderSide, OrderStatus, Price, Quantity
from nautilus_trader.trading import Strategy

pytestmark = pytest.mark.skipif(not HAS_FAKE, reason="requires testing wheel")


class BracketProbe(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self.orders: list[Any] = []
        self.events: list[Any] = []

    def on_start(self) -> None:
        self.subscribe_trades(INSTRUMENT_ID)

    def on_trade(self, trade: Any) -> None:
        if self.orders:
            return
        self.orders = self.order_factory.bracket(
            instrument_id=INSTRUMENT_ID,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(2),
            entry_tags=["rithmic:bracket_reference=21000"],
            sl_trigger_price=Price.from_str("20990.00"),
            tp_price=Price.from_str("21020.00"),
            tp_post_only=False,
        )
        self.submit_order_list(self.orders)

    def on_order_event(self, event: Any) -> None:
        self.events.append(event)


@pytest.mark.parametrize("exit_path", ["target", "stop", "partial_target_stop"])
def test_native_bracket_live_node(tmp_path: Any, exit_path: str) -> None:
    venues = Venues(tmp_path)
    node = build_node(venues)
    strategy = BracketProbe()
    node.add_strategy(strategy)
    handle = node.handle()
    cache = node.cache

    def current(order: Any) -> Any:
        return cache.order(order.client_order_id)

    def emit(kind: str, index: int, **extra: Any) -> None:
        # Rithmic children inherit entry's localid, and identify their parent
        # by original_basket_id. Every child has its own venue basket id.
        row = notification(
            kind,
            str(strategy.orders[0].client_order_id),
            basket_id=["B", "BS", "BT"][index],
            original_basket_id="B" if index else None,
            linked_basket_ids="BS,BT" if not index else "BT" if index == 1 else "BS",
            transaction_type=1 if not index else 2,
            price_type=2 if not index else 4 if index == 1 else 1,
            duration=2,
            quantity=2,
            price=None if index != 2 else 21020.25,
            trigger_price=20990.25 if index == 1 else None,
            **extra,
        )
        venues.exec.script.emit("order", row)

    async def drive() -> None:
        pusher = asyncio.create_task(pnl_pushes(venues))
        task = asyncio.create_task(node.run_async())
        try:
            await until(lambda: handle.is_running or task.done(), "node running")
            assert not task.done(), task.exception()
            await until(
                lambda: any("subscribe NQH6" in c for c in venues.data.script.calls()),
                "trade subscription",
            )
            venues.data.script.emit(
                "ticker",
                dict(
                    type="last_trade",
                    symbol="NQH6",
                    exchange="CME",
                    trade_price=21000.0,
                    trade_size=2,
                    aggressor=1,
                    ts_event_ns=TS,
                ),
            )
            await until(lambda: strategy.orders, "bracket orders")
            await until(
                lambda: any(
                    "place_bracket_order" in c for c in venues.exec.script.calls()
                ),
                "one native bracket command",
            )
            entry, stop, target = strategy.orders
            for index in range(3):
                emit("accepted", index)
            await until(
                lambda: all(
                    current(o).status == OrderStatus.ACCEPTED for o in strategy.orders
                ),
                "all legs accepted",
            )
            # Rithmic anchored the exits to the venue reference; the rc9 cache
            # carries the venue terms (OrderUpdated), not the submitted ones.
            await until(
                lambda: (
                    str(current(stop).trigger_price) == "20990.25"
                    and str(current(target).price) == "21020.25"
                ),
                "venue-anchored exit terms",
            )
            emit(
                "filled",
                0,
                fill_id="entry",
                fill_size=2,
                total_fill_size=2,
                fill_price=21000.25,
            )
            await until(
                lambda: current(entry).status == OrderStatus.FILLED, "entry fill"
            )
            assert str(current(entry).avg_px) == "21000.25"
            # Submit the actual rc9 ModifyOrder path at breakeven.
            strategy.modify_order(
                stop.client_order_id, trigger_price=Price.from_str("21000.25")
            )
            await until(
                lambda: any(
                    "adjust_bracket_stop B 0 None" in c
                    for c in venues.exec.script.calls()
                ),
                "native stop adjustment",
            )
            if exit_path == "partial_target_stop":
                emit(
                    "filled",
                    2,
                    fill_id="tp-part",
                    fill_size=1,
                    total_fill_size=1,
                    fill_price=21020.25,
                )
                await until(
                    lambda: current(target).status == OrderStatus.PARTIALLY_FILLED,
                    "partial target",
                )
                # The venue resizes the stop. A bracket update is a hint to
                # reconcile; it is not a fabricated stop cancel/fill.
                row = notification(
                    "updated",
                    str(entry.client_order_id),
                    basket_id="BS",
                    original_basket_id="B",
                    transaction_type=2,
                    price_type=4,
                    duration=2,
                    quantity=1,
                    trigger_price=21000.25,
                )
                venues.exec.script.emit("order", row)
                await until(
                    lambda: int(current(stop).quantity) == 1, "venue stop resize"
                )
                emit(
                    "filled",
                    1,
                    fill_id="stop-rest",
                    fill_size=1,
                    total_fill_size=1,
                    fill_price=21000.25,
                )
                emit("canceled", 2)
                await until(
                    lambda: (
                        current(stop).status == OrderStatus.FILLED
                        and current(target).status == OrderStatus.CANCELED
                    ),
                    "remaining stop and OCO cancel",
                )
            else:
                winner, other = (2, 1) if exit_path == "target" else (1, 2)
                emit(
                    "filled",
                    winner,
                    fill_id="exit",
                    fill_size=2,
                    total_fill_size=2,
                    fill_price=21020.25 if winner == 2 else 20990.25,
                )
                emit("canceled", other)
                await until(
                    lambda: (
                        current(strategy.orders[winner]).status == OrderStatus.FILLED
                        and current(strategy.orders[other]).status
                        == OrderStatus.CANCELED
                    ),
                    "exit fill and OCO cancel",
                )
            assert cache.positions_open() == []
            calls = venues.exec.script.calls()
            assert sum("place_bracket_order" in c for c in calls) == 1
            assert not any(c.startswith("place_order ") for c in calls)
            assert not any(c.startswith("modify_order ") for c in calls)
            assert not any(c.startswith("cancel_order ") for c in calls)
        finally:
            handle.stop()
            await asyncio.wait_for(task, 15)
            pusher.cancel()
            await asyncio.gather(pusher, return_exceptions=True)

    asyncio.run(drive())
