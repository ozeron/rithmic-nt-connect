"""NautilusTrader 2 ``LiveNode`` end-to-end over the Rust ``FakeVenue`` (no creds).

Drives the real factories, ``RithmicDataClient`` and ``RithmicExecutionClient``
inside a ``LiveNode`` whose sessions are ``_lib.Session.fake`` (the same session
host and plant code as production, with a scripted venue). Proves the v2 client
contract end to end: instruments, quotes/trades/EXTERNAL bars, a completed bar
history request, AccountState before connect returns, submit → venue accept →
venue fill → filled order in the core cache, position reconciliation, the
exposure hub, and clean shutdown.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from _fake_venue import (
    BAR_TYPE,
    HAS_FAKE,
    INSTRUMENT_ID,
    TS,
    Venues,
    build_node,
    notification,
    pnl_pushes,
    until,
)
from nautilus_trader.model import (
    AccountId,
    OrderSide,
    OrderStatus,
    OrderStatusReport,
    Price,
    Quantity,
)
from nautilus_trader.trading import Strategy
from rithmic_nt_connect.exposure import EXPOSURE_HUB

pytestmark = pytest.mark.skipif(not HAS_FAKE, reason="extension built without testing")


class _Probe(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self.quotes: list[Any] = []
        self.trades: list[Any] = []
        self.bars: list[Any] = []
        self.history_done: list[str] = []
        self.order: Any = None
        self.events: list[str] = []
        self.fills: list[Any] = []

    def on_start(self) -> None:
        self.subscribe_quotes(INSTRUMENT_ID)
        self.subscribe_trades(INSTRUMENT_ID)
        self.subscribe_bars(BAR_TYPE)
        end = datetime.now(UTC)
        self.request_bars(
            BAR_TYPE,
            start=end - timedelta(minutes=30),
            end=end,
            callback=self.history_done.append,
        )

    def on_quote(self, quote: Any) -> None:
        self.quotes.append(quote)

    def on_trade(self, trade: Any) -> None:
        self.trades.append(trade)
        if self.order is None:
            self.order = self.order_factory.limit(
                INSTRUMENT_ID,
                OrderSide.BUY,
                Quantity.from_int(1),
                Price.from_str("21000.00"),
            )
            self.submit_order(self.order)

    def on_bar(self, bar: Any) -> None:
        self.bars.append(bar)

    def on_order_submitted(self, event: Any) -> None:
        self.events.append("submitted")

    def on_order_accepted(self, event: Any) -> None:
        self.events.append("accepted")

    def on_order_filled(self, event: Any) -> None:
        self.events.append("filled")
        self.fills.append(event)


def test_live_node_runs_rithmic_clients_end_to_end(tmp_path: Any) -> None:
    venues = Venues(tmp_path)
    node = build_node(venues)
    strategy = _Probe()
    node.add_strategy(strategy)
    handle = node.handle()
    cache = node.cache
    account_id = AccountId("RITHMIC-ACC1")
    results: dict[str, Any] = {}

    async def drive() -> None:
        pusher = asyncio.create_task(pnl_pushes(venues))
        task = asyncio.create_task(node.run_async())
        await until(lambda: handle.is_running or task.done(), "node running")
        assert not task.done(), task.exception()
        await until(lambda: strategy.history_done, "bar history completion")
        assert cache.instrument(INSTRUMENT_ID) is not None
        assert cache.account(account_id) is not None
        # Subscribe is an async intent: let it reach the venue before emitting.
        await until(
            lambda: _subscribed(venues, "subscribe NQH6", "subscribe_time_bars"),
            "market-data subscriptions",
        )

        venues.data.script.emit(
            "ticker",
            {
                "type": "bbo",
                "symbol": "NQH6",
                "exchange": "CME",
                "bid_price": 20999.75,
                "bid_size": 3,
                "ask_price": 21000.25,
                "ask_size": 4,
                "ts_event_ns": TS,
            },
        )
        venues.data.script.emit(
            "ticker",
            {
                "type": "last_trade",
                "symbol": "NQH6",
                "exchange": "CME",
                "trade_price": 21000.0,
                "trade_size": 2,
                "aggressor": 1,
                "ts_event_ns": TS,
            },
        )
        venues.data.script.emit(
            "history",
            {
                "type": "time_bar",
                "symbol": "NQH6",
                "exchange": "CME",
                "bar_type": 2,
                "period": "60",
                "open_price": 21000.0,
                "high_price": 21001.0,
                "low_price": 20999.0,
                "close_price": 21000.5,
                "volume": 42,
                "ts_event_ns": TS,
            },
        )
        await until(lambda: strategy.order is not None, "order submitted")
        await until(lambda: "submitted" in strategy.events, "OrderSubmitted")
        tag = strategy.order.client_order_id.value
        venues.exec.script.emit("order", notification("accepted", tag))
        await until(lambda: "accepted" in strategy.events, "OrderAccepted")
        venues.exec.script.emit(
            "order",
            notification(
                "filled",
                tag,
                fill_id="F1",
                fill_size=1,
                fill_price=21000.0,
                total_fill_size=1,
                ssboe=TS // 1_000_000_000,
            ),
        )
        await until(lambda: "filled" in strategy.events, "OrderFilled")
        venues.exec.script.emit(
            "pnl",
            {
                "type": "instrument_pnl",
                "symbol": "NQH6",
                "exchange": "CME",
                "account_id": "ACC1",
                "net_quantity": 1,
            },
        )
        await until(lambda: EXPOSURE_HUB.has_responder, "exposure responder registered")
        # The PnL row is applied by the exec poll loop; wait until it lands.
        deadline = asyncio.get_running_loop().time() + 10
        while True:
            results["exposure"] = await EXPOSURE_HUB.request("NQ")
            if results["exposure"].net == 1:
                break
            if asyncio.get_running_loop().time() > deadline:
                break
            await asyncio.sleep(0.02)
        await until(lambda: strategy.bars, "EXTERNAL bar")
        handle.stop()
        await asyncio.wait_for(task, 15)
        pusher.cancel()

    asyncio.run(drive())

    assert strategy.quotes and strategy.quotes[0].bid_price == Price.from_str(
        "20999.75"
    )
    assert strategy.trades and strategy.trades[0].size == Quantity.from_int(2)
    assert strategy.bars[0].close == Price.from_str("21000.50")
    assert strategy.events == ["submitted", "accepted", "filled"]
    fill = strategy.fills[0]
    assert fill.account_id == account_id
    assert fill.last_px == Price.from_str("21000.00")
    assert fill.last_qty == Quantity.from_int(1)
    order = cache.order(strategy.order.client_order_id)
    assert order is not None
    assert order.status == OrderStatus.FILLED
    assert cache.client_order_id(order.venue_order_id) == order.client_order_id
    exposure = results["exposure"]
    assert exposure.root == "NQ" and exposure.net == 1 and exposure.open_orders == 0
    # Shutdown unregistered the exec client and joined its tasks.
    assert not EXPOSURE_HUB.has_responder
    calls = venues.exec.script.calls()
    assert any(c.startswith("place_order") for c in calls)
    assert any(c.startswith("subscribe_time_bars") for c in venues.data.script.calls())


def _subscribed(venues: Venues, *prefixes: str) -> bool:
    calls = venues.data.script.calls()
    return all(any(c.startswith(p) for c in calls) for p in prefixes)


class _CancelProbe(_Probe):
    def on_order_canceled(self, event: Any) -> None:
        self.events.append("canceled")


def test_stale_accepted_report_does_not_regress_a_canceled_order(
    tmp_path: Any,
) -> None:
    """The MY043-001 2026-08-21 hazard on the v2 engine: a working-orders drain
    row taken before a cancel propagated reports ACCEPTED for a leg the venue
    already canceled. The engine must not move the order back to ACCEPTED."""
    venues = Venues(tmp_path)
    node = build_node(venues)
    strategy = _CancelProbe()
    node.add_strategy(strategy)
    handle = node.handle()
    cache = node.cache

    async def drive() -> None:
        pusher = asyncio.create_task(pnl_pushes(venues))
        task = asyncio.create_task(node.run_async())
        await until(lambda: handle.is_running or task.done(), "node running")
        await until(lambda: _subscribed(venues, "subscribe NQH6"), "trade subscription")
        venues.data.script.emit(
            "ticker",
            {
                "type": "last_trade",
                "symbol": "NQH6",
                "exchange": "CME",
                "trade_price": 21000.0,
                "trade_size": 1,
                "aggressor": 1,
                "ts_event_ns": TS,
            },
        )
        await until(lambda: "submitted" in strategy.events, "OrderSubmitted")
        tag = strategy.order.client_order_id.value
        venues.exec.script.emit("order", notification("accepted", tag))
        await until(lambda: "accepted" in strategy.events, "OrderAccepted")
        venues.exec.script.emit(
            "order",
            notification("canceled", tag, status="CANCELLED", ts_event_ns=TS + 1),
        )
        await until(lambda: "canceled" in strategy.events, "OrderCanceled")
        client = venues.exec_client
        assert client is not None
        order = cache.order(strategy.order.client_order_id)
        stale = client._order_status_report_for(order, client.clock.timestamp_ns())
        assert stale is not None
        stale = OrderStatusReport(
            account_id=stale.account_id,
            instrument_id=stale.instrument_id,
            venue_order_id=stale.venue_order_id,
            order_side=stale.order_side,
            order_type=stale.order_type,
            time_in_force=stale.time_in_force,
            order_status=OrderStatus.ACCEPTED,
            quantity=stale.quantity,
            filled_qty=stale.filled_qty,
            ts_accepted=TS,
            ts_last=TS,
            ts_init=client.clock.timestamp_ns(),
            client_order_id=stale.client_order_id,
            price=stale.price,
        )
        client._send_order_status_report(stale)
        await asyncio.sleep(0.3)
        handle.stop()
        await asyncio.wait_for(task, 15)
        pusher.cancel()

    asyncio.run(drive())

    order = cache.order(strategy.order.client_order_id)
    assert order is not None
    assert order.status == OrderStatus.CANCELED, "stale ACCEPTED must not regress"
    assert strategy.events == ["submitted", "accepted", "canceled"]


class _QueryProbe(_CancelProbe):
    def query(self) -> None:
        self.query_order(self.order)


def test_query_order_reaches_the_client_and_publishes_a_report(
    tmp_path: Any,
) -> None:
    """``Strategy.query_order`` is routed to ``_query_order`` on NautilusTrader 2.

    An in-flight order the venue drain cannot resolve stays SUBMITTED (fail
    closed: no fabricated state); once accepted, the query publishes a status
    report the engine applies without disturbing the order.
    """
    venues = Venues(tmp_path)
    node = build_node(venues)
    strategy = _QueryProbe()
    node.add_strategy(strategy)
    handle = node.handle()
    cache = node.cache

    async def drive() -> None:
        pusher = asyncio.create_task(pnl_pushes(venues))
        task = asyncio.create_task(node.run_async())
        await until(lambda: handle.is_running or task.done(), "node running")
        await until(lambda: _subscribed(venues, "subscribe NQH6"), "trade subscription")
        venues.data.script.emit(
            "ticker",
            {
                "type": "last_trade",
                "symbol": "NQH6",
                "exchange": "CME",
                "trade_price": 21000.0,
                "trade_size": 1,
                "aggressor": 1,
                "ts_event_ns": TS,
            },
        )
        await until(lambda: "submitted" in strategy.events, "OrderSubmitted")
        drains_before = sum(
            c.startswith("load_orders") for c in venues.exec.script.calls()
        )
        strategy.query()
        await until(
            lambda: (
                sum(c.startswith("load_orders") for c in venues.exec.script.calls())
                > drains_before
            ),
            "in-flight query drains the venue",
        )
        await asyncio.sleep(0.2)
        order = cache.order(strategy.order.client_order_id)
        assert order is not None
        assert order.status == OrderStatus.SUBMITTED

        tag = strategy.order.client_order_id.value
        venues.exec.script.emit("order", notification("accepted", tag))
        await until(lambda: "accepted" in strategy.events, "OrderAccepted")
        strategy.query()
        await asyncio.sleep(0.3)
        handle.stop()
        await asyncio.wait_for(task, 15)
        pusher.cancel()

    asyncio.run(drive())

    order = cache.order(strategy.order.client_order_id)
    assert order is not None
    assert order.status == OrderStatus.ACCEPTED
    assert strategy.events == ["submitted", "accepted"]
