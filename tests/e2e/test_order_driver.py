"""Offline tests for the live exec harness (exec_harness.py).

The ``OrderDriver`` unit tests pin the double-submission bug caught in Oracle
review: a quote-independent spec (e.g. ``market()``) was submitted by
``on_start`` and then submitted *again* by the first quote tick, because
``initial`` was never drained. ``test_harness_drives_an_order_over_the_fake_venue``
runs the whole harness (node thread, proxies, report pulls, shutdown) against
the Rust ``FakeVenue`` — no credentials.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest
from _fake_venue import (
    HAS_FAKE,
    INSTRUMENT_ID,
    TS,
    Venues,
    build_node,
    notification,
)
from exec_harness import ExecHarness, NodeThreadError, OrderDriver
from nautilus_trader.model import (
    ClientOrderId,
    InstrumentId,
    OrderSide,
    OrderStatus,
    Price,
    TimeInForce,
)
from order_dsl import below, limit, market


class _HarnessDriver(OrderDriver):
    """OrderDriver with Nautilus wiring stubbed to count submissions."""

    def __init__(self) -> None:
        instrument = SimpleNamespace(
            id=InstrumentId.from_str("NQU6.RITHMIC"),
            price_increment=SimpleNamespace(as_double=lambda: 0.25),
            price_precision=2,
        )
        super().__init__(instrument)
        self.submitted: list[object] = []
        self.stub_factory = SimpleNamespace(market=lambda *args, **kwargs: object())

    def subscribe_quotes(self, *args: Any, **kwargs: Any) -> None:
        pass  # Stub: not exercised by submit-book tests.

    def submit_order(self, order: Any, *args: Any, **kwargs: Any) -> None:
        self.submitted.append(order)

    @property
    def order_factory(self) -> Any:  # read-only on the native ``Strategy`` base
        return self.stub_factory


def _quote_tick(*, bid=None, ask=None) -> SimpleNamespace:
    return SimpleNamespace(bid_price=bid, ask_price=ask)


def test_quote_independent_spec_submitted_once() -> None:
    """A spec that never needs a quote must be submitted by ``on_start`` and NOT
    again by the first quote tick."""
    driver = _HarnessDriver()

    def spec(strat, inst, bid, ask):
        return object()

    driver.initial.append(spec)
    driver.on_start()
    driver.on_quote(_quote_tick())

    assert len(driver.submitted) == 1


def test_market_dsl_spec_submitted_once() -> None:
    """The real ``market()`` DSL spec is quote-independent — same guarantee."""
    driver = _HarnessDriver()
    driver.initial.append(market(OrderSide.BUY, tif=TimeInForce.GTC))
    driver.on_start()
    driver.on_quote(_quote_tick())

    assert len(driver.submitted) == 1


def test_quote_dependent_spec_deferred_to_first_quote_only() -> None:
    """A quote-dependent spec stays pending through ``on_start``, submits on the
    first quote, and must NOT submit again on later quotes."""
    driver = _HarnessDriver()

    def spec(strat, inst, bid, ask):
        return None if bid is None else object()

    driver.initial.append(spec)
    driver.on_start()
    assert driver.submitted == []

    driver.on_quote(_quote_tick(bid=object(), ask=object()))
    assert len(driver.submitted) == 1

    driver.on_quote(_quote_tick(bid=object(), ask=object()))
    assert len(driver.submitted) == 1


@pytest.fixture
def pnl_pushes(tmp_path: Any) -> Iterator[Venues]:
    """FakeVenue sessions plus a PnL stream (the order plant re-arms on it)."""
    venues = Venues(tmp_path)
    stop = threading.Event()

    def push() -> None:
        while not stop.wait(0.1):
            venues.exec.script.emit(
                "pnl",
                {
                    "type": "account_pnl",
                    "account_id": "ACC1",
                    "fcm_id": "FCM",
                    "ib_id": "IB",
                    "account_balance": "50000.0",
                    "cash_on_hand": "50000.0",
                },
            )

    pusher = threading.Thread(target=push, daemon=True)
    pusher.start()
    try:
        yield venues
    finally:
        stop.set()
        pusher.join(timeout=5)


def _called(session: Any, prefix: str) -> bool:
    return any(c.startswith(prefix) for c in session.script.calls())


def _wait(predicate: Any, what: str, timeout: float = 10.0) -> None:
    done = threading.Event()
    deadline = timeout
    while not predicate():
        if deadline <= 0:
            raise AssertionError(f"timed out waiting for {what}")
        done.wait(0.05)
        deadline -= 0.05


@pytest.mark.skipif(not HAS_FAKE, reason="extension built without testing")
def test_harness_drives_an_order_over_the_fake_venue(pnl_pushes: Venues) -> None:
    venues = pnl_pushes
    instrument = SimpleNamespace(
        id=INSTRUMENT_ID,
        price_increment=SimpleNamespace(as_double=lambda: 0.25),
        price_precision=2,
    )
    harness = ExecHarness(lambda register: build_node(venues, register), instrument)
    cid = ClientOrderId("H-1")
    harness.initial.append(
        limit(OrderSide.BUY, below, TimeInForce.GTC, client_order_id=cid.value)
    )
    try:
        harness.start()
        _wait(
            lambda: any(
                c.startswith("subscribe NQH6") for c in venues.data.script.calls()
            ),
            "quote subscription",
        )
        venues.data.script.emit(
            "ticker",
            {
                "type": "bbo",
                "symbol": "NQH6",
                "exchange": "CME",
                "bid_price": 21000.0,
                "bid_size": 1,
                "ask_price": 21000.25,
                "ask_size": 1,
                "ts_event_ns": TS,
            },
        )
        harness.wait_event("OrderSubmitted", client_order_id=cid)
        venues.exec.script.emit("order", notification("accepted", cid.value))
        harness.wait_for_venue_outcome("OrderAccepted", client_order_id=cid)

        order = harness.cache.order(cid)
        assert order is not None and order.status == OrderStatus.ACCEPTED
        # ``below``: 500 ticks of 0.25 under the 21000.00 bid.
        assert order.price == Price.from_str("20875.00")
        # The scripted venue drains no working orders: an open-only check is
        # advisory and reports nothing rather than failing.
        assert harness.drain_open_orders() == []
        report = harness.order_status_report(cid)
        assert report is not None and report.order_status == OrderStatus.ACCEPTED

        harness.cancel(cid)
        _wait(
            lambda: any(
                c.startswith("cancel_order") for c in venues.exec.script.calls()
            ),
            "venue cancel",
        )
        venues.exec.script.emit(
            "order", notification("canceled", cid.value, ts_event_ns=TS + 1)
        )
        harness.wait_order_status(cid, OrderStatus.CANCELED)
        assert harness.event_types() == [
            "OrderInitialized",
            "OrderSubmitted",
            "OrderAccepted",
            "OrderCanceled",
        ]
        harness.stop_and_wait()
        # The cache stays readable after stop (served by the node thread).
        assert harness.cache.order(cid).status == OrderStatus.CANCELED
    finally:
        harness.shutdown()
    assert any(c.startswith("place_order") for c in venues.exec.script.calls())


@pytest.mark.skipif(not HAS_FAKE, reason="extension built without testing")
def test_node_thread_errors_carry_no_node_frames(tmp_path: Any) -> None:
    """A failure on the node thread reaches the caller as text only: no chained
    exception whose frames could drop node objects on this thread."""
    venues = Venues(tmp_path)
    instrument = SimpleNamespace(
        id=INSTRUMENT_ID,
        price_increment=SimpleNamespace(as_double=lambda: 0.25),
        price_precision=2,
    )
    harness = ExecHarness(lambda register: build_node(venues, register), instrument)

    def boom() -> None:
        raise ValueError("boom")

    try:
        harness.start()
        with pytest.raises(NodeThreadError, match="ValueError: boom") as caught:
            harness.call(boom)
        assert caught.value.__context__ is None
        assert caught.value.__cause__ is None
    finally:
        harness.shutdown()
