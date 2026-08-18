"""Unit tests for the ``OrderDriver`` harness (exec_harness.py).

These pin the double-submission bug caught in Oracle review: a quote-independent
spec (e.g. ``market()``) was submitted by ``on_start`` and then submitted *again*
by the first quote tick, because ``initial`` was never drained. Pure harness
logic — no credentials or TradingNode needed.
"""

from __future__ import annotations

from types import SimpleNamespace

from exec_harness import OrderDriver, OrderDriverConfig
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from order_dsl import market


class _HarnessDriver(OrderDriver):
    """OrderDriver with Nautilus wiring stubbed to count submissions."""

    def __init__(self) -> None:
        instrument = SimpleNamespace(
            id=InstrumentId.from_str("NQU6.RITHMIC"),
            price_increment=SimpleNamespace(as_double=lambda: 0.25),
            price_precision=2,
        )
        super().__init__(
            OrderDriverConfig(instrument_id=str(instrument.id)),
            instrument,
        )
        self.submitted: list[object] = []
        self.order_factory = SimpleNamespace(market=lambda *args, **kwargs: object())

    def subscribe_quote_ticks(
        self,
        instrument_id,
        client_id=None,
        update_catalog=True,
        aggregate_spread_quotes=True,
        params=None,
    ) -> None:
        pass

    def submit_order(
        self, order, position_id=None, client_id=None, params=None
    ) -> None:
        self.submitted.append(order)

    @property
    def order_factory(self):
        # ``order_factory`` is cdef read-only on the Cython ``Strategy`` base;
        # re-expose it as a writable property (same trick as the exec-recon
        # unit doubles) so the ``market()`` DSL spec can build its order.
        # ``object.__getattribute__``/``__setattr__`` keep the dynamic slot
        # invisible to the type checker (no ``type: ignore``).
        return object.__getattribute__(self, "_OrderDriver__factory")

    @order_factory.setter
    def order_factory(self, value) -> None:
        object.__setattr__(self, "_OrderDriver__factory", value)


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
    driver.on_quote_tick(_quote_tick())

    assert len(driver.submitted) == 1


def test_market_dsl_spec_submitted_once() -> None:
    """The real ``market()`` DSL spec is quote-independent — same guarantee."""
    driver = _HarnessDriver()
    driver.initial.append(market(OrderSide.BUY, tif=TimeInForce.GTC))
    driver.on_start()
    driver.on_quote_tick(_quote_tick())

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

    driver.on_quote_tick(_quote_tick(bid=object(), ask=object()))
    assert len(driver.submitted) == 1

    driver.on_quote_tick(_quote_tick(bid=object(), ask=object()))
    assert len(driver.submitted) == 1
