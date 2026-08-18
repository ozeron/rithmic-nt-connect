"""Order-building DSL for the live exec suite (tests/e2e).

A *spec* is a callable ``(strategy, instrument, bid: Price|None, ask: Price|None)
-> Order|None`` — the signature used by ``OrderDriver``. Price-dependent specs
return ``None`` until the first quote tick supplies bid/ask.

The curried ``relative`` price functions are the point of this module: they
make a spec's price/trigger a plain callable, so test case tables read as data
instead of a pile of ``lambda i, b, a: ...`` wrappers.
"""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


def price(instrument, base: float, delta_ticks: int) -> Price:
    """``base`` plus ``delta_ticks`` at the instrument's tick increment."""
    inc = instrument.price_increment.as_double()
    return Price(
        Decimal(str(base)) + Decimal(str(inc)) * Decimal(delta_ticks),
        instrument.price_precision,
    )


def relative(delta_ticks: int):
    """Price fn ``(instrument, bid, ask) -> Price``: bid-relative if negative,
    ask-relative if positive. Returns ``None`` before the first quote tick."""

    def _fn(instrument, bid, ask):
        if bid is None or ask is None:
            return None
        base = bid if delta_ticks < 0 else ask
        return price(instrument, base.as_double(), delta_ticks)

    return _fn


# Named price/trigger fns used by the TC case tables.
below = relative(-500)      # buy limit below bid (rests, no fill)
above = relative(1)         # sell limit above ask / marketable limit
far_below = relative(-500)  # sell stop below bid (rests)
far_above = relative(500)   # buy stop above ask (rests)


def _cid(client_order_id: str | None) -> ClientOrderId | None:
    """Coerce an optional id; ``None`` lets Nautilus generate one."""
    return ClientOrderId(client_order_id) if client_order_id else None


def _quote_gated(build):
    """Turn a ``build(strat, inst, bid, ask)`` into a spec that returns ``None``
    (deferred to the first quote tick) until bid/ask are supplied."""

    def _spec(strat, inst, bid, ask):
        if bid is None or ask is None:
            return None
        return build(strat, inst, bid, ask)

    return _spec


def market(side: OrderSide, qty: str = "1", tif: TimeInForce = TimeInForce.GTC):
    def _spec(strat, inst, bid, ask):
        return strat.order_factory.market(
            inst.id, side, Quantity.from_str(qty), tif
        )

    return _spec


def limit(
    side: OrderSide,
    price_fn,
    tif: TimeInForce = TimeInForce.GTC,
    qty: str = "1",
    client_order_id: str | None = None,
):
    @_quote_gated
    def _build(strat, inst, bid, ask):
        px = price_fn(inst, bid, ask)
        if px is None:
            return None
        return strat.order_factory.limit(
            inst.id,
            side,
            Quantity.from_str(qty),
            px,
            tif,
            client_order_id=_cid(client_order_id),
        )

    return _build


def stop_market(
    side: OrderSide,
    trigger_fn,
    tif: TimeInForce = TimeInForce.GTC,
    qty: str = "1",
    client_order_id: str | None = None,
):
    @_quote_gated
    def _build(strat, inst, bid, ask):
        trigger = trigger_fn(inst, bid, ask)
        if trigger is None:
            return None
        return strat.order_factory.stop_market(
            inst.id,
            side,
            Quantity.from_str(qty),
            trigger,
            time_in_force=tif,
            client_order_id=_cid(client_order_id),
        )

    return _build


def stop_limit(
    side: OrderSide,
    trigger_fn,
    limit_fn,
    tif: TimeInForce = TimeInForce.GTC,
    qty: str = "1",
    client_order_id: str | None = None,
):
    @_quote_gated
    def _build(strat, inst, bid, ask):
        trigger = trigger_fn(inst, bid, ask)
        px = limit_fn(inst, bid, ask)
        if trigger is None or px is None:
            return None
        return strat.order_factory.stop_limit(
            inst.id,
            side,
            Quantity.from_str(qty),
            px,
            trigger,
            time_in_force=tif,
            client_order_id=_cid(client_order_id),
        )

    return _build
