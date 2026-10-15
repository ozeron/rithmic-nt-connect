"""Plant brackets are all-or-nothing: every leg gets the same events, and a list
that cannot be sent whole is denied whole."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from _stubs import WireSessionStub
from nautilus_trader.model.enums import OrderSide, OrderType, TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId, StrategyId
from test_exec_transport_e2e import _trading_client

NQ = InstrumentId.from_str("NQU6-CME.RITHMIC")


def _leg(
    cid: str,
    side: OrderSide,
    order_type: OrderType,
    *,
    price: float | None = None,
    trigger: float | None = None,
    parent: str | None = None,
    linked: list[str] | None = None,
) -> Any:
    return SimpleNamespace(
        client_order_id=ClientOrderId(cid),
        strategy_id=StrategyId("S-001"),
        instrument_id=NQ,
        side=side,
        order_type=order_type,
        time_in_force=TimeInForce.DAY,
        quantity=1,
        price=price,
        has_price=price is not None,
        trigger_price=trigger,
        has_trigger_price=trigger is not None,
        parent_order_id=ClientOrderId(parent) if parent else None,
        linked_order_ids=[ClientOrderId(x) for x in (linked or [])],
    )


def _bracket(stop: float = 20990.0, target: float = 21020.0) -> list[Any]:
    return [
        _leg("E", OrderSide.BUY, OrderType.LIMIT, price=21000.0, linked=["SL", "TP"]),
        _leg("SL", OrderSide.SELL, OrderType.STOP_MARKET, trigger=stop, parent="E"),
        _leg("TP", OrderSide.SELL, OrderType.LIMIT, price=target, parent="E"),
    ]


class _Rejected(RuntimeError):
    """A failed command carrying the venue-reject class code."""

    code = "venue_rejected"


class _BracketSession(WireSessionStub):
    def __init__(self, fail: BaseException | None = None) -> None:
        self.placed: list[tuple[Any, ...]] = []
        self.fail = fail

    def place_bracket_order(self, *args: Any) -> None:
        if self.fail is not None:
            raise self.fail
        self.placed.append(args)


def _client(session: _BracketSession, monkeypatch: pytest.MonkeyPatch) -> Any:
    client: Any = _trading_client(session)
    client._allow_brackets = True
    monkeypatch.setattr(
        client._cache,
        "instrument",
        lambda _iid: SimpleNamespace(
            price_increment=0.25,
            info={"rithmic_symbol": "NQU6", "rithmic_exchange": "CME"},
        ),
        raising=False,
    )
    events: list[tuple[str, str]] = []
    for name in ("submitted", "denied", "rejected"):
        monkeypatch.setattr(
            client,
            f"generate_order_{name}",
            lambda *a, _n=name: events.append((_n, str(a[2]))),
        )
    client.events = events
    return client


def _submit(client: Any, orders: list[Any]) -> None:
    command = SimpleNamespace(
        order_list=SimpleNamespace(orders=orders),
        trader_id=None,
        strategy_id=StrategyId("S-001"),
    )
    asyncio.run(client._submit_order_list(command))


def test_every_leg_is_submitted(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _BracketSession()
    client = _client(session, monkeypatch)
    _submit(client, _bracket())
    assert len(session.placed) == 1
    assert sorted(client.events) == [
        ("submitted", "E"),
        ("submitted", "SL"),
        ("submitted", "TP"),
    ]


def test_a_venue_reject_rejects_every_leg(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _BracketSession(fail=_Rejected("bad price"))
    client = _client(session, monkeypatch)
    _submit(client, _bracket())
    rejected = sorted(cid for kind, cid in client.events if kind == "rejected")
    assert rejected == ["E", "SL", "TP"]


def test_a_stop_that_rounds_to_zero_ticks_denies_the_whole_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _BracketSession()
    client = _client(session, monkeypatch)
    _submit(client, _bracket(stop=21000.0))  # zero ticks from the entry
    assert session.placed == [], "nothing sent without the stop"
    assert sorted(client.events) == [
        ("denied", "E"),
        ("denied", "SL"),
        ("denied", "TP"),
    ]


def test_unlinked_orders_with_mixed_sides_are_not_a_bracket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _BracketSession()
    client = _client(session, monkeypatch)
    orders = [
        _leg("A", OrderSide.BUY, OrderType.LIMIT, price=1.0),
        _leg("B", OrderSide.SELL, OrderType.LIMIT, price=2.0),
        _leg("C", OrderSide.SELL, OrderType.LIMIT, price=3.0),
    ]
    assert client._is_bracket_order_list(orders) is False
