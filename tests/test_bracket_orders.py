"""Plant brackets are all-or-nothing: every leg gets the same events, and a list
that cannot be sent whole is denied whole."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from _stubs import WireSessionStub
from nautilus_trader.model import (
    AccountId,
    ClientOrderId,
    InstrumentId,
    OrderSide,
    OrderStatus,
    OrderType,
    Quantity,
    StrategyId,
    TimeInForce,
    VenueOrderId,
)
from rithmic_nt_connect._orders import order_notification_to_fields
from rithmic_nt_connect.brackets import brackets_for
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
        tags=[],
        status=OrderStatus.SUBMITTED,
        is_closed=False,
        filled_qty=Quantity.from_int(0),
        leaves_qty=Quantity.from_int(1),
        avg_px=None,
        venue_order_id=None,
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
        _leg(
            "SL",
            OrderSide.SELL,
            OrderType.STOP_MARKET,
            trigger=stop,
            parent="E",
            linked=["TP"],
        ),
        _leg(
            "TP",
            OrderSide.SELL,
            OrderType.LIMIT,
            price=target,
            parent="E",
            linked=["SL"],
        ),
    ]


class _Rejected(RuntimeError):
    """A failed command carrying the venue-reject class code."""

    code = "venue_rejected"


class _BracketSession(WireSessionStub):
    def __init__(self, fail: BaseException | None = None) -> None:
        self.placed: list[tuple[Any, ...]] = []
        self.fail = fail
        self.adjusted: list[tuple[Any, ...]] = []
        self.rows: list[dict[str, Any]] = []

    def adjust_bracket_stop(self, *args: Any) -> None:
        self.adjusted.append(("stop", *args))

    def adjust_bracket_target(self, *args: Any) -> None:
        self.adjusted.append(("target", *args))

    def load_orders(self, *args: Any) -> list[dict[str, Any]]:
        return self.rows

    def place_bracket_order(self, *args: Any) -> None:
        if self.fail is not None:
            raise self.fail
        self.placed.append(args)


def _client(session: _BracketSession, monkeypatch: pytest.MonkeyPatch) -> Any:
    client: Any = _trading_client(session)
    monkeypatch.setattr(
        client._cache,
        "instrument",
        lambda _iid: SimpleNamespace(
            price_increment=0.25,
            price_precision=2,
            info={"rithmic_symbol": "NQU6", "rithmic_exchange": "CME"},
        ),
        raising=False,
    )
    events: list[tuple[str, str]] = []
    for name in ("submitted", "denied", "rejected"):
        monkeypatch.setattr(
            client,
            f"generate_order_{name}",
            lambda *a, _n=name: events.append((_n, str(a[0].client_order_id))),
        )
    client.events = events
    return client


def _submit(client: Any, orders: list[Any]) -> None:
    # A v2 OrderList holds client order ids; the orders live in the cache.
    for order in orders:
        client._cache._orders[order.client_order_id.value] = order
    command = SimpleNamespace(
        order_list=SimpleNamespace(
            client_order_ids=lambda: [o.client_order_id for o in orders]
        ),
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


@pytest.mark.parametrize("market", [True, False])
def test_market_and_marketable_entry_reference(monkeypatch, market):
    session = _BracketSession()
    client = _client(session, monkeypatch)
    orders = _bracket()
    orders[0].tags = ["rithmic:bracket_reference=21000.25"]
    if market:
        orders[0].order_type = OrderType.MARKET
        orders[0].price = None
        orders[0].has_price = False
    _submit(client, orders)
    assert session.placed[0][-2:] == (41, 79)
    assert session.placed[0][6] == (None if market else 21000.0)


@pytest.mark.parametrize(
    "bad",
    [
        "fraction",
        "side",
        "quantity",
        "stop_limit",
        "market_no_reference",
        "nonfinite",
        "bad_reference",
        "post_only",
        "tif",
    ],
)
def test_unrepresentable_denied_before_send(monkeypatch, bad):
    session = _BracketSession()
    client = _client(session, monkeypatch)
    orders = _bracket()
    if bad == "fraction":
        orders[1].trigger_price = 20990.1
    if bad == "side":
        orders[1].trigger_price = 21001
    if bad == "quantity":
        orders[2].quantity = 0.5
    if bad == "stop_limit":
        orders[1].order_type = OrderType.STOP_LIMIT
    if bad == "market_no_reference":
        orders[0].order_type = OrderType.MARKET
        orders[0].has_price = False
        orders[0].price = None
    if bad == "nonfinite":
        orders[0].tags = ["rithmic:bracket_reference=NaN"]
    if bad == "bad_reference":
        orders[0].tags = ["rithmic:bracket_reference=bad"]
    if bad == "post_only":
        orders[2].is_post_only = True
    if bad == "tif":
        orders[2].time_in_force = TimeInForce.GTC
    _submit(client, orders)
    assert not session.placed
    assert sorted(client.events) == [
        ("denied", "E"),
        ("denied", "SL"),
        ("denied", "TP"),
    ]


def _notification(cid, kind, *, qty=2, filled=0, fill_qty=None, fill_id=None):
    return dict(
        type="order_notification",
        source="exchange" if kind == "filled" else "rithmic",
        kind=kind,
        basket_id={"E": "B", "SL": "BS", "TP": "BT"}[cid],
        original_basket_id="B" if cid != "E" else None,
        linked_basket_ids="BT" if cid == "SL" else "BS" if cid == "TP" else "BS,BT",
        user_tag="E",
        account_id="A",
        symbol="NQU6",
        exchange="CME",
        transaction_type=1 if cid == "E" else 2,
        price_type=4 if cid == "SL" else 1,
        duration=1,
        quantity=qty,
        total_fill_size=filled,
        total_unfilled_size=qty - filled,
        fill_size=fill_qty,
        price=21020 if cid == "TP" else 21000.25,
        trigger_price=20990.25 if cid == "SL" else None,
        avg_fill_price=21000.25 if cid == "E" and filled else None,
        fill_price=21000.25 if cid == "E" else 21020.25 if cid == "TP" else 20990.25,
        fill_id=fill_id,
        ts_event_ns=1_000_000_000,
    )


def _lifecycle_client(monkeypatch):
    session = _BracketSession()
    client = _client(session, monkeypatch)
    client.account_id = AccountId("RITHMIC-A")
    orders = _bracket()
    for o in orders:
        o.quantity = Quantity.from_int(2)
        o.leaves_qty = Quantity.from_int(2)
    _submit(client, orders)
    client.events.clear()

    def accept(order, venue, ts):
        order.status = OrderStatus.ACCEPTED
        client.events.append(("accepted", str(order.client_order_id)))

    def cancel(order, venue, ts):
        order.status = OrderStatus.CANCELED
        order.is_closed = True
        client.events.append(("canceled", str(order.client_order_id)))

    def fill(order, venue, pos, trade, qty, price, *args):
        order.filled_qty = Quantity.from_int(int(order.filled_qty) + int(qty))
        order.leaves_qty = Quantity.from_int(
            int(order.quantity) - int(order.filled_qty)
        )
        order.avg_px = price
        order.status = (
            OrderStatus.FILLED if not order.leaves_qty else OrderStatus.PARTIALLY_FILLED
        )
        order.is_closed = order.status == OrderStatus.FILLED
        client.events.append(("filled", str(order.client_order_id)))

    def update(order, venue, qty, price, trigger, *args):
        # Venue-anchored exit terms land on the cached order, as OrderUpdated would.
        order.price, order.trigger_price = price, trigger

    monkeypatch.setattr(client, "generate_order_accepted", accept)
    monkeypatch.setattr(client, "generate_order_canceled", cancel)
    monkeypatch.setattr(client, "generate_order_filled", fill)
    monkeypatch.setattr(client, "generate_order_updated", update)
    for cid in ["E", "SL", "TP"]:
        client._dispatch_order_event(_notification(cid, "accepted"))
    client._dispatch_order_event(
        _notification("E", "filled", filled=2, fill_qty=2, fill_id="entry")
    )
    return client, session, orders


@pytest.mark.parametrize("winner,other", [("TP", "SL"), ("SL", "TP")])
def test_full_exit_and_venue_oco_cancel(monkeypatch, winner, other):
    client, _session, orders = _lifecycle_client(monkeypatch)
    client._dispatch_order_event(
        _notification(winner, "filled", filled=2, fill_qty=2, fill_id="exit")
    )
    client._dispatch_order_event(_notification(other, "canceled"))
    assert ("filled", winner) in client.events
    assert ("canceled", other) in client.events
    assert ("filled", other) not in client.events
    assert client._venue_id_for_order(orders[0]) == "B"
    assert client._venue_id_for_order(orders[1]) == "BS"
    assert client._venue_id_for_order(orders[2]) == "BT"


def test_partial_target_then_stop_and_bracket_refresh(monkeypatch):
    async def run():
        # Setup outside this coroutine: _submit drives its own event loop.
        client._dispatch_order_event(
            _notification("TP", "filled", filled=1, fill_qty=1, fill_id="partial")
        )
        row = _notification("SL", "updated", qty=1)
        session.rows = [row]
        reports = []
        monkeypatch.setattr(client, "_send_order_status_report", reports.append)
        client._dispatch_order_event(
            dict(
                type="bracket_update",
                basket_id="B",
                stop_quantity=2,
                stop_quantity_released=2,
                target_quantity=2,
                target_quantity_released=2,
            )
        )
        task = brackets_for(client).refresh_task
        assert task is not None
        await task
        assert len(reports) == 1
        assert int(reports[0].quantity) == 1
        assert str(reports[0].client_order_id) == "SL"
        # Mimic the engine applying the venue report; the release counters
        # deliberately differ from leaves and must not set stop quantity.
        orders[1].quantity = reports[0].quantity
        orders[1].leaves_qty = reports[0].quantity
        client._dispatch_order_event(
            _notification("SL", "filled", qty=1, filled=1, fill_qty=1, fill_id="stop")
        )
        client._dispatch_order_event(_notification("TP", "canceled"))
        assert client.events.count(("filled", "TP")) == 1
        assert client.events.count(("filled", "SL")) == 1
        assert ("canceled", "TP") in client.events

    client, session, orders = _lifecycle_client(monkeypatch)
    asyncio.run(run())


@pytest.mark.parametrize(
    "cid,price,want",
    [
        ("SL", 21000.25, ("stop", "B", 0, None)),
        ("SL", 21000.50, ("stop", "B", -1, None)),
        ("TP", 21001.25, ("target", "B", 4, None)),
    ],
)
def test_modify_uses_actual_fill_and_parent_basket(monkeypatch, cid, price, want):
    client, session, _orders = _lifecycle_client(monkeypatch)
    command = SimpleNamespace(
        client_order_id=ClientOrderId(cid),
        venue_order_id=None,
        instrument_id=NQ,
        quantity=None,
        price=price if cid == "TP" else None,
        trigger_price=price if cid == "SL" else None,
    )
    asyncio.run(client._modify_order(command))
    assert session.adjusted == [want]


def test_modify_unknown_fill_refused(monkeypatch):
    session = _BracketSession()
    client = _client(session, monkeypatch)
    _submit(client, _bracket())
    reasons = []
    monkeypatch.setattr(
        client,
        "_modify_rejected",
        lambda command, venue, reason: reasons.append(reason),
    )
    command = SimpleNamespace(
        client_order_id=ClientOrderId("SL"),
        venue_order_id=None,
        instrument_id=NQ,
        quantity=None,
        price=None,
        trigger_price=21000,
    )
    asyncio.run(client._modify_order(command))
    assert not session.adjusted
    assert "fill unknown" in reasons[0]


def test_restart_recovers_child_identity_and_report_linkage(monkeypatch):
    client, _session, _orders = _lifecycle_client(monkeypatch)
    # Lose adapter-local state, retain only the NT persisted cache/orders.
    del client._native_brackets
    client._venue_ids.clear()
    client._client_ids.clear()
    fields = order_notification_to_fields(_notification("SL", "accepted"))
    row = client._drain_row_from_fields(fields, 1_000_000_000)
    assert str(row.report.client_order_id) == "SL"
    assert str(row.report.parent_order_id) == "E"
    assert [str(cid) for cid in row.report.linked_order_ids] == ["TP"]
    assert row.report.order_type == OrderType.STOP_MARKET
    assert brackets_for(client).groups["E"].basket == "B"


@pytest.mark.parametrize("shape", ["missing_target", "foreign_parent", "extra_leg"])
def test_malformed_linked_lists_never_decompose(monkeypatch, shape):
    session = _BracketSession()
    client = _client(session, monkeypatch)
    orders = _bracket()
    if shape == "missing_target":
        orders.pop()
    elif shape == "foreign_parent":
        orders[1].parent_order_id = ClientOrderId("outside")
    else:
        orders.append(
            _leg("EXTRA", OrderSide.SELL, OrderType.LIMIT, price=21030, parent="E")
        )
    _submit(client, orders)
    assert not session.placed
    assert len(client.events) == len(orders)
    assert all(event[0] == "denied" for event in client.events)


def test_inherited_tag_without_parent_never_rebinds_entry(monkeypatch):
    client, _session, _orders = _lifecycle_client(monkeypatch)
    fields = order_notification_to_fields(_notification("SL", "accepted"))
    fields["basket_id"] = "unknown-child"
    fields["original_basket_id"] = None
    assert client._resolve_client_order_id(fields) is None
    report = client._drain_row_from_fields(fields, 1_000_000_000).report
    assert report.client_order_id is None


@pytest.mark.parametrize("kind", ["accepted", "modified"])
def test_delayed_nonterminal_row_never_reopens_a_closed_leg(monkeypatch, kind):
    client, _session, orders = _lifecycle_client(monkeypatch)
    stop = orders[1]
    stop.status = OrderStatus.CANCELED
    stop.is_closed = True
    fields = order_notification_to_fields(_notification("SL", kind))
    assert client._drain_row_from_fields(fields, 1_000_000_000).report is None
    canceled = order_notification_to_fields(_notification("SL", "canceled"))
    report = client._drain_row_from_fields(canceled, 1_000_000_000).report
    assert report is not None and str(report.client_order_id) == "SL"


def test_accepted_exit_publishes_fill_anchored_venue_terms(monkeypatch):
    """Rithmic re-anchors exits to the fill; the cache must learn the venue terms."""
    session = _BracketSession()
    client = _client(session, monkeypatch)
    client.account_id = AccountId("RITHMIC-A")
    orders = _bracket()
    _submit(client, orders)
    updates = []
    monkeypatch.setattr(
        client,
        "generate_order_updated",
        lambda order, venue, qty, price, trigger, *a: updates.append(
            (str(order.client_order_id), price, trigger)
        ),
    )
    monkeypatch.setattr(client, "generate_order_accepted", lambda *a: None)
    for cid in ["E", "SL", "TP"]:
        client._dispatch_order_event(_notification(cid, "accepted"))
    # Entry and the unchanged target publish nothing; the re-anchored stop does.
    assert [(cid, float(t)) for cid, _p, t in updates if t is not None] == [
        ("SL", 20990.25)
    ]
    assert all(cid != "E" for cid, *_ in updates)


def test_partial_target_recon_keeps_partial_status(monkeypatch):
    client, _session, _orders = _lifecycle_client(monkeypatch)
    fields = order_notification_to_fields(
        _notification("TP", "filled", filled=1, fill_qty=1, fill_id="partial")
    )
    report = client._drain_row_from_fields(fields, 1_000_000_000).report
    assert report.order_status == OrderStatus.PARTIALLY_FILLED
    assert str(report.parent_order_id) == "E"


def test_bracket_update_is_not_a_synthetic_cancel(monkeypatch):
    client, _session, _orders = _lifecycle_client(monkeypatch)

    async def run():
        # Empty best-effort drain never proves that the venue canceled legs.
        client._dispatch_order_event(
            dict(
                type="bracket_update", basket_id="B", stop_quantity=0, target_quantity=0
            )
        )
        task = brackets_for(client).refresh_task
        assert task is not None
        await task

    asyncio.run(run())
    assert not any(kind == "canceled" for kind, cid in client.events)


@pytest.mark.parametrize("entry_side", [OrderSide.BUY, OrderSide.SELL])
def test_tick_directions_are_lossless_for_both_sides(monkeypatch, entry_side):
    session = _BracketSession()
    client = _client(session, monkeypatch)
    orders = _bracket()
    if entry_side == OrderSide.SELL:
        orders[0].side = OrderSide.SELL
        for leg in orders[1:]:
            leg.side = OrderSide.BUY
        orders[1].trigger_price = 21010
        orders[2].price = 20980
    _submit(client, orders)
    assert session.placed[0][-2:] == (40, 80)


def test_reconnect_drain_rebinds_both_legs_before_modify(monkeypatch):
    client, session, orders = _lifecycle_client(monkeypatch)
    del client._native_brackets
    client._venue_ids.clear()
    client._client_ids.clear()
    reports = []
    monkeypatch.setattr(client, "_send_order_status_report", reports.append)
    client._apply_drain_rows(
        [_notification("SL", "accepted"), _notification("TP", "accepted")]
    )
    assert {str(report.client_order_id) for report in reports} == {"SL", "TP"}
    assert all(str(report.parent_order_id) == "E" for report in reports)
    assert client._venue_id_for_order(orders[1]) == "BS"
    assert client._venue_id_for_order(orders[2]) == "BT"
    command = SimpleNamespace(
        client_order_id=ClientOrderId("SL"),
        venue_order_id=VenueOrderId("BS"),
        instrument_id=NQ,
        quantity=None,
        price=None,
        trigger_price=21000.25,
    )
    asyncio.run(client._modify_order(command))
    assert session.adjusted == [("stop", "B", 0, None)]
