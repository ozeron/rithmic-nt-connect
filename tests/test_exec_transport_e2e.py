"""Transport-level exec e2e tests (no creds, CI-green).

Drives the real ``RithmicExecutionClient`` against ``FaultInjectingSession``
doubles to prove the execution-safety invariants that depend on wire behavior:

- P1: an in-flight order query while latched/un-armed raises fail-closed
  (never a fabricated report); a healthy per-tag drain recovers the venue id
  before the engine's in-flight retries run out; reconnect re-arms only after
  a successful bounded drain.
- P2/P3: overfill, stale trigger, sentinel prices, race idempotency, and
  transport fault injection.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, cast

import pytest
from _stubs import (
    FaultInjectingSession,
    WireSessionStub,
    _CacheStub,
    _CaptureLog,
    _Log,
    _TestClient,
)
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    GenerateOrderStatusReport,
    GenerateOrderStatusReports,
    GeneratePositionStatusReports,
    SubmitOrder,
)
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.model.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from nautilus_trader.model.events import OrderSubmitted
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientOrderId,
    InstrumentId,
    StrategyId,
    TraderId,
    VenueOrderId,
)
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.orders import LimitOrder
from rithmic_nt_connect._order_plant import OrderPlantPolicy, OrderPlantState
from rithmic_nt_connect._orders import order_notification_to_fields
from rithmic_nt_connect.errors import (
    ReconciliationUnavailableError,
    VenueQueryUnavailable,
)
from rithmic_nt_connect.execution import RithmicExecutionClient
from rithmic_nt_connect.session import WireSession


def _client() -> _TestClient:
    client = _TestClient.__new__(_TestClient)
    client._log = _Log()
    client._clock = SimpleNamespace(timestamp_ns=lambda: 2)
    client._cache = _CacheStub()
    client.account_id = None
    return client


def _trading_client(
    session: WireSessionStub | None = None,
    *,
    enable_trading: bool = True,
    plant_state: OrderPlantState = OrderPlantState.LIVE,
) -> _TestClient:
    client = _client()
    client._config_local = cast(
        Any, SimpleNamespace(enable_trading=enable_trading, soft_fail_pnl=False)
    )
    client._order_plant = OrderPlantPolicy(plant_state)
    client._order_poll_transient_streak = 0
    client._pnl_snapshot_observed = asyncio.Event()
    client._session = cast(WireSession, session or FaultInjectingSession())
    client._seen_fill_keys = OrderedDict()
    client._untracked_status_keys = {}
    client._positions = {}
    client._account_seeded = True
    client.account_id = AccountId("RITHMIC-ACC1")
    return client


def _inflight_order(cid: str = "O-1") -> SimpleNamespace:
    """A tracked order stuck in-flight (SUBMITTED) with no venue id yet."""
    return SimpleNamespace(
        client_order_id=ClientOrderId(cid),
        venue_order_id=None,
        instrument_id=InstrumentId.from_str("NQ.GLBX"),
        is_inflight=True,
        status=OrderStatus.SUBMITTED,
        is_closed=False,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        quantity=Quantity.from_int(2),
        filled_qty=Quantity.zero(),
        ts_accepted=0,
        ts_last=1,
        price=Price.from_str("21000.0"),
        has_price=True,
        trigger_price=None,
        has_trigger_price=False,
        is_reduce_only=False,
        avg_px=None,
    )


def _working_order_row(
    *, tag: str = "O-1", basket: str = "B1", ts_event: int = 1_700_000_000_000_000_000
) -> dict[str, object]:
    """A normalized wire row for a working order whose tag matches a tracked
    client order id (what ``load_orders`` returns for a placed order)."""
    return {
        "type": "order_notification",
        "source": "rithmic",
        "kind": "accepted",
        "status": "OPEN",
        "basket_id": basket,
        "user_tag": tag,
        "symbol": "NQU6",
        "account_id": "ACC1",
        "quantity": 2,
        "total_fill_size": 0,
        "price": 21000.0,
        "ts_event_ns": ts_event,
        "transaction_type": 1,
        "price_type": 1,
        "duration": 1,
    }


def _fill_notification(
    *,
    fill_id: str = "F1",
    basket: str = "B1",
    fill_size: int = 2,
    fill_price: float = 21000.0,
) -> dict[str, object]:
    return {
        "type": "order_notification",
        "source": "exchange",
        "kind": "filled",
        "status": "COMPLETE",
        "basket_id": basket,
        "user_tag": "O-1",
        "symbol": "NQU6",
        "fill_id": fill_id,
        "fill_price": fill_price,
        "fill_size": fill_size,
        "quantity": 1,
        "transaction_type": 1,
        "ssboe": 1_700_000_000,
    }


def _trigger_order(*, status: OrderStatus, is_closed: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        client_order_id=ClientOrderId("O-1"),
        strategy_id=StrategyId("STRATEGY-1"),
        instrument_id=InstrumentId.from_str("NQ.GLBX"),
        order_type=OrderType.STOP_LIMIT,
        side=OrderSide.BUY,
        is_closed=is_closed,
        status=status,
    )


def _trigger_notification() -> dict[str, object]:
    return {
        "type": "order_notification",
        "source": "exchange",
        "symbol": "NQU6",
        "kind": "triggered",
        "notify_type_name": "TRIGGER",
        "basket_id": "B1",
        "user_tag": "O-1",
        "ssboe": 1_700_000_000,
    }


def _accepted_notification() -> dict[str, object]:
    return {
        "type": "order_notification",
        "source": "rithmic",
        "kind": "accepted",
        "status": "OPEN",
        "basket_id": "B1",
        "user_tag": "O-1",
        "symbol": "NQU6",
        "account_id": "ACC1",
        "quantity": 2,
        "price": 21000.0,
        "transaction_type": 1,
        "price_type": 1,
        "duration": 1,
    }


def _sentinel_status_fields() -> dict[str, object]:
    return {
        "type": "order_notification",
        "source": "rithmic",
        "kind": "accepted",
        "status": "OPEN",
        "basket_id": "B1",
        "user_tag": "O-1",
        "symbol": "NQU6",
        "account_id": "ACC1",
        "quantity": 2,
        "total_fill_size": 0,
        "price": -1.0,
        "trigger_price": -1.0,
        "avg_fill_price": -1.0,
        "transaction_type": 1,
        "price_type": 1,
        "duration": 1,
    }


def _query(cid: str = "O-1") -> GenerateOrderStatusReport:
    return GenerateOrderStatusReport(None, ClientOrderId(cid), None, UUID4(), 1)


def _status_cmd() -> GenerateOrderStatusReports:
    return GenerateOrderStatusReports(
        instrument_id=None,
        start=None,
        end=None,
        open_only=False,
        command_id=UUID4(),
        ts_init=1,
    )


def _position_cmd() -> GeneratePositionStatusReports:
    return GeneratePositionStatusReports(
        instrument_id=None, start=None, end=None, command_id=UUID4(), ts_init=1
    )


def _submitted_limit_order() -> LimitOrder:
    """A real Nautilus LIMIT order driven to SUBMITTED (in-flight, no venue id)."""
    iid = InstrumentId.from_str("NQ.GLBX")
    cid = ClientOrderId("O-1")
    trader = TraderId("TRADER-1")
    strategy = StrategyId("STRATEGY-1")
    account = AccountId("RITHMIC-ACC1")
    ts = 1_700_000_000_000_000_000
    order = LimitOrder(
        trader,
        strategy,
        iid,
        cid,
        OrderSide.BUY,
        Quantity.from_int(1),
        Price.from_str("21000.0"),
        UUID4(),
        0,
        TimeInForce.GTC,
    )
    order.apply(OrderSubmitted(trader, strategy, iid, cid, account, UUID4(), ts, ts))
    return order


def _submit_cmd() -> SubmitOrder:
    return SubmitOrder(
        TraderId("TRADER-1"),
        StrategyId("STRATEGY-1"),
        _submitted_limit_order(),
        UUID4(),
        1,
    )


# --------------------------------------------------------------------------- #
# P1: in-flight UNKNOWN fail-closed + per-tag drain recovery
# --------------------------------------------------------------------------- #


def test_unresolved_inflight_query_raises_not_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An in-flight query the drain cannot resolve must fail closed: raises
    ``VenueQueryUnavailable`` unconditionally (never ``None``, which the engine
    could treat as known) and never emits a fabricated reject/cancel."""
    client = _trading_client(
        FaultInjectingSession(), plant_state=OrderPlantState.LATCHED
    )
    order = _inflight_order()
    client._cache._orders[str(order.client_order_id)] = order
    rejected: list[tuple[object, ...]] = []
    canceled: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        client, "generate_order_rejected", lambda *a: rejected.append(a)
    )
    monkeypatch.setattr(
        client, "generate_order_canceled", lambda *a: canceled.append(a)
    )

    with pytest.raises(VenueQueryUnavailable, match="in-flight"):
        asyncio.run(client.generate_order_status_report(_query()))

    assert rejected == []
    assert canceled == []


def test_unbound_submitted_query_recovers_by_tag_from_drain() -> None:
    """A healthy plant answers an in-flight query from the bounded per-tag
    drain: the venue id gets bound and a real report is returned."""
    session = FaultInjectingSession(working_orders=[_working_order_row()])
    client = _trading_client(session)
    order = _inflight_order()
    client._cache._orders[str(order.client_order_id)] = order

    report = asyncio.run(client.generate_order_status_report(_query()))

    assert report is not None
    assert report.venue_order_id == VenueOrderId("B1")
    assert report.client_order_id == ClientOrderId("O-1")
    assert report.order_status == OrderStatus.ACCEPTED
    assert client._cache.venue_order_id(ClientOrderId("O-1")) == VenueOrderId("B1")
    assert "load_orders" in session.calls


def test_unusable_drain_row_does_not_bind_or_disable_recovery() -> None:
    """P1: a drain row matching the tag but unusable (no order terms) must not
    bind the venue id nor fall back to a cache-derived report — the query
    fails closed, and the recovery branch stays enabled for the next query."""
    bare = _working_order_row()
    bare["quantity"] = 0  # a bare TRIGGER-like row: no order terms
    session = FaultInjectingSession(working_orders=[bare])
    client = _trading_client(session)
    order = _inflight_order()
    client._cache._orders[str(order.client_order_id)] = order

    with pytest.raises(VenueQueryUnavailable, match="in-flight"):
        asyncio.run(client.generate_order_status_report(_query()))

    assert client._cache.venue_order_id(ClientOrderId("O-1")) is None

    # The venue heals: the same still-in-flight order recovers by tag.
    session.working_orders = [_working_order_row()]
    report = asyncio.run(client.generate_order_status_report(_query()))

    assert report is not None
    assert report.venue_order_id == VenueOrderId("B1")
    assert client._cache.venue_order_id(ClientOrderId("O-1")) == VenueOrderId("B1")


def test_recovered_report_uses_venue_row_timestamp() -> None:
    """P1: the recovered report carries the venue row's ``ts_event``, never the
    query time (venue time -> ``ts_event``; adapter clock -> ``ts_init``)."""
    row_ts = 1_700_000_000_000_000_000
    session = FaultInjectingSession(
        working_orders=[_working_order_row(ts_event=row_ts)]
    )
    client = _trading_client(session)
    order = _inflight_order()
    client._cache._orders[str(order.client_order_id)] = order

    report = asyncio.run(client.generate_order_status_report(_query()))

    assert report is not None
    assert report.ts_accepted == row_ts
    assert report.ts_last == row_ts


def test_recovery_drain_rejects_row_with_fabricated_terms() -> None:
    """P0 #2: a row whose order terms would be fabricated by the permissive
    recon builder (missing price_type) is not trusted for recovery — no bind,
    no report; the query fails closed, and the venue id is bound only once a
    real row arrives."""
    bare = _working_order_row()
    bare["price_type"] = None  # recon builder would fabricate MARKET
    session = FaultInjectingSession(working_orders=[bare])
    client = _trading_client(session)
    order = _inflight_order()
    client._cache._orders[str(order.client_order_id)] = order

    with pytest.raises(VenueQueryUnavailable, match="in-flight"):
        asyncio.run(client.generate_order_status_report(_query()))

    assert client._cache.venue_order_id(ClientOrderId("O-1")) is None

    # A real working-order row recovers normally.
    session.working_orders = [_working_order_row()]
    report = asyncio.run(client.generate_order_status_report(_query()))

    assert report is not None
    assert report.venue_order_id == VenueOrderId("B1")


def test_recovery_drain_rejects_row_with_complete_status() -> None:
    """Macroscope #3: a strict recovery row whose status is COMPLETE (which
    the status mapper would otherwise report as ACCEPTED) is not trusted for
    recovery — the query fails closed rather than binding a venue id with a
    fabricated open state."""
    bare = _working_order_row()
    bare["kind"] = None  # no kind -> the status marker alone decides
    bare["status"] = "COMPLETE"
    session = FaultInjectingSession(working_orders=[bare])
    client = _trading_client(session)
    order = _inflight_order()
    client._cache._orders[str(order.client_order_id)] = order

    with pytest.raises(VenueQueryUnavailable, match="in-flight"):
        asyncio.run(client.generate_order_status_report(_query()))

    assert client._cache.venue_order_id(ClientOrderId("O-1")) is None


def test_stale_drain_row_does_not_regress_live_resolution() -> None:
    """P1: if the live stream resolves the order while the drain is in flight
    (bound venue id, terminal state), the drain's stale OPEN row must not be
    reported over the newer live state."""
    session = FaultInjectingSession(working_orders=[_working_order_row()])
    client = _trading_client(session)
    order = _inflight_order()
    client._cache._orders[str(order.client_order_id)] = order

    def _resolve_live() -> None:
        # The live stream accepts + fills the order while the drain is in flight.
        client._cache.add_venue_order_id(ClientOrderId("O-1"), VenueOrderId("B1"))
        order.status = OrderStatus.FILLED
        order.is_closed = True
        order.filled_qty = Quantity.from_int(1)
        order.leaves_qty = Quantity.zero()

    session.on_load_orders = _resolve_live

    report = asyncio.run(client.generate_order_status_report(_query()))

    assert report is not None
    assert report.order_status == OrderStatus.FILLED  # live state wins
    assert report.venue_order_id == VenueOrderId("B1")


def test_recovery_prefers_latest_strict_row() -> None:
    """Oracle #5: per-tag recovery answers with the NEWEST matching strict row
    (same (ts_event, arrival) policy as the bulk status path), not the first —
    a drain holding ACCEPTED(t1) then CANCELED(t2) for the same order must not
    bind on the stale OPEN row."""
    accepted = _working_order_row(
        tag="O-1", basket="B1", ts_event=1_700_000_000_000_000_001
    )
    canceled = _working_order_row(
        tag="O-1", basket="B1", ts_event=1_700_000_000_000_000_002
    )
    canceled.update({"kind": "canceled", "status": "CANCELLED"})
    session = FaultInjectingSession(working_orders=[accepted, canceled])
    client = _trading_client(session)
    order = _inflight_order()
    client._cache._orders[str(order.client_order_id)] = order

    report = asyncio.run(client.generate_order_status_report(_query()))

    assert report is not None
    assert report.order_status == OrderStatus.CANCELED  # newest row wins
    assert report.venue_order_id == VenueOrderId("B1")
    assert client._cache.venue_order_id(ClientOrderId("O-1")) == VenueOrderId("B1")


def test_order_poll_persistent_transient_fails_closed() -> None:
    """Oracle #6: persistent non-channel errors on the order stream fail closed
    (latch) instead of being swallowed as transient forever while the plant
    stays LIVE and silent."""
    client = _trading_client()
    calls = {"n": 0}

    def poll_fn() -> dict[str, object] | None:
        calls["n"] += 1
        raise ValueError("protocol decode error")

    awaitable = RithmicExecutionClient._plant_poll_loop(
        cast(RithmicExecutionClient, client),
        name="order",
        poll_fn=poll_fn,
        on_event=lambda event: None,
        on_resync=lambda: None,
    )
    asyncio.run(awaitable)

    assert calls["n"] == RithmicExecutionClient._ORDER_POLL_MAX_TRANSIENT
    assert client._order_plant.latched
    assert client._order_plant.state is OrderPlantState.LATCHED
    assert not client._order_plant.allow_submit()


class _ConnectSession(WireSessionStub):
    """Session double for ``_connect``: PnL + order plants connect, and
    ``load_orders`` fails, heals, or runs a hook on demand."""

    def __init__(
        self,
        *,
        fail_load_orders: bool,
        on_load_orders: Callable[[], None] | None = None,
    ) -> None:
        self.fail_load_orders = fail_load_orders
        self.on_load_orders = on_load_orders
        self.calls: list[str] = []
        self._inner = self  # ``_connect_once`` treats a plain session as itself

    def connect(self) -> None:
        self.calls.append("connect")

    def subscribe_pnl(self) -> None:
        self.calls.append("subscribe_pnl")

    def resolved_account(self) -> dict[str, object] | None:
        return None

    def poll_pnl_event(self) -> dict[str, object] | None:
        return None

    def subscribe_order_updates(self) -> None:
        self.calls.append("subscribe_order_updates")

    def disconnect_order_plant(self) -> None:
        self.calls.append("disconnect_order_plant")

    def poll_order_event(self) -> dict[str, object] | None:
        return None

    def load_orders(self, start: int, end: int) -> list[dict[str, object]]:
        self.calls.append("load_orders")
        if self.on_load_orders is not None:
            self.on_load_orders()
        if self.fail_load_orders:
            raise ReconciliationUnavailableError("order recon unavailable")
        return []


def _connect_client(
    session: WireSessionStub,
    monkeypatch: pytest.MonkeyPatch,
    *,
    stub_snapshot_wait: bool = True,
) -> _TestClient:
    client = _trading_client(session, plant_state=OrderPlantState.LATCHED)
    monkeypatch.setattr(client, "_apply_resolved_account_id", lambda: None)
    monkeypatch.setattr(client, "_seed_account_if_needed", lambda *a, **k: None)

    async def _await_account_registered() -> None:
        return None

    monkeypatch.setattr(client, "_await_account_registered", _await_account_registered)

    def _create_task(coro: Any, log_msg: str | None = None) -> asyncio.Task[Any]:
        # Real poll task (never-drop): ``rearm`` requires a live poll task, so
        # a None task must not count as alive. The session doubles return None
        # from their poll fns, so the loop idles; asyncio.run cancels it at
        # loop shutdown.
        return asyncio.ensure_future(coro)

    monkeypatch.setattr(client, "create_task", _create_task)
    if stub_snapshot_wait:
        # Most reconnect tests stub the PnL-snapshot wait (the PnL session
        # double delivers nothing); the dedicated
        # ``test_reconnect_ream_requires_pnl_snapshot`` exercises it for real.
        async def _no_snapshot_wait() -> None:
            return None

        monkeypatch.setattr(client, "_await_pnl_snapshot", _no_snapshot_wait)
    return client


def test_reconnect_ream_requires_successful_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reconnect must not re-arm on re-subscribe alone: while the bounded
    drain fails, the latch survives and submit stays denied; once the drain
    heals, the next connect re-arms."""
    session = _ConnectSession(fail_load_orders=True)
    client = _connect_client(session, monkeypatch)

    asyncio.run(client._connect())

    # A failed re-arm leaves the plant LATCHED (recon pending): blocked.
    assert client._order_plant.latched, "latch must survive a failed re-arm"
    assert client._order_plant.state is OrderPlantState.LATCHED
    assert not client._order_plant.allow_submit()
    assert "load_orders" in session.calls

    # Venue heals: the next connect re-arms (latch cleared, plant LIVE).
    session.fail_load_orders = False
    asyncio.run(client._connect())

    assert not client._order_plant.latched
    assert client._order_plant.state is OrderPlantState.LIVE
    assert client._order_plant.allow_submit()


def test_unlatched_reconnect_still_runs_rearm_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Macroscope #1: an ordinary (un-latched) reconnect must not go LIVE on
    re-subscribe alone — the drain must succeed (and PnL be re-observed)
    before trading is re-armed on every connect."""
    session = _ConnectSession(fail_load_orders=True)
    client = _connect_client(session, monkeypatch)  # starts LIVE: un-latched

    asyncio.run(client._connect())

    # A failed drain leaves the plant LATCHED (recon pending): blocked.
    assert client._order_plant.state is OrderPlantState.LATCHED
    assert not client._order_plant.allow_submit()

    # Venue heals: the next (still un-latched) connect re-arms.
    session.fail_load_orders = False
    asyncio.run(client._connect())

    assert client._order_plant.state is OrderPlantState.LIVE
    assert client._order_plant.allow_submit()


def test_reconnect_ream_requires_pnl_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P1/R2: the re-arm barrier also requires a fresh account/position PnL
    observation; a session whose PnL stream never delivers keeps the latch and
    the plant un-armed."""
    monkeypatch.setattr(RithmicExecutionClient, "_REARM_PNL_SNAPSHOT_TIMEOUT_S", 0.2)
    client = _connect_client(
        _ConnectSession(fail_load_orders=False),
        monkeypatch,
        stub_snapshot_wait=False,
    )

    asyncio.run(client._connect())

    assert client._order_plant.latched
    assert client._order_plant.state is OrderPlantState.LATCHED
    assert not client._order_plant.allow_submit()


def test_reconnect_ream_requires_plant_stayed_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P1: the re-arm barrier clears the latch only if the order plant remains
    in the state in which the barrier started (CONNECTING). Any anomaly during
    the drain — a newer latch (e.g. an overfill) or a stream state change
    (resync failure / handler break, no new latch) — leaves CONNECTING, and
    the re-arm must not clear a latch over state it did not observe."""
    for anomaly in ("latch", "stream_state"):
        session = _ConnectSession(fail_load_orders=False)
        client = _connect_client(session, monkeypatch)

        def _anomaly_during_drain(
            _client: _TestClient = client, _anomaly: str = anomaly
        ) -> None:
            if _anomaly == "latch":
                _client._latch_order_plant("overfill", "fill exceeds leaves mid-drain")
            else:
                # A stream failure without a new latch (resync failure /
                # handler break) also leaves CONNECTING.
                _client._order_plant.disconnect()

        session.on_load_orders = _anomaly_during_drain

        asyncio.run(client._connect())

        # Either anomaly during the drain leaves the plant un-armed: the
        # re-arm must not clear a latch over state it did not observe.
        assert not client._order_plant.allow_submit()
        if anomaly == "latch":
            assert client._order_plant.latched
            assert client._order_plant.state is OrderPlantState.LATCHED
        else:
            assert client._order_plant.state is OrderPlantState.DISCONNECTED


def test_reconnect_ream_applies_drain_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Macroscope #2: the re-arm barrier applies the drained rows — a tracked
    in-flight order matching a drain row gets its venue id bound and a status
    report is published, so it is not left unresolved after the plant re-arms
    (commands cannot duplicate/conflict with an order the venue already
    accepted)."""
    session = FaultInjectingSession(
        working_orders=[_working_order_row(tag="O-1", basket="B1")]
    )
    client = _connect_client(session, monkeypatch)
    order = _inflight_order()
    client._cache._orders[str(order.client_order_id)] = order
    reports: list[OrderStatusReport] = []
    monkeypatch.setattr(
        client, "_send_order_status_report", lambda report: reports.append(report)
    )

    asyncio.run(client._connect())

    assert client._order_plant.allow_submit()
    assert client._cache.venue_order_id(ClientOrderId("O-1")) == VenueOrderId("B1")
    assert len(reports) == 1
    assert reports[0].venue_order_id == VenueOrderId("B1")


def test_reconnect_ream_drain_binds_only_usable_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Macroscope #2-followup: the re-arm drain must not bind a venue id from
    a row that cannot build a usable report — binding would mark a stale local
    order "venue-resolved" and skip fail-closed recovery (build-then-bind,
    same as ``_resolve_inflight_by_tag``). A row missing closed-set terms
    (price_type/duration) must not bind either (strict trust boundary), while
    a fully usable row still binds and publishes."""
    malformed = _working_order_row(tag="O-2", basket="B2")
    malformed["quantity"] = 0  # no order terms: cannot build any report
    no_terms = _working_order_row(tag="O-3", basket="B3")
    del no_terms["price_type"]
    del no_terms["duration"]
    session = FaultInjectingSession(
        working_orders=[
            _working_order_row(tag="O-1", basket="B1"),
            malformed,
            no_terms,
        ]
    )
    client = _connect_client(session, monkeypatch)
    for cid in ("O-1", "O-2", "O-3"):
        client._cache._orders[str(ClientOrderId(cid))] = _inflight_order(cid=cid)
    reports: list[OrderStatusReport] = []
    monkeypatch.setattr(
        client, "_send_order_status_report", lambda report: reports.append(report)
    )

    asyncio.run(client._connect())

    assert client._order_plant.allow_submit()
    # Fully usable row: bound + published.
    assert client._cache.venue_order_id(ClientOrderId("O-1")) == VenueOrderId("B1")
    # Malformed row: no bind, no publish.
    assert client._cache.venue_order_id(ClientOrderId("O-2")) is None
    # Terms-missing row: advisory report still publishes, but no bind.
    assert client._cache.venue_order_id(ClientOrderId("O-3")) is None
    assert {r.venue_order_id for r in reports} == {
        VenueOrderId("B1"),
        VenueOrderId("B3"),
    }


def test_drain_row_boundary_usable_bindable_are_one_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Root-cause item 2: the drain-row interpretation is ONE boundary
    (``_drain_row_from_fields``). For any row: ``bindable`` (strict) implies
    ``usable`` (permissive), and a row is usable iff a permissive report
    builds — callers never re-interpret the row."""
    client = _trading_client()

    # Fully usable row: strict == permissive report.
    row = client._drain_row_from_fields(_working_order_row(tag="O-1", basket="B1"), 1)
    assert row.usable and row.bindable
    assert row.strict_report is row.permissive_report
    assert row.permissive_report is not None

    # Terms-missing row (no price_type/duration): advisory (usable) but not
    # bindable — the permissive fallback fabricates MARKET/GTC for publish.
    no_terms = _working_order_row(tag="O-1", basket="B1")
    del no_terms["price_type"]
    del no_terms["duration"]
    row = client._drain_row_from_fields(no_terms, 1)
    assert row.usable and not row.bindable
    assert row.strict_report is None
    assert row.permissive_report is not None

    # Malformed row (no order terms): neither usable nor bindable.
    malformed = _working_order_row(tag="O-1", basket="B1")
    malformed["quantity"] = 0
    row = client._drain_row_from_fields(malformed, 1)
    assert not row.usable and not row.bindable
    assert row.strict_report is None and row.permissive_report is None


def test_strict_drain_row_triggered_binds_and_reports_triggered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Oracle #2: an exchange TRIGGER drain row (triggered-but-working stop) is
    strict-usable — it binds a venue id and reports TRIGGERED for a limit-style
    stop, ACCEPTED (working) for a market-style stop. Previously the row was
    strict-unusable and in-flight recovery skipped it entirely."""
    client = _trading_client()
    # Limit-style stop (price_type=3): TRIGGERED.
    stop_limit = _working_order_row(tag="O-1", basket="B1")
    stop_limit.update(
        {
            "source": "exchange",
            "kind": "triggered",
            "status": "TRIGGERED",
            "price_type": 3,
        }
    )
    row = client._drain_row_from_fields(stop_limit, 1)
    assert row.bindable
    assert row.strict_report is not None
    assert row.strict_report.order_status is OrderStatus.TRIGGERED
    # Market-style stop (price_type=4): no TRIGGERED state, stays working.
    stop_market = _working_order_row(tag="O-2", basket="B2")
    stop_market.update(
        {
            "source": "exchange",
            "kind": "triggered",
            "status": "TRIGGERED",
            "price_type": 4,
        }
    )
    row = client._drain_row_from_fields(stop_market, 1)
    assert row.bindable
    assert row.strict_report is not None
    assert row.strict_report.order_status is OrderStatus.ACCEPTED


def test_reconnect_ream_drain_publish_failure_fails_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Oracle #4: the re-arm barrier must not succeed when a drained status
    report fails to publish — the venue id is bound only after the report is
    applied, and a publication failure aborts the barrier (latch)."""
    session = FaultInjectingSession(
        working_orders=[_working_order_row(tag="O-1", basket="B1")]
    )
    client = _connect_client(session, monkeypatch)
    order = _inflight_order()
    client._cache._orders[str(order.client_order_id)] = order
    monkeypatch.setattr(
        client,
        "_send_order_status_report",
        lambda report: (_ for _ in ()).throw(RuntimeError("engine bus down")),
    )

    asyncio.run(client._connect())

    # Barrier aborted: plant latched, venue id NOT bound (commit ordering).
    assert client._order_plant.latched
    assert client._order_plant.state is OrderPlantState.LATCHED
    assert not client._order_plant.allow_submit()
    assert client._cache.venue_order_id(ClientOrderId("O-1")) is None


def test_apply_drain_rows_skips_stale_live_advanced_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Oracle #7: the live stream wins over a stale drain snapshot — a tracked
    order the live stream already resolved (terminal, or newer local ts_last)
    must not be re-published from the captured drain row."""
    session = FaultInjectingSession(
        working_orders=[_working_order_row(tag="O-1", basket="B1")]
    )
    client = _connect_client(session, monkeypatch)
    # The live stream advanced the order AFTER the drain captured its ACCEPTED
    # row: still open, but with a newer local ts_last than the drain row.
    order = _inflight_order()
    order.ts_last = 1_700_000_000_000_000_001  # newer than the row (…000)
    client._cache._orders[str(order.client_order_id)] = order
    reports: list[OrderStatusReport] = []
    monkeypatch.setattr(
        client, "_send_order_status_report", lambda report: reports.append(report)
    )

    asyncio.run(client._connect())

    assert client._order_plant.allow_submit()
    assert reports == [], "stale drain row must not be published over live state"
    assert client._cache.venue_order_id(ClientOrderId("O-1")) is None


def test_pnl_marker_only_after_successful_account_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P1 #3: the PnL activity marker is set only after account processing
    fully succeeds — a handler that raises must not satisfy the re-arm gate."""
    client = _trading_client(enable_trading=False)

    def _boom(**kwargs: object) -> None:
        raise RuntimeError("account state publication failed")

    monkeypatch.setattr(client, "generate_account_state", _boom)
    with pytest.raises(RuntimeError, match="publication failed"):
        client._dispatch_pnl_event(
            {"type": "account_pnl", "account_id": "ACC1", "cash_on_hand": "100.0"}
        )

    assert not client._pnl_snapshot_observed.is_set()

    # A healthy dispatch sets the marker.
    healthy = _trading_client(enable_trading=False)
    monkeypatch.setattr(healthy, "generate_account_state", lambda **k: None)
    healthy._dispatch_pnl_event(
        {"type": "account_pnl", "account_id": "ACC1", "cash_on_hand": "100.0"}
    )
    assert healthy._pnl_snapshot_observed.is_set()


# --------------------------------------------------------------------------- #
# P2: overfill / stale trigger / basket guard / sentinel
# --------------------------------------------------------------------------- #


def test_tracked_overfill_emits_latches_and_logs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A2: a unique venue fill beyond the local remaining qty is emitted
    (never-drop, never cap) with a loud error and a plant latch."""
    client = _trading_client()
    order = SimpleNamespace(
        client_order_id=ClientOrderId("O-1"),
        strategy_id=StrategyId("STRATEGY-1"),
        instrument_id=InstrumentId.from_str("NQ.GLBX"),
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        is_closed=False,
        leaves_qty=Quantity.from_int(1),
        quantity=Quantity.from_int(1),
        filled_qty=Quantity.zero(),
    )
    client._cache._orders[str(order.client_order_id)] = order
    log = _CaptureLog()
    client._log = log
    filled: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        client, "generate_order_filled", lambda *a, **k: filled.append(a)
    )
    monkeypatch.setattr(
        client,
        "_price_for_instrument",
        lambda instrument_id, value: Price.from_str("21000.0"),
    )

    RithmicExecutionClient._handle_order_notification(
        cast(RithmicExecutionClient, client), _fill_notification(fill_size=2)
    )

    assert len(filled) == 1, "the real venue fill must never be dropped"
    assert filled[0][8] == Quantity.from_int(2), "the emitted qty must not be capped"
    assert client._order_plant.latched
    assert client._order_plant.state is OrderPlantState.LATCHED
    assert any("overfill" in message for message in log.messages)


def test_stale_basket_of_closed_order_goes_untracked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A5: a basket mapping to a closed order must route to the untracked
    report path — never a typed event for the stale incarnation."""
    client = _trading_client()
    order = _trigger_order(status=OrderStatus.CANCELED, is_closed=True)
    client._cache._orders[str(order.client_order_id)] = order
    client._cache.add_venue_order_id(ClientOrderId("O-1"), VenueOrderId("B1"))
    typed: list[tuple[object, ...]] = []
    reports: list[object] = []
    monkeypatch.setattr(
        client, "generate_order_accepted", lambda *a, **k: typed.append(a)
    )
    monkeypatch.setattr(
        client, "_send_order_status_report", lambda report: reports.append(report)
    )

    RithmicExecutionClient._handle_order_notification(
        cast(RithmicExecutionClient, client), _accepted_notification()
    )

    assert typed == []
    assert len(reports) == 1


def test_stale_trigger_after_fill_suppressed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A1+A5: a late TRIGGER for an already-closed (filled) order routes to the
    untracked report path — never a typed OrderTriggered for the stale
    incarnation (attribution guard), and no crash."""
    client = _trading_client()
    order = _trigger_order(status=OrderStatus.FILLED, is_closed=True)
    client._cache._orders[str(order.client_order_id)] = order
    client._cache.add_venue_order_id(ClientOrderId("O-1"), VenueOrderId("B1"))
    triggered: list[tuple[object, ...]] = []
    log = _CaptureLog()
    client._log = log
    monkeypatch.setattr(
        client, "generate_order_triggered", lambda *a: triggered.append(a)
    )

    RithmicExecutionClient._handle_order_notification(
        cast(RithmicExecutionClient, client), _trigger_notification()
    )

    assert triggered == []
    # The closed-order guard at ``_resolve_client_order_id`` routes it
    # untracked: the untracked-path log proves it never reached the tracked
    # trigger branch.
    assert any("untracked" in message for message in log.messages)


def test_duplicate_trigger_emits_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """A1: a STOP_LIMIT emits OrderTriggered exactly once per live trigger."""
    client = _trading_client()
    order = _trigger_order(status=OrderStatus.ACCEPTED)
    client._cache._orders[str(order.client_order_id)] = order
    triggered: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        client, "generate_order_triggered", lambda *a: triggered.append(a)
    )

    RithmicExecutionClient._handle_order_notification(
        cast(RithmicExecutionClient, client), _trigger_notification()
    )
    order.status = OrderStatus.TRIGGERED  # Nautilus applied the emitted event
    RithmicExecutionClient._handle_order_notification(
        cast(RithmicExecutionClient, client), _trigger_notification()
    )

    assert len(triggered) == 1


def test_sentinel_status_fields_are_none() -> None:
    """A3: venue sentinel prices (-1.0) convert to None at the convert
    boundary — never a fake ``Price``/``Decimal`` in an ``OrderStatusReport``."""
    client = _trading_client()
    fields = order_notification_to_fields(_sentinel_status_fields())

    report = client._order_status_report_from_fields(fields, 2)

    assert report is not None
    assert report.avg_px is None
    assert report.price is None
    assert report.trigger_price is None


def test_sentinel_fill_price_suppressed_not_crashed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A3: a tracked fill whose price is the venue sentinel cannot be priced;
    it is suppressed with an error, never emitted with a fake price."""
    client = _trading_client()
    order = SimpleNamespace(
        client_order_id=ClientOrderId("O-1"),
        strategy_id=StrategyId("STRATEGY-1"),
        instrument_id=InstrumentId.from_str("NQ.GLBX"),
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        is_closed=False,
        leaves_qty=Quantity.from_int(2),
        quantity=Quantity.from_int(2),
        filled_qty=Quantity.zero(),
    )
    client._cache._orders[str(order.client_order_id)] = order
    log = _CaptureLog()
    client._log = log
    filled: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        client, "generate_order_filled", lambda *a, **k: filled.append(a)
    )

    def _price_for_instrument(instrument_id: InstrumentId, value: object) -> Price:
        if value is None:
            raise ValueError("bad px")
        return Price.from_str("21000.0")

    monkeypatch.setattr(client, "_price_for_instrument", _price_for_instrument)

    # Enter via the convert boundary: ``-1.0`` becomes ``None`` there, and the
    # definitive fill still reaches the fill branch (fill classification is
    # independent of priceability), which fails pricing and latches.
    fields = order_notification_to_fields(_fill_notification(fill_price=-1.0))
    RithmicExecutionClient._handle_order_notification(
        cast(RithmicExecutionClient, client), fields
    )

    assert filled == []
    assert any("suppressed" in message for message in log.messages)
    # P1 #4: an unpriceable definitive fill latches the plant (exposure may
    # be incomplete) — the dedup key is not consumed, so a later priced replay
    # of the same fill id can still recover.
    assert client._order_plant.latched
    assert client._order_plant.state is OrderPlantState.LATCHED


def test_sentinel_untracked_fill_report_is_none() -> None:
    """A3: a sentinel-priced fill is ``None`` at the convert boundary, so the
    untracked FillReport builder cannot fabricate a price for it."""
    client = _trading_client()
    fields = order_notification_to_fields(_fill_notification(fill_price=-1.0))

    report = client._fill_report_from_fields(fields, 2)

    assert report is None


# --------------------------------------------------------------------------- #
# P3/R9: transport fault injection + race/recovery idempotency
# --------------------------------------------------------------------------- #


def test_fault_inject_submit_unknown_latches_and_recovers_without_duplicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R9: cutting the channel after a submit send leaves the order in-flight
    (unknown stays unknown), latches the plant, and a later reconnect re-arms
    via the drain without duplicating the order."""
    session = FaultInjectingSession(
        fault="submit", working_orders=[_working_order_row(tag="O-1", basket="B1")]
    )
    client = _trading_client(session)
    monkeypatch.setattr(client, "_route", lambda instrument_id: ("NQU6", "CME"))
    submitted: list[tuple[object, ...]] = []
    rejected: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        client, "generate_order_submitted", lambda *a, **k: submitted.append(a)
    )
    monkeypatch.setattr(
        client, "generate_order_rejected", lambda *a, **k: rejected.append(a)
    )

    asyncio.run(client._submit_order(_submit_cmd()))

    assert len(submitted) == 1, "local submit is emitted before the send"
    assert rejected == [], "a transport cut must not fabricate a venue reject"
    assert client._order_plant.latched
    assert client._order_plant.state is OrderPlantState.LATCHED
    assert not client._order_plant.allow_submit()

    # Heal + reconnect: the bounded drain returns the working order, re-arms
    # the plant, and the batch status query reports it exactly once.
    session.fault = None
    client = _connect_client(session, monkeypatch)
    monkeypatch.setattr(client, "_send_order_status_report", lambda report: None)
    asyncio.run(client._connect())

    assert not client._order_plant.latched
    assert client._order_plant.state is OrderPlantState.LIVE
    reports = asyncio.run(client.generate_order_status_reports(_status_cmd()))
    assert [r.venue_order_id for r in reports] == [VenueOrderId("B1")]


def test_cancel_after_fill_race_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late CANCEL for a filled order produces no typed OrderCanceled; the
    now-closed order routes untracked (A5) and the fill is emitted once."""
    client = _trading_client()
    order = SimpleNamespace(
        client_order_id=ClientOrderId("O-1"),
        strategy_id=StrategyId("STRATEGY-1"),
        instrument_id=InstrumentId.from_str("NQ.GLBX"),
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        is_closed=False,
        status=OrderStatus.ACCEPTED,
        leaves_qty=Quantity.from_int(1),
        quantity=Quantity.from_int(1),
        filled_qty=Quantity.zero(),
    )
    client._cache._orders[str(order.client_order_id)] = order
    client._cache.add_venue_order_id(ClientOrderId("O-1"), VenueOrderId("B1"))
    filled: list[tuple[Any, ...]] = []
    canceled: list[tuple[Any, ...]] = []
    reports: list[object] = []
    monkeypatch.setattr(
        client, "generate_order_filled", lambda *a, **k: filled.append(a)
    )
    monkeypatch.setattr(
        client, "generate_order_canceled", lambda *a, **k: canceled.append(a)
    )
    monkeypatch.setattr(
        client, "_send_order_status_report", lambda report: reports.append(report)
    )
    monkeypatch.setattr(
        client,
        "_price_for_instrument",
        lambda instrument_id, value: Price.from_str("21000.0"),
    )

    # Fill while open -> typed OrderFilled.
    RithmicExecutionClient._handle_order_notification(
        cast(RithmicExecutionClient, client), _fill_notification(fill_size=1)
    )
    # Nautilus applies the fill: the order is now closed.
    order.is_closed = True
    order.status = OrderStatus.FILLED
    order.leaves_qty = Quantity.zero()
    order.filled_qty = Quantity.from_int(1)
    # Late CANCEL -> closed order routes untracked, no typed OrderCanceled.
    RithmicExecutionClient._handle_order_notification(
        cast(RithmicExecutionClient, client),
        {
            "type": "order_notification",
            "source": "rithmic",
            "kind": "canceled",
            "status": "CANCELED",
            "basket_id": "B1",
            "user_tag": "O-1",
            "symbol": "NQU6",
            "account_id": "ACC1",
            "quantity": 1,
            "transaction_type": 1,
            "price_type": 1,
            "duration": 1,
        },
    )

    assert len(filled) == 1
    assert canceled == []
    assert len(reports) == 1  # the late cancel surfaced as an external report


def test_fill_after_cancel_race_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late FILL for a canceled order is reported as external (never a typed
    OrderFilled for the closed incarnation) and deduped on replay."""
    client = _trading_client()
    order = SimpleNamespace(
        client_order_id=ClientOrderId("O-1"),
        strategy_id=StrategyId("STRATEGY-1"),
        instrument_id=InstrumentId.from_str("NQ.GLBX"),
        order_type=OrderType.LIMIT,
        side=OrderSide.BUY,
        is_closed=True,
        status=OrderStatus.CANCELED,
        leaves_qty=Quantity.zero(),
        quantity=Quantity.from_int(1),
        filled_qty=Quantity.zero(),
    )
    client._cache._orders[str(order.client_order_id)] = order
    client._cache.add_venue_order_id(ClientOrderId("O-1"), VenueOrderId("B1"))
    typed_filled: list[tuple[Any, ...]] = []
    external_fills: list[object] = []
    monkeypatch.setattr(
        client, "generate_order_filled", lambda *a, **k: typed_filled.append(a)
    )
    monkeypatch.setattr(client, "_send_order_status_report", lambda report: None)
    monkeypatch.setattr(
        client, "_send_fill_report", lambda report: external_fills.append(report)
    )
    monkeypatch.setattr(
        client,
        "_price_for_instrument",
        lambda instrument_id, value: Price.from_str("21000.0"),
    )

    for _ in range(2):  # the same late fill replays twice
        RithmicExecutionClient._handle_order_notification(
            cast(RithmicExecutionClient, client), _fill_notification(fill_size=1)
        )

    assert typed_filled == []
    assert len(external_fills) == 1  # deduped by venue fill id


def test_latched_submit_recovered_via_status_reports() -> None:
    """B7: even while latched, the batch status query drains the venue and
    reports the working order by its user_tag (the recovery path)."""
    session = FaultInjectingSession(working_orders=[_working_order_row()])
    client = _trading_client(session, plant_state=OrderPlantState.LATCHED)

    reports = asyncio.run(client.generate_order_status_reports(_status_cmd()))

    assert [r.venue_order_id for r in reports] == [VenueOrderId("B1")]
    assert [r.client_order_id for r in reports] == [ClientOrderId("O-1")]


def test_position_query_failure_emits_no_fills(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """B8: a position-source failure raises (never an empty \"flat\" success)
    and never emits a fill/flatten event."""
    client = _trading_client()

    class _BoomPositions(dict[str, dict[str, object]]):
        def values(self):  # type: ignore[override]
            raise RuntimeError("position query failed")

    client._positions = _BoomPositions()
    filled: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        client, "generate_order_filled", lambda *a, **k: filled.append(a)
    )

    with pytest.raises(RuntimeError, match="position query failed"):
        asyncio.run(client.generate_position_status_reports(_position_cmd()))

    assert filled == []


def test_nonzero_position_visible_before_trading_armed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """B9: an existing venue position is reported before the trading path is
    armed (read-only mode), so startup exposure is never hidden."""
    client = _trading_client(enable_trading=False)
    client._positions = {
        "NQ": {
            "instrument_id": "NQ.GLBX",
            "position_side": "LONG",
            "quantity": 1,
            "avg_px_open": 21000.0,
        }
    }
    monkeypatch.setattr(
        client._cache,
        "instrument",
        lambda instrument_id: SimpleNamespace(),
        raising=False,
    )

    reports = asyncio.run(client.generate_position_status_reports(_position_cmd()))

    assert len(reports) == 1
    assert reports[0].quantity == Quantity.from_int(1)
