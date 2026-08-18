"""Unit tests for exec recon honesty (empty recon, fill dedup, status mapping)."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

import pytest
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    GenerateFillReports,
    GenerateOrderStatusReport,
    GenerateOrderStatusReports,
    GeneratePositionStatusReports,
)
from nautilus_trader.model.enums import (
    LiquiditySide,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    TriggerType,
)
from nautilus_trader.model.events import OrderAccepted, OrderFilled, OrderSubmitted
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientOrderId,
    InstrumentId,
    StrategyId,
    TradeId,
    TraderId,
    VenueOrderId,
)
from nautilus_trader.model.objects import Currency, Money, Price, Quantity
from nautilus_trader.model.orders import LimitOrder, Order
from rithmic_nt_connect._order_plant import OrderPlantPolicy, OrderPlantState
from rithmic_nt_connect.errors import (
    ReconciliationUnavailableError,
    VenueQueryUnavailable,
)
from rithmic_nt_connect.execution import RithmicExecutionClient
from rithmic_nt_connect.session import WireSession


def _client() -> _TestClient:
    """Build a test double without the Cython base's ``__init__``.

    The base's ``_log`` / ``_clock`` / ``_cache`` / ``account_id`` are cdef
    read-only, so ``_TestClient`` re-exposes them as writable properties. The
    defaults here keep every code path loggable, clocked, and cache-backed.
    """
    client = _TestClient.__new__(_TestClient)
    client._log = _Log()
    client._clock = SimpleNamespace(timestamp_ns=lambda: 2)
    client._cache = _CacheStub()
    client.account_id = None
    return client


class _TestClient(RithmicExecutionClient):
    """Test double: skips the Cython base's ``__init__`` and exposes the cdef
    read-only ``_log`` / ``_clock`` / ``_cache`` / ``account_id`` as writable
    properties, so real adapter methods run on a bare instance."""

    # The cdef base declares these read-only; re-expose as writable via
    # name-mangled storage. ``object.__getattribute__`` / ``__setattr__`` keep
    # the dynamic slots invisible to the type checker (no ``type: ignore``).

    @property
    def _log(self) -> _Log:
        return cast(_Log, object.__getattribute__(self, "_TestClient__log"))

    @_log.setter
    def _log(self, value: _Log) -> None:
        object.__setattr__(self, "_TestClient__log", value)

    @property
    def _clock(self) -> SimpleNamespace:
        return cast(
            SimpleNamespace, object.__getattribute__(self, "_TestClient__clock")
        )

    @_clock.setter
    def _clock(self, value: SimpleNamespace) -> None:
        object.__setattr__(self, "_TestClient__clock", value)

    @property
    def _cache(self) -> _CacheStub:
        return cast(_CacheStub, object.__getattribute__(self, "_TestClient__cache"))

    @_cache.setter
    def _cache(self, value: _CacheStub) -> None:
        object.__setattr__(self, "_TestClient__cache", value)

    @property
    def account_id(self) -> AccountId | None:
        return cast(
            AccountId | None, object.__getattribute__(self, "_TestClient__account_id")
        )

    @account_id.setter
    def account_id(self, value: AccountId | None) -> None:
        object.__setattr__(self, "_TestClient__account_id", value)


class _Log:
    def debug(self, *args: object, **kwargs: object) -> None:
        pass

    def warning(self, *args: object, **kwargs: object) -> None:
        pass

    def error(self, *args: object, **kwargs: object) -> None:
        pass

    def exception(self, *args: object, **kwargs: object) -> None:
        pass


class _CacheOrder:
    """Cache presence marker; ``is_closed`` mirrors Nautilus 1.231's property."""

    def __init__(self, closed: bool = False) -> None:
        self._closed = closed

    @property
    def is_closed(self) -> bool:
        return self._closed


class _CacheStub:
    """Minimal cache backing ``_resolve_client_order_id`` / ``_venue_id_for``."""

    def __init__(self) -> None:
        self._venue_to_client: dict[str, ClientOrderId] = {}
        self._client_to_venue: dict[str, VenueOrderId] = {}
        # Mixed marker stubs and ``_OrderStub`` doubles (matches ``order()``'s
        # ``object | None`` return).
        self._orders: dict[str, object] = {}

    def client_order_id(self, venue_order_id: VenueOrderId) -> ClientOrderId | None:
        return self._venue_to_client.get(venue_order_id.value)

    def venue_order_id(self, client_order_id: ClientOrderId) -> VenueOrderId | None:
        return self._client_to_venue.get(client_order_id.value)

    def add_venue_order_id(self, client: ClientOrderId, venue: VenueOrderId) -> None:
        self._client_to_venue[client.value] = venue
        self._venue_to_client[venue.value] = client

    def order(self, client_order_id: ClientOrderId) -> object | None:
        # Presence in the cache is the adapter's source of truth for "tracked".
        return self._orders.get(client_order_id.value)


class _LoadOrdersSession:
    def __init__(self, events: list[dict[str, object]]) -> None:
        self._events = events

    def load_orders(self, start: int, end: int) -> list[dict[str, object]]:
        return self._events


def _fill_event(*, fill_id: str = "F1", basket: str = "B1") -> dict[str, object]:
    return {
        "type": "order_notification",
        "source": "exchange",
        "kind": "filled",
        "status": "COMPLETE",
        "basket_id": basket,
        "exchange_order_id": "E1",
        "symbol": "NQU6",
        "account_id": "ACC1",
        "fill_id": fill_id,
        "fill_size": 1,
        "fill_price": 21000.0,
        "quantity": 2,
        "total_fill_size": 2,
        "transaction_type": 1,
        "price_type": 1,
        "duration": 1,
        "ssboe": 1_700_000_000,
        "usecs": 0,
    }


def _status_event(
    *, kind: str, status: str, ts_event: int | None = None
) -> dict[str, object]:
    raw: dict[str, object] = {
        "type": "order_notification",
        "source": "rithmic",
        "kind": kind,
        "status": status,
        "basket_id": "B1",
        "symbol": "NQU6",
        "user_tag": "T1",
        "quantity": 2,
        "price_type": 1,
        "duration": 1,
        "transaction_type": 1,
    }
    if ts_event is not None:
        raw["ts_event_ns"] = ts_event
    return raw


def _trading_client(
    session: object | None = None,
    *,
    enable_trading: bool = True,
) -> RithmicExecutionClient:
    """A trading client with the order plant LIVE and a venue source wired.

    ``session=None`` uses a source that raises ``ReconciliationUnavailableError``.
    """
    client = _client()
    client._config_local = cast(Any, SimpleNamespace(enable_trading=enable_trading))
    client._order_plant = OrderPlantPolicy(OrderPlantState.LIVE)
    source = session if session is not None else _UnavailableLoadOrdersSession()
    client._session = cast(WireSession, source)
    client._seen_fill_keys = OrderedDict()
    return client


def _report_client(
    *,
    order_type: OrderType,
    tif: TimeInForce,
    status: OrderStatus,
    trigger_type: TriggerType = TriggerType.NO_TRIGGER,
) -> SimpleNamespace:
    """A stub whose ``_order_status_report_from_fields`` collaborators are fixed,
    so a test varies only the wire fields."""
    return SimpleNamespace(
        account_id="RITHMIC-ACC1",
        _clock=SimpleNamespace(timestamp_ns=lambda: 2),
        _seed_account_if_needed=lambda account_raw: None,
        _client_order_id_for_tag=lambda tag: None,
        _order_type_from_event=lambda fields: order_type,
        _tif_from_event=lambda fields: tif,
        _order_status_from_event=lambda fields: status,
        _trigger_type_from_event=lambda fields: trigger_type,
    )


def _fill_cmd() -> GenerateFillReports:
    return GenerateFillReports(
        instrument_id=None,
        venue_order_id=None,
        start=None,
        end=None,
        command_id=UUID4(),
        ts_init=1,
    )


def _status_cmd() -> GenerateOrderStatusReports:
    return GenerateOrderStatusReports(
        instrument_id=None,
        start=None,
        end=None,
        open_only=False,
        command_id=UUID4(),
        ts_init=1,
    )


def test_order_handler_failure_stops_order_poll_and_fails_closed() -> None:
    client = SimpleNamespace(
        _order_plant=OrderPlantPolicy(OrderPlantState.LIVE),
        _log=_Log(),
    )

    async def poll_session_event(poll_fn):
        return poll_fn()

    def poll_fn() -> dict[str, object] | None:
        return {"type": "order_notification"}

    def on_event(event: dict[str, object]) -> None:
        raise RuntimeError("handler regression")

    client._poll_session_event = poll_session_event
    awaitable = RithmicExecutionClient._plant_poll_loop(
        cast(RithmicExecutionClient, client),
        name="order",
        poll_fn=poll_fn,
        on_event=on_event,
        on_resync=lambda: None,
    )
    asyncio.run(awaitable)

    assert client._order_plant.state is OrderPlantState.DISCONNECTED


def test_untracked_order_reports_status_without_strategy_ownership() -> None:
    status_reports: list[object] = []
    client = SimpleNamespace(
        account_id="RITHMIC-ACC1",
        _clock=SimpleNamespace(timestamp_ns=lambda: 2),
        _log=_Log(),
        _send_order_status_report=status_reports.append,
        _seed_account_if_needed=lambda account_raw: None,
        _untracked_status_keys={},
    )
    status_report = SimpleNamespace(venue_order_id="B-EXTERNAL", client_order_id=None)
    client._order_status_report_from_fields = lambda event, ts: status_report
    fields = {
        "basket_id": "B-EXTERNAL",
        "symbol": "MNQU6",
        "account_id": "ACC1",
        "status": "open",
        "kind": "accepted",
        "price_type": 1,
        "duration": 1,
        "quantity": 1,
        "total_fill_size": 0,
        "transaction_type": 1,
    }

    RithmicExecutionClient._handle_untracked_notification(
        cast(RithmicExecutionClient, client), fields
    )

    assert len(status_reports) == 1
    assert status_reports[0] is status_report


def test_untracked_status_suppresses_unchanged_re_push() -> None:
    published: list[object] = []
    client = SimpleNamespace(
        account_id="RITHMIC-ACC1",
        _clock=SimpleNamespace(timestamp_ns=lambda: 2),
        _log=_Log(),
        _send_order_status_report=published.append,
        _seed_account_if_needed=lambda account_raw: None,
    )
    status = SimpleNamespace(
        venue_order_id="B-EXT", order_status="OPEN", filled_qty="1", avg_px="100.5"
    )
    client._order_status_report_from_fields = lambda event, ts: status
    client._untracked_status_keys = {}
    fields = {
        "basket_id": "B-EXT",
        "symbol": "MNQU6",
        "account_id": "ACC1",
        "status": "OPEN",
        "kind": "accepted",
        "price_type": 1,
        "duration": 1,
        "quantity": 1,
        "total_fill_size": 0,
        "transaction_type": 1,
    }

    handle = RithmicExecutionClient._handle_untracked_notification
    handle(cast(RithmicExecutionClient, client), fields)
    handle(cast(RithmicExecutionClient, client), fields)  # re-push

    assert len(published) == 1

    changed = SimpleNamespace(
        venue_order_id="B-EXT", order_status="FILLED", filled_qty="1", avg_px="100.5"
    )
    client._order_status_report_from_fields = lambda event, ts: changed
    RithmicExecutionClient._handle_untracked_notification(
        cast(RithmicExecutionClient, client), fields
    )

    assert len(published) == 2


def test_untracked_status_re_push_with_changed_terms_reports() -> None:
    """An external re-push that mutates order terms must not be deduped away.

    Regression for the Macroscope review: the dedup key only covered status +
    fill data, so an ACCEPTED re-push that changed quantity/price/trigger was
    discarded and Nautilus kept stale terms.
    """
    published: list[object] = []
    client = SimpleNamespace(
        account_id="RITHMIC-ACC1",
        _clock=SimpleNamespace(timestamp_ns=lambda: 2),
        _log=_Log(),
        _send_order_status_report=published.append,
        _seed_account_if_needed=lambda account_raw: None,
        _untracked_status_keys={},
    )
    status = SimpleNamespace(
        venue_order_id="B-EXT",
        order_status="OPEN",
        quantity="1",
        price="100.0",
        trigger_price="",
        filled_qty="0",
        avg_px="",
    )
    client._order_status_report_from_fields = lambda event, ts: status
    fields = {
        "basket_id": "B-EXT",
        "symbol": "MNQU6",
        "account_id": "ACC1",
        "status": "OPEN",
        "kind": "accepted",
        "price_type": 1,
        "duration": 1,
        "quantity": 1,
        "total_fill_size": 0,
        "transaction_type": 1,
    }

    handle = RithmicExecutionClient._handle_untracked_notification
    handle(cast(RithmicExecutionClient, client), fields)
    handle(cast(RithmicExecutionClient, client), fields)
    assert len(published) == 1

    # Same status + fill data, but the order quantity changed: must re-publish.
    changed = SimpleNamespace(
        venue_order_id="B-EXT",
        order_status="OPEN",
        quantity="2",
        price="100.0",
        trigger_price="",
        filled_qty="0",
        avg_px="",
    )
    client._order_status_report_from_fields = lambda event, ts: changed
    RithmicExecutionClient._handle_untracked_notification(
        cast(RithmicExecutionClient, client), fields
    )

    assert len(published) == 2


def test_untracked_status_publication_failure_does_not_escape_handler() -> None:
    client = SimpleNamespace(
        account_id="RITHMIC-ACC1",
        _clock=SimpleNamespace(timestamp_ns=lambda: 2),
        _log=_Log(),
        _send_order_status_report=lambda report: (_ for _ in ()).throw(
            RuntimeError("stale report")
        ),
        _seed_account_if_needed=lambda account_raw: None,
        _untracked_status_keys={},
    )
    status_report = SimpleNamespace(venue_order_id="B-EXTERNAL", client_order_id=None)
    client._order_status_report_from_fields = lambda event, ts: status_report

    RithmicExecutionClient._handle_untracked_notification(
        cast(RithmicExecutionClient, client),
        {
            "basket_id": "B-EXTERNAL",
            "symbol": "MNQU6",
            "account_id": "ACC1",
            "status": "CANCELED",
            "kind": "canceled",
        },
    )


# --------------------------------------------------------------------------- #
# #3812 producer guard: TRIGGER notifications only emit OrderTriggered for
# limit-style stops (STOP_LIMIT / TRAILING_STOP_LIMIT / LIMIT_IF_TOUCHED).
# Market-style stops (STOP_MARKET / MARKET_IF_TOUCHED) execute straight to
# FILLED and must never emit OrderTriggered (the 1.231.x model rejects it).
# --------------------------------------------------------------------------- #


@dataclass
class _OrderStub:
    """Minimal order double: the handler only reads identity fields + type."""

    client_order_id: ClientOrderId
    strategy_id: StrategyId
    instrument_id: InstrumentId
    order_type: OrderType
    side: OrderSide
    is_closed: bool = False


def _order(order_type: OrderType) -> _OrderStub:
    return _OrderStub(
        client_order_id=ClientOrderId("O-1"),
        strategy_id=StrategyId("STRATEGY-1"),
        instrument_id=InstrumentId.from_str("NQ.GLBX"),
        order_type=order_type,
        side=OrderSide.BUY,
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


@pytest.mark.parametrize(
    ("order_type", "emits_triggered"),
    [
        (OrderType.STOP_MARKET, False),
        (OrderType.MARKET_IF_TOUCHED, False),
        (OrderType.STOP_LIMIT, True),
        (OrderType.TRAILING_STOP_LIMIT, True),
        (OrderType.LIMIT_IF_TOUCHED, True),
    ],
)
def test_trigger_notification_emission_guarded_by_order_type(
    monkeypatch: pytest.MonkeyPatch,
    order_type: OrderType,
    emits_triggered: bool,
) -> None:
    """#3812: a venue TRIGGER only emits OrderTriggered for limit-style stops.

    Market-style stops go straight to FILLED on trigger; emitting
    OrderTriggered for them is rejected by the 1.231.x model, which would kill
    the order event stream.
    """
    client = _client()
    order = _order(order_type)
    client._cache._orders[str(order.client_order_id)] = order
    triggered: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        client, "generate_order_triggered", lambda *args: triggered.append(args)
    )

    RithmicExecutionClient._handle_order_notification(
        cast(RithmicExecutionClient, client), _trigger_notification()
    )

    assert bool(triggered) is emits_triggered
    if emits_triggered:
        assert len(triggered) == 1
        assert triggered[0][2] == ClientOrderId("O-1")


def test_stop_market_trigger_then_fill_still_emits_fill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A STOP_MARKET TRIGGER is skipped, and the follow-up FILL still works."""
    client = _client()
    order = _order(OrderType.STOP_MARKET)
    client._cache._orders[str(order.client_order_id)] = order
    client._seen_fill_keys = OrderedDict()
    triggered: list[tuple[object, ...]] = []
    filled: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        client, "generate_order_triggered", lambda *args: triggered.append(args)
    )
    monkeypatch.setattr(
        client,
        "generate_order_filled",
        lambda *args, **kwargs: filled.append((args, kwargs)),
    )
    monkeypatch.setattr(
        client,
        "_price_for_instrument",
        lambda instrument_id, value: Price.from_str("21000.0"),
    )

    RithmicExecutionClient._handle_order_notification(
        cast(RithmicExecutionClient, client), _trigger_notification()
    )
    assert triggered == []

    RithmicExecutionClient._handle_order_notification(
        cast(RithmicExecutionClient, client),
        {
            "type": "order_notification",
            "source": "exchange",
            "symbol": "NQU6",
            "kind": "filled",
            "notify_type_name": "FILL",
            "basket_id": "B1",
            "user_tag": "O-1",
            "fill_id": "F1",
            "fill_price": 21000.0,
            "fill_size": 1,
            "transaction_type": 1,
            "ssboe": 1_700_000_000,
        },
    )
    assert len(filled) == 1


def test_status_report_publication_failure_is_non_fatal_to_fill_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
# --------------------------------------------------------------------------- #
# #3812 producer guard: TRIGGER notifications only emit OrderTriggered for
# limit-style stops (STOP_LIMIT / TRAILING_STOP_LIMIT / LIMIT_IF_TOUCHED).
# Market-style stops (STOP_MARKET / MARKET_IF_TOUCHED) execute straight to
# FILLED and must never emit OrderTriggered (the 1.231.x model rejects it).
# --------------------------------------------------------------------------- #


@dataclass
class _OrderStub:
    """Minimal order double: the handler only reads identity fields + type."""

    client_order_id: ClientOrderId
    strategy_id: StrategyId
    instrument_id: InstrumentId
    order_type: OrderType
    side: OrderSide
    is_closed: bool = False


def _order(order_type: OrderType) -> _OrderStub:
    return _OrderStub(
        client_order_id=ClientOrderId("O-1"),
        strategy_id=StrategyId("STRATEGY-1"),
        instrument_id=InstrumentId.from_str("NQ.GLBX"),
        order_type=order_type,
        side=OrderSide.BUY,
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


@pytest.mark.parametrize(
    ("order_type", "emits_triggered"),
    [
        (OrderType.STOP_MARKET, False),
        (OrderType.MARKET_IF_TOUCHED, False),
        (OrderType.STOP_LIMIT, True),
        (OrderType.TRAILING_STOP_LIMIT, True),
        (OrderType.LIMIT_IF_TOUCHED, True),
    ],
)
def test_trigger_notification_emission_guarded_by_order_type(
    monkeypatch: pytest.MonkeyPatch,
    order_type: OrderType,
    emits_triggered: bool,
) -> None:
    """#3812: a venue TRIGGER only emits OrderTriggered for limit-style stops.

    Market-style stops go straight to FILLED on trigger; emitting
    OrderTriggered for them is rejected by the 1.231.x model, which would kill
    the order event stream.
    """
    client = _client()
    order = _order(order_type)
    client._cache._orders[str(order.client_order_id)] = order
    triggered: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        client, "generate_order_triggered", lambda *args: triggered.append(args)
    )

    RithmicExecutionClient._handle_order_notification(
        cast(RithmicExecutionClient, client), _trigger_notification()
    )

    assert bool(triggered) is emits_triggered
    if emits_triggered:
        assert len(triggered) == 1
        assert triggered[0][2] == ClientOrderId("O-1")


def test_stop_market_trigger_then_fill_still_emits_fill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A STOP_MARKET TRIGGER is skipped, and the follow-up FILL still works."""
    client = _client()
    order = _order(OrderType.STOP_MARKET)
    client._cache._orders[str(order.client_order_id)] = order
    client._seen_fill_keys = OrderedDict()
    triggered: list[tuple[object, ...]] = []
    filled: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        client, "generate_order_triggered", lambda *args: triggered.append(args)
    )
    monkeypatch.setattr(
        client,
        "generate_order_filled",
        lambda *args, **kwargs: filled.append((args, kwargs)),
    )
    monkeypatch.setattr(
        client,
        "_price_for_instrument",
        lambda instrument_id, value: Price.from_str("21000.0"),
    )

    RithmicExecutionClient._handle_order_notification(
        cast(RithmicExecutionClient, client), _trigger_notification()
    )
    assert triggered == []

    RithmicExecutionClient._handle_order_notification(
        cast(RithmicExecutionClient, client),
        {
            "type": "order_notification",
            "source": "exchange",
            "symbol": "NQU6",
            "kind": "filled",
            "notify_type_name": "FILL",
            "basket_id": "B1",
            "user_tag": "O-1",
            "fill_id": "F1",
            "fill_price": 21000.0,
            "fill_size": 1,
            "transaction_type": 1,
            "ssboe": 1_700_000_000,
        },
    )
    assert len(filled) == 1


def test_status_report_publication_failure_is_non_fatal_to_fill_reconciliation() -> None:
    client = _trading_client(_LoadOrdersSession([_fill_event()]))
    monkeypatch.setattr(
        client, "_fill_report_from_fields", lambda fields, ts_event: object()
    )
    monkeypatch.setattr(
        client, "_order_status_report_from_fields", lambda fields, ts_event: object()
    )
    monkeypatch.setattr(
        client,
        "_send_order_status_report",
        lambda report: (_ for _ in ()).throw(RuntimeError("stale report")),
    )

    reports = asyncio.run(client.generate_fill_reports(_fill_cmd()))

    assert reports == []


def test_fill_reconciliation_skips_fill_without_status_prerequisite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fill whose status prerequisite cannot be built must not be emitted.

    Regression for the Macroscope review: a fill with valid fill fields but
    malformed status fields was appended even though
    ``_order_status_report_from_fields`` returned ``None``, so Nautilus
    received a fill with no order prerequisite.
    """
    client = _trading_client(_LoadOrdersSession([_fill_event()]))
    monkeypatch.setattr(
        client, "_fill_report_from_fields", lambda fields, ts_event: object()
    )
    monkeypatch.setattr(
        client, "_order_status_report_from_fields", lambda fields, ts_event: None
    )
    monkeypatch.setattr(client, "_send_order_status_report", lambda report: None)

    reports = asyncio.run(client.generate_fill_reports(_fill_cmd()))

    assert reports == []


def test_replayed_fill_publishes_status_before_fill_and_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recovered fill must have an order prerequisite and replay once only."""
    client = _trading_client(_LoadOrdersSession([_fill_event(fill_id="REPLAY-1")]))
    status_report = object()
    fill_report = object()
    published: list[tuple[str, object]] = []
    monkeypatch.setattr(
        client,
        "_order_status_report_from_fields",
        lambda fields, ts_event: status_report,
    )
    monkeypatch.setattr(
        client, "_fill_report_from_fields", lambda fields, ts_event: fill_report
    )
    monkeypatch.setattr(
        client,
        "_send_order_status_report",
        lambda report: published.append(("status", report)),
    )

    first = asyncio.run(client.generate_fill_reports(_fill_cmd()))
    second = asyncio.run(client.generate_fill_reports(_fill_cmd()))

    assert first == [fill_report]
    assert second == []
    # The adapter publishes the prerequisite status immediately and returns the
    # fill report to the execution engine, which publishes it after this call.
    assert published == [("status", status_report)]


def test_partial_recovered_fill_maps_to_partial_status() -> None:
    client = _client()
    fields = {
        "kind": "accepted",
        "status": "OPEN",
        "quantity": 2,
        "total_fill_size": 1,
    }
    assert client._order_status_from_event(fields) is OrderStatus.PARTIALLY_FILLED


def test_recovered_external_fill_status_has_no_client_order_id() -> None:
    client = _report_client(
        order_type=OrderType.MARKET, tif=TimeInForce.DAY, status=OrderStatus.FILLED
    )
    fields = _fill_event(fill_id="EXTERNAL-1", basket="EXTERNAL-BASKET")
    fields.pop("user_tag", None)
    report = RithmicExecutionClient._order_status_report_from_fields(
        cast(RithmicExecutionClient, client), fields, 2
    )
    assert report is not None
    assert report.client_order_id is None


def test_recovered_reports_preserve_exact_instrument_and_account_identity() -> None:
    client = _report_client(
        order_type=OrderType.LIMIT, tif=TimeInForce.GTC, status=OrderStatus.ACCEPTED
    )
    for symbol in ("NQU6", "MNQU6"):
        fields = {
            "basket_id": f"B-{symbol}",
            "symbol": symbol,
            "account_id": "ACC1",
            "price_type": 1,
            "duration": 2,
            "kind": "accepted",
            "status": "OPEN",
            "transaction_type": 1,
            "quantity": 1,
            "total_fill_size": 0,
            "price": 21000.0,
        }
        report = RithmicExecutionClient._order_status_report_from_fields(
            cast(RithmicExecutionClient, client), fields, 2
        )
        assert report is not None
        assert str(report.instrument_id) == f"{symbol}.RITHMIC"
        assert str(report.account_id) == "RITHMIC-ACC1"


# --------------------------------------------------------------------------- #
# _order_status_from_event: a rejected cancel is still working, not REJECTED.
# --------------------------------------------------------------------------- #


def test_cancel_rejected_with_reject_status_is_accepted():
    client = _client()
    fields = {
        "kind": "cancel_rejected",
        "status": "REJECTED",
        "quantity": 1,
        "total_fill_size": 0,
    }
    assert client._order_status_from_event(fields) == OrderStatus.ACCEPTED


def test_cancel_rejected_with_cancellation_failed_is_accepted():
    client = _client()
    fields = {
        "kind": "cancel_rejected",
        "status": "CANCELLATION_FAILED",
        "quantity": 1,
        "total_fill_size": 0,
    }
    assert client._order_status_from_event(fields) == OrderStatus.ACCEPTED


def test_stop_order_reconciliation_preserves_trigger_type() -> None:
    client = _client()
    assert client._trigger_type_from_event({"price_type": 4}) is TriggerType.DEFAULT
    assert client._trigger_type_from_event({"price_type": 3}) is TriggerType.DEFAULT
    assert client._trigger_type_from_event({"price_type": 1}) is TriggerType.NO_TRIGGER


def test_stop_order_status_report_has_trigger_type() -> None:
    client = _report_client(
        order_type=OrderType.STOP_MARKET,
        tif=TimeInForce.DAY,
        status=OrderStatus.ACCEPTED,
        trigger_type=TriggerType.DEFAULT,
    )
    fields = {
        "basket_id": "B-STOP",
        "symbol": "MNQU6",
        "account_id": "ACC1",
        "price_type": 4,
        "duration": 1,
        "kind": "accepted",
        "status": "OPEN",
        "transaction_type": 1,
        "quantity": 1,
        "total_fill_size": 0,
        "trigger_price": 30263.25,
    }

    report = RithmicExecutionClient._order_status_report_from_fields(
        cast(RithmicExecutionClient, client), fields, 2
    )

    assert report is not None
    assert report.order_type.name == "STOP_MARKET"
    assert report.trigger_type is TriggerType.DEFAULT
    assert report.trigger_price == Price.from_str("30263.25")


def test_stop_query_replay_never_returns_none_trigger_type_or_attribute_error() -> None:
    """Regression for the prior stop ``query_order`` report failure."""
    client = _report_client(
        order_type=OrderType.STOP_LIMIT,
        tif=TimeInForce.GTC,
        status=OrderStatus.ACCEPTED,
        trigger_type=TriggerType.DEFAULT,
    )
    fields = {
        "basket_id": "B-QUERY-STOP",
        "symbol": "NQU6",
        "account_id": "ACC1",
        "price_type": 3,
        "duration": 2,
        "kind": "accepted",
        "status": "OPEN",
        "transaction_type": 1,
        "quantity": 1,
        "total_fill_size": 0,
        "price": 21010.0,
        "trigger_price": 21005.0,
    }
    report = RithmicExecutionClient._order_status_report_from_fields(
        cast(RithmicExecutionClient, client), fields, 2
    )
    assert report is not None
    assert report.trigger_type is TriggerType.DEFAULT
    assert report.trigger_price == Price.from_str("21005.00")


def _stop_recon_event() -> dict[str, object]:
    """A resting stop (price_type 4 = StopMarket) as the venue drain returns it."""
    return {
        "type": "order_notification",
        "source": "rithmic",
        "kind": "accepted",
        "status": "OPEN",
        "basket_id": "B-STOP-RECON",
        "symbol": "NQU6",
        "account_id": "ACC1",
        "user_tag": "S84",
        "quantity": 1,
        "total_fill_size": 0,
        "price_type": 4,  # Rithmic StopMarket
        "duration": 2,  # GTC
        "transaction_type": 1,
        "trigger_price": 30263.25,
    }


def test_generate_order_status_reports_preserves_stop_trigger_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Full recon drain: a resting stop's status report must keep
    ``TriggerType.DEFAULT`` and the native ``trigger_price``.

    Regression guard for the stop recon report failure (the report previously
    carried ``trigger_type=None`` / raised ``AttributeError``). Drives the same
    ``generate_order_status_reports`` code path the live TC-E84 exercises through
    Nautilus, with the venue drain stubbed to return one stop event.
    """
    client = _trading_client(_LoadOrdersSession([_stop_recon_event()]))
    client.account_id = "RITHMIC-ACC1"
    monkeypatch.setattr(client, "_seed_account_if_needed", lambda account_raw: None)
    monkeypatch.setattr(client, "_client_order_id_for_tag", lambda tag: None)

    reports = asyncio.run(client.generate_order_status_reports(_status_cmd()))

    assert len(reports) == 1
    report = reports[0]
    assert report.order_type.name == "STOP_MARKET"
    assert report.trigger_type is TriggerType.DEFAULT
    assert report.trigger_price == Price.from_str("30263.25")


def test_unknown_outcome_closes_order_gate_without_rejection() -> None:
    client = _client()
    client._order_plant = OrderPlantPolicy(OrderPlantState.LIVE)
    client._order_plant_latched = False
    client._mark_order_plant_failed("submit", RuntimeError("timeout after send"))
    assert client._order_plant.state is OrderPlantState.DISCONNECTED
    assert (
        client._order_status_from_event({"kind": "unknown", "status": "UNKNOWN"})
        is not OrderStatus.REJECTED
    )


def test_order_plant_failure_latches_across_resync() -> None:
    """A transport resync must not re-enable commands after an unknown outcome.

    Regression for the Macroscope review: ``_mark_order_plant_failed`` blocks
    commands, but a later channel error ran ``_resync_order_subscription``,
    which restored the plant to ``LIVE`` without resolving the original
    unknown-outcome operation.
    """
    client = _client()
    client._order_plant = OrderPlantPolicy(OrderPlantState.LIVE)
    client._order_plant_latched = False
    client._session = cast(
        WireSession,
        SimpleNamespace(
            disconnect_order_plant=lambda: None,
            subscribe_order_updates=lambda: None,
        ),
    )

    client._mark_order_plant_failed("submit", RuntimeError("timeout after send"))
    assert client._order_plant.state is OrderPlantState.DISCONNECTED

    asyncio.run(client._resync_order_subscription())

    assert client._order_plant.state is OrderPlantState.DISCONNECTED
    assert not client._order_plant.allow_submit()


def test_resync_without_latch_restores_live() -> None:
    """A plain channel-error resync (no unknown outcome) still recovers to LIVE."""
    client = _client()
    client._order_plant = OrderPlantPolicy(OrderPlantState.LIVE)
    client._order_plant_latched = False
    client._session = cast(
        WireSession,
        SimpleNamespace(
            disconnect_order_plant=lambda: None,
            subscribe_order_updates=lambda: None,
        ),
    )

    asyncio.run(client._resync_order_subscription())

    assert client._order_plant.state is OrderPlantState.LIVE


def test_cached_stop_order_status_report_preserves_native_order_metadata() -> None:
    client_order_id = ClientOrderId("S-CACHED")
    venue_order_id = VenueOrderId("V-STOP")
    order = SimpleNamespace(
        client_order_id=client_order_id,
        venue_order_id=None,
        instrument_id=InstrumentId.from_str("MNQU6.RITHMIC"),
        side=OrderSide.BUY,
        order_type=OrderType.STOP_MARKET,
        time_in_force=TimeInForce.DAY,
        status=OrderStatus.ACCEPTED,
        quantity=Quantity.from_int(1),
        filled_qty=Quantity.from_int(0),
        ts_accepted=1,
        ts_last=2,
        has_price=False,
        price=None,
        has_trigger_price=True,
        trigger_price=Price.from_str("30203.00"),
        trigger_type=TriggerType.DEFAULT,
        is_reduce_only=True,
        avg_px=0.0,  # resting stop: no fills yet
    )
    client = SimpleNamespace(
        _cache=SimpleNamespace(venue_order_id=lambda cid: venue_order_id),
        account_id=AccountId("RITHMIC-ACC1"),
    )

    report = RithmicExecutionClient._order_status_report_for(
        cast(RithmicExecutionClient, client), cast(Order, order), 3
    )

    assert report is not None
    assert report.venue_order_id == venue_order_id
    assert report.trigger_type is TriggerType.DEFAULT
    assert report.reduce_only is True
    assert report.avg_px is None


def test_true_reject_still_terminal_rejected():
    client = _client()
    fields = {
        "kind": "rejected",
        "status": "REJECTED",
        "quantity": 1,
        "total_fill_size": 0,
    }
    assert client._order_status_from_event(fields) == OrderStatus.REJECTED


# --------------------------------------------------------------------------- #
# _recon_window_sec: wrong units must not build a decades-wide window.
# --------------------------------------------------------------------------- #


def test_recon_window_clamps_wrong_units():
    client = _client()
    # epoch-seconds ints (where ns is expected) produce a ~1970 window; clamp it.
    start, end = client._recon_window_sec(1_700_000_000, 1_700_000_000 + 60)
    assert 0 <= start <= end
    assert end - start <= 32 * 86_400


def test_recon_window_clamps_inverted_bounds():
    client = _client()
    start, end = client._recon_window_sec(200, 100)
    assert end - start == 0


# --------------------------------------------------------------------------- #
# _resolve_client_order_id / _venue_id_for: external tags and venue-id recovery.
# --------------------------------------------------------------------------- #


def test_resolve_unknown_external_tag_without_basket_is_none():
    client = _client()
    # An external (untracked) notification must NOT resolve to a synthetic
    # ClientOrderId; it must fall through to the untracked path.
    assert client._resolve_client_order_id({"user_tag": "EXT-1"}) is None


def test_resolve_unknown_external_tag_falls_back_to_basket_cache():
    client = _client()
    coid = ClientOrderId("C1")
    client._cache._venue_to_client["B1"] = coid
    assert (
        client._resolve_client_order_id({"user_tag": "EXT-1", "basket_id": "B1"})
        == coid
    )


def test_resolve_known_tag_maps_to_client_order():
    client = _client()
    # A tag owned by this client resolves to a ClientOrderId only when the
    # order is actually tracked in the cache (the single source of truth).
    coid = ClientOrderId("C1")
    client._cache._orders["C1"] = _CacheOrder(closed=False)
    assert client._resolve_client_order_id({"user_tag": "C1"}) == coid


def test_resolve_tag_of_closed_order_goes_untracked():
    # A terminal order remains in the cache. A later external order reusing its
    # tag must NOT be attributed to the old (closed) order; it routes to the
    # untracked path instead.
    client = _client()
    client._cache._orders["C1"] = _CacheOrder(closed=True)
    assert client._resolve_client_order_id({"user_tag": "C1"}) is None


def test_venue_id_for_prefers_cached_venue_id_over_tag():
    client = _client()
    coid = ClientOrderId("C1")
    client._cache.add_venue_order_id(coid, VenueOrderId("VB1"))
    # No basket_id in the notification: the cached venue id must win, so
    # modify_rejected/cancel_rejected reference the real venue order.
    assert client._venue_id_for({"user_tag": "C1"}, coid) == "VB1"


def test_venue_id_for_falls_back_to_tag_when_not_cached():
    client = _client()
    coid = ClientOrderId("C1")
    assert client._venue_id_for({"user_tag": "C1"}, coid) == "C1"


def test_resolve_prefers_venue_basket_over_colliding_tag():
    # A tracked order owns the tag, but the notification's basket maps to a
    # different venue order; the basket (venue identity) must win so an external
    # order is not misattributed to our tracked order.
    client = _client()
    other = ClientOrderId("C2")
    client._cache._orders["C1"] = _CacheOrder(closed=False)
    client._cache._venue_to_client["B-EXT"] = other
    assert (
        client._resolve_client_order_id({"user_tag": "C1", "basket_id": "B-EXT"})
        == other
    )


def test_resolve_tag_when_basket_not_yet_bound():
    # First (accept) notification carries a basket that is not yet bound in the
    # cache; the tracked tag must still resolve the order by cache presence.
    client = _client()
    coid = ClientOrderId("C1")
    client._cache._orders["C1"] = _CacheOrder(closed=False)
    assert (
        client._resolve_client_order_id({"user_tag": "C1", "basket_id": "B1"}) == coid
    )


def test_publish_account_nonnumeric_balance_does_not_seed(
    monkeypatch: pytest.MonkeyPatch,
):
    client = _client()
    client._account_seeded = False
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr(
        RithmicExecutionClient,
        "_set_account_id",
        lambda self, aid: calls.append(("set", aid)),
    )
    monkeypatch.setattr(
        RithmicExecutionClient,
        "_seed_account_if_needed",
        lambda self, account_raw=None: calls.append(("seed", account_raw)),
    )
    # Non-numeric balance must not seed/publish fabricated funds.
    client._publish_account(
        {"type": "account_pnl", "account_id": "ACC1", "cash_on_hand": "abc"}
    )
    assert ("seed", "ACC1") not in calls


def test_publish_account_nonfinite_balance_does_not_seed(
    monkeypatch: pytest.MonkeyPatch,
):
    client = _client()
    client._account_seeded = False
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr(
        RithmicExecutionClient,
        "_set_account_id",
        lambda self, aid: calls.append(("set", aid)),
    )
    monkeypatch.setattr(
        RithmicExecutionClient,
        "_seed_account_if_needed",
        lambda self, account_raw=None: calls.append(("seed", account_raw)),
    )
    # Decimal("NaN")/Decimal("Infinity") are valid Decimal but must be rejected.
    client._publish_account(
        {"type": "account_pnl", "account_id": "ACC1", "cash_on_hand": "NaN"}
    )
    assert ("seed", "ACC1") not in calls


def test_publish_account_suppresses_unchanged_state() -> None:
    published: list[dict[str, Any]] = []

    def set_account_id(client: SimpleNamespace, account_id: object) -> None:
        client.account_id = account_id

    client = SimpleNamespace(
        account_id=None,
        _account_seeded=False,
        _clock=SimpleNamespace(timestamp_ns=lambda: 123),
        _set_account_id=lambda account_id: set_account_id(client, account_id),
        _seed_account_if_needed=lambda account_raw=None: None,
        generate_account_state=lambda **kwargs: published.append(kwargs),
    )
    event = {"type": "account_pnl", "account_id": "ACC1", "cash_on_hand": "49977.00"}

    publish = RithmicExecutionClient._publish_account
    publish(cast(RithmicExecutionClient, client), event)
    publish(cast(RithmicExecutionClient, client), dict(event))
    publish(
        cast(RithmicExecutionClient, client),
        {"type": "account_pnl", "account_id": "ACC1", "cash_on_hand": "49978.00"},
    )

    assert len(published) == 2
    assert published[0]["ts_event"] == 123
    assert published[1]["balances"][0].free.as_decimal() == 49978


# --------------------------------------------------------------------------- #
# _load_orders_events: an empty bounded drain is a valid best-effort answer.
# --------------------------------------------------------------------------- #


def test_load_orders_events_returns_empty_recon():
    client = _trading_client(_LoadOrdersSession([]))
    assert asyncio.run(client._load_orders_events(1, 2)) == []


# --------------------------------------------------------------------------- #
# generate_fill_reports: shares the adapter-wide fill dedup store.
# --------------------------------------------------------------------------- #


def _fill_reports_client(
    monkeypatch: pytest.MonkeyPatch,
    events: list[dict[str, object]],
    report: object | None = None,
) -> RithmicExecutionClient:
    """Client whose fill/status conversion is stubbed to return ``report``/a status.

    The status stub is a valid prerequisite so the fill paths under test are
    reached; the ``status is None`` (no-prerequisite) case has its own test.
    """
    if report is None:
        report = object()
    client = _trading_client(_LoadOrdersSession(events))
    monkeypatch.setattr(
        client, "_fill_report_from_fields", lambda fields, ts_event: report
    )
    monkeypatch.setattr(
        client,
        "_order_status_report_from_fields",
        lambda fields, ts_event: SimpleNamespace(venue_order_id="V-EXT"),
    )
    monkeypatch.setattr(client, "_send_order_status_report", lambda status_report: None)
    return client


def test_generate_fill_reports_suppresses_fill_seen_live(
    monkeypatch: pytest.MonkeyPatch,
):
    client = _fill_reports_client(monkeypatch, [_fill_event(fill_id="F1")])
    # The live path already marked this fill.
    client._mark_fill_key("ACC1|NQU6.RITHMIC|F1")
    reports = asyncio.run(client.generate_fill_reports(_fill_cmd()))
    assert reports == []


def test_generate_fill_reports_emits_and_marks_unseen_fill(
    monkeypatch: pytest.MonkeyPatch,
):
    report = object()
    client = _fill_reports_client(monkeypatch, [_fill_event(fill_id="F1")], report)
    reports = asyncio.run(client.generate_fill_reports(_fill_cmd()))
    assert reports == [report]
    assert client._fill_key_seen("ACC1|NQU6.RITHMIC|F1")


def test_generate_fill_reports_dedups_fill_without_venue_id(
    monkeypatch: pytest.MonkeyPatch,
):
    raw = _fill_event(fill_id="")
    report = object()
    client = _fill_reports_client(monkeypatch, [raw, raw], report)
    reports = asyncio.run(client.generate_fill_reports(_fill_cmd()))
    assert reports == [report]


# --------------------------------------------------------------------------- #
# generate_order_status_reports: latest row wins on equal timestamps.
# --------------------------------------------------------------------------- #


def test_order_status_reports_last_row_wins_on_equal_ts(
    monkeypatch: pytest.MonkeyPatch,
):
    client = _trading_client(
        _LoadOrdersSession(
            [
                _status_event(kind="accepted", status="OPEN", ts_event=0),
                _status_event(kind="canceled", status="CANCELLED", ts_event=0),
            ]
        )
    )
    monkeypatch.setattr(
        client,
        "_matches_instrument",
        lambda fields, instrument_id, venue_order_id: True,
    )
    monkeypatch.setattr(
        client,
        "_order_status_report_from_fields",
        lambda fields, ts_event: SimpleNamespace(order_status=fields["kind"]),
    )
    reports = asyncio.run(client.generate_order_status_reports(_status_cmd()))
    assert [r.order_status for r in reports] == ["canceled"]


# --------------------------------------------------------------------------- #
# Fail-closed recon: when the venue order-history source is unavailable, recon
# must raise (never return [] as authoritative venue-empty, never present local
# cache as venue state).
# --------------------------------------------------------------------------- #


class _UnavailableLoadOrdersSession:
    def load_orders(self, start: int, end: int) -> list[dict[str, object]]:
        raise ReconciliationUnavailableError("order-history reconciliation unavailable")


def test_fill_reports_raises_when_unavailable_source():
    client = _trading_client(session=_UnavailableLoadOrdersSession())
    with pytest.raises(ReconciliationUnavailableError, match="unavailable"):
        asyncio.run(client.generate_fill_reports(_fill_cmd()))


def test_fill_reports_raises_when_trading_disabled():
    client = _trading_client(enable_trading=False)
    with pytest.raises(VenueQueryUnavailable, match="unavailable"):
        asyncio.run(client.generate_fill_reports(_fill_cmd()))


def test_status_reports_raises_when_trading_source_unavailable():
    # A trading client whose order-history source is unavailable must not fall
    # back to local cache as authoritative venue state.
    client = _trading_client(session=_UnavailableLoadOrdersSession())
    with pytest.raises(ReconciliationUnavailableError, match="unavailable"):
        asyncio.run(client.generate_order_status_reports(_status_cmd()))


def test_status_reports_cache_backed_for_read_only(monkeypatch: pytest.MonkeyPatch):
    # A read-only client legitimately reports only its locally cached orders
    # (never claims venue authority); it must not raise.
    client = _trading_client(enable_trading=False)
    monkeypatch.setattr(
        client, "_cache_backed_order_status_reports", lambda cmd: ["cached"]
    )
    reports = asyncio.run(client.generate_order_status_reports(_status_cmd()))
    assert reports == ["cached"]


def test_load_orders_events_does_not_retry_unavailable_error():
    # Reconciliation-unavailable is non-retryable: fail on the first attempt,
    # not after 3 retries.
    calls = {"n": 0}

    class _CountingSession:
        def load_orders(self, start: int, end: int) -> list[dict[str, object]]:
            calls["n"] += 1
            raise ReconciliationUnavailableError(
                "order-history reconciliation unavailable"
            )

    client = _trading_client(session=_CountingSession())
    with pytest.raises(ReconciliationUnavailableError, match="unavailable"):
        asyncio.run(client._load_orders_events(1, 2))
    assert calls["n"] == 1


class _InstrumentCache:
    def __init__(self, loaded: set[str]) -> None:
        self._loaded = loaded

    def instrument(self, instrument_id: InstrumentId) -> str | None:
        key = str(instrument_id)
        return key if key in self._loaded else None


def _position_cmd() -> GeneratePositionStatusReports:
    return GeneratePositionStatusReports(
        instrument_id=None,
        start=None,
        end=None,
        command_id=UUID4(),
        ts_init=1,
    )


def test_position_status_reports_skip_unloaded_instruments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    client._positions = {
        "MNQU6.RITHMIC": {"instrument_id": "MNQU6.RITHMIC", "quantity": 0},
        "NQU6.RITHMIC": {"instrument_id": "NQU6.RITHMIC", "quantity": 0},
    }
    monkeypatch.setattr(client, "_cache", _InstrumentCache({"MNQU6.RITHMIC"}))
    emitted: list[str] = []
    monkeypatch.setattr(
        client,
        "_position_report_from_fields",
        lambda fields, ts_init: (
            emitted.append(str(fields["instrument_id"]))
            or f"R:{fields['instrument_id']}"
        ),
    )
    reports = asyncio.run(client.generate_position_status_reports(_position_cmd()))
    assert [str(r) for r in reports] == ["R:MNQU6.RITHMIC"]
    assert emitted == ["MNQU6.RITHMIC"]


def test_position_status_reports_warn_on_unloaded_nonzero_position(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    client._positions = {
        "NQU6.RITHMIC": {"instrument_id": "NQU6.RITHMIC", "quantity": 2},
    }
    monkeypatch.setattr(client, "_cache", _InstrumentCache(set()))
    warnings: list[str] = []
    monkeypatch.setattr(
        client,
        "_log",
        SimpleNamespace(warning=lambda msg, *a, **k: warnings.append(str(msg))),
    )
    emitted: list[str] = []
    monkeypatch.setattr(
        client,
        "_position_report_from_fields",
        lambda fields, ts_init: (
            emitted.append(str(fields["instrument_id"]))
            or f"R:{fields['instrument_id']}"
        ),
    )
    reports = asyncio.run(client.generate_position_status_reports(_position_cmd()))
    assert reports == []
    assert emitted == []
    assert len(warnings) == 1
    assert "NQU6.RITHMIC" in warnings[0]
    assert "qty=2" in warnings[0]


class _SingleOrderCache(_CacheStub):
    """Cache that resolves one real Nautilus order for status queries."""

    def __init__(self, order: object) -> None:
        super().__init__()
        self._only = order

    def order(self, client_order_id: ClientOrderId) -> object:
        return self._only


def _filled_limit_order() -> LimitOrder:
    """A real Nautilus order driven through SUBMITTED/ACCEPTED to FILLED."""
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
        Quantity.from_int(2),
        Price.from_str("21000.0"),
        UUID4(),
        0,
        TimeInForce.GTC,
    )
    order.apply(OrderSubmitted(trader, strategy, iid, cid, account, UUID4(), ts, ts))
    order.apply(
        OrderAccepted(
            trader, strategy, iid, cid, VenueOrderId("B-1"), account, UUID4(), ts, ts
        )
    )
    order.apply(
        OrderFilled(
            trader,
            strategy,
            iid,
            cid,
            VenueOrderId("B-1"),
            account,
            TradeId("T1"),
            None,  # position_id
            OrderSide.BUY,
            OrderType.LIMIT,
            Quantity.from_int(2),
            Price.from_str("21000.5"),
            Currency.from_str("USD"),
            Money(0, Currency.from_str("USD")),
            LiquiditySide.NO_LIQUIDITY_SIDE,
            UUID4(),
            ts,
            ts,
        )
    )
    return order


def test_cached_status_query_carries_avg_px_for_filled_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A status query for a FILLED order must report its average fill price.

    Nautilus ExecEngine warns on ``report.avg_px is None`` when reconciling a
    filled order, so the cache-backed pull path must not drop the fill price.
    """
    order = _filled_limit_order()
    client = _trading_client()
    client.account_id = AccountId("RITHMIC-ACC1")
    monkeypatch.setattr(client, "_cache", _SingleOrderCache(order))

    command = GenerateOrderStatusReport(
        None,
        ClientOrderId("O-1"),
        None,
        UUID4(),
        1,
    )
    report = asyncio.run(client.generate_order_status_report(command))

    assert report is not None
    assert report.order_status == OrderStatus.FILLED
    assert report.avg_px == Decimal("21000.5")


def test_cached_status_query_non_stop_order_does_not_crash_on_trigger_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only stop orders expose ``trigger_type`` on the 1.231.x model; a limit
    order status query must not raise and reports NO_TRIGGER."""
    order = _filled_limit_order()
    client = _trading_client()
    client.account_id = AccountId("RITHMIC-ACC1")
    monkeypatch.setattr(client, "_cache", _SingleOrderCache(order))

    command = GenerateOrderStatusReport(
        None,
        ClientOrderId("O-1"),
        None,
        UUID4(),
        1,
    )
    report = asyncio.run(client.generate_order_status_report(command))

    assert report is not None
    assert report.trigger_type == TriggerType.NO_TRIGGER
    assert report.avg_px == Decimal("21000.5")
