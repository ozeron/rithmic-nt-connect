"""Unit tests for exec recon honesty (empty recon, fill dedup, status mapping)."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from types import SimpleNamespace

import pytest
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.model.enums import OrderStatus

from rithmic_nt_connect._order_plant import OrderPlantPolicy
from rithmic_nt_connect._order_plant import OrderPlantState
from rithmic_nt_connect.errors import ReconciliationUnavailableError
from rithmic_nt_connect.errors import VenueQueryUnavailable
from rithmic_nt_connect.execution import RithmicExecutionClient


def _client() -> RithmicExecutionClient:
    """Construct without the Cython base (unit helpers only)."""
    return _TestClient.__new__(_TestClient)


class _TestClient(RithmicExecutionClient):
    """Subclass that supplies a logger; the Cython base's ``_log`` is read-only."""

    @property
    def _log(self) -> _Log:
        return _Log()


class _Log:
    def warning(self, *args: object, **kwargs: object) -> None:
        pass

    def error(self, *args: object, **kwargs: object) -> None:
        pass


class _EmptyLoadOrdersSession:
    def load_orders(self, start: int, end: int) -> list[dict[str, object]]:
        return []


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


def _status_event(*, kind: str, status: str, ts_event: int | None = None) -> dict[str, object]:
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


def _trading_client(session: _LoadOrdersSession) -> RithmicExecutionClient:
    client = _client()
    client._config_local = SimpleNamespace(enable_trading=True)
    client._order_plant = OrderPlantPolicy(OrderPlantState.LIVE)
    client._session = session
    client._seen_fill_keys = OrderedDict()
    return client


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


def test_true_reject_still_terminal_rejected():
    client = _client()
    fields = {"kind": "rejected", "status": "REJECTED", "quantity": 1, "total_fill_size": 0}
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
# _load_orders_events: a clean-but-empty drain is NOT authoritative venue-empty.
# --------------------------------------------------------------------------- #


def test_load_orders_events_raises_on_empty_recon():
    client = _trading_client(_EmptyLoadOrdersSession())
    with pytest.raises(RuntimeError, match="failed after 3 attempts") as exc_info:
        asyncio.run(client._load_orders_events(1, 2))
    assert exc_info.value.__cause__ is not None
    assert "no rows" in str(exc_info.value.__cause__)


# --------------------------------------------------------------------------- #
# generate_fill_reports: shares the adapter-wide fill dedup store.
# --------------------------------------------------------------------------- #


def test_generate_fill_reports_suppresses_fill_seen_live():
    client = _trading_client(_LoadOrdersSession([_fill_event(fill_id="F1")]))
    report = object()
    client._fill_report_from_fields = lambda fields, ts_event: report  # type: ignore[method-assign]
    # The live path already marked this fill.
    client._mark_fill_key("ACC1|NQU6.RITHMIC|F1")
    cmd = GenerateFillReports(
        instrument_id=None,
        venue_order_id=None,
        start=None,
        end=None,
        command_id=UUID4(),
        ts_init=1,
    )
    reports = asyncio.run(client.generate_fill_reports(cmd))
    assert reports == []


def test_generate_fill_reports_emits_and_marks_unseen_fill():
    client = _trading_client(_LoadOrdersSession([_fill_event(fill_id="F1")]))
    report = object()
    client._fill_report_from_fields = lambda fields, ts_event: report  # type: ignore[method-assign]
    cmd = GenerateFillReports(
        instrument_id=None,
        venue_order_id=None,
        start=None,
        end=None,
        command_id=UUID4(),
        ts_init=1,
    )
    reports = asyncio.run(client.generate_fill_reports(cmd))
    assert reports == [report]
    assert client._fill_key_seen("ACC1|NQU6.RITHMIC|F1")


def test_generate_fill_reports_dedups_fill_without_venue_id():
    raw = _fill_event(fill_id="")
    client = _trading_client(_LoadOrdersSession([raw, raw]))
    report = object()
    client._fill_report_from_fields = lambda fields, ts_event: report  # type: ignore[method-assign]
    cmd = GenerateFillReports(
        instrument_id=None,
        venue_order_id=None,
        start=None,
        end=None,
        command_id=UUID4(),
        ts_init=1,
    )
    reports = asyncio.run(client.generate_fill_reports(cmd))
    assert reports == [report]


# --------------------------------------------------------------------------- #
# generate_order_status_reports: latest row wins on equal timestamps.
# --------------------------------------------------------------------------- #


def test_order_status_reports_last_row_wins_on_equal_ts():
    client = _trading_client(
        _LoadOrdersSession(
            [
                _status_event(kind="accepted", status="OPEN", ts_event=0),
                _status_event(kind="canceled", status="CANCELLED", ts_event=0),
            ]
        )
    )
    client._matches_instrument = lambda fields, instrument_id, venue_order_id: True  # type: ignore[method-assign]
    client._order_status_report_from_fields = (  # type: ignore[method-assign]
        lambda fields, ts_event: SimpleNamespace(order_status=fields["kind"])
    )
    cmd = GenerateOrderStatusReports(
        instrument_id=None,
        start=None,
        end=None,
        open_only=False,
        command_id=UUID4(),
        ts_init=1,
    )
    reports = asyncio.run(client.generate_order_status_reports(cmd))
    assert [r.order_status for r in reports] == ["canceled"]


# --------------------------------------------------------------------------- #
# Fail-closed recon: when the venue order-history source is unavailable, recon
# must raise (never return [] as authoritative venue-empty, never present local
# cache as venue state).
# --------------------------------------------------------------------------- #


class _UnavailableLoadOrdersSession:
    def load_orders(self, start: int, end: int) -> list[dict[str, object]]:
        raise ReconciliationUnavailableError("order-history reconciliation unavailable")


def _client_unavailable(
    *,
    enable_trading: bool = True,
    load_available: bool = False,
    session: object | None = None,
) -> RithmicExecutionClient:
    client = _client()
    client._config_local = SimpleNamespace(enable_trading=enable_trading)
    client._order_plant = OrderPlantPolicy(
        OrderPlantState.LIVE if load_available else OrderPlantState.DISCONNECTED
    )
    client._session = session if session is not None else _UnavailableLoadOrdersSession()
    client._seen_fill_keys = OrderedDict()
    return client


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


def test_fill_reports_raises_when_unavailable_source():
    client = _client_unavailable(session=_UnavailableLoadOrdersSession())
    with pytest.raises(ReconciliationUnavailableError, match="unavailable"):
        asyncio.run(client.generate_fill_reports(_fill_cmd()))


def test_fill_reports_raises_when_trading_disabled():
    client = _client_unavailable(enable_trading=False)
    with pytest.raises(VenueQueryUnavailable, match="unavailable"):
        asyncio.run(client.generate_fill_reports(_fill_cmd()))


def test_status_reports_raises_when_trading_source_unavailable():
    # A trading client whose order-history source is unavailable must not fall
    # back to local cache as authoritative venue state.
    client = _client_unavailable(load_available=True, session=_UnavailableLoadOrdersSession())
    with pytest.raises(ReconciliationUnavailableError, match="unavailable"):
        asyncio.run(client.generate_order_status_reports(_status_cmd()))


def test_status_reports_cache_backed_for_read_only():
    # A read-only client legitimately reports only its locally cached orders
    # (never claims venue authority); it must not raise.
    client = _client_unavailable(enable_trading=False)
    client._cache_backed_order_status_reports = lambda cmd: ["cached"]  # type: ignore[method-assign]
    reports = asyncio.run(client.generate_order_status_reports(_status_cmd()))
    assert reports == ["cached"]


def test_load_orders_events_does_not_retry_unavailable_error():
    # Reconciliation-unavailable is non-retryable: fail on the first attempt,
    # not after 3 retries.
    calls = {"n": 0}

    class _CountingSession:
        def load_orders(self, start: int, end: int) -> list[dict[str, object]]:
            calls["n"] += 1
            raise ReconciliationUnavailableError("order-history reconciliation unavailable")

    client = _client_unavailable(session=_CountingSession())
    with pytest.raises(ReconciliationUnavailableError, match="unavailable"):
        asyncio.run(client._load_orders_events(1, 2))
    assert calls["n"] == 1