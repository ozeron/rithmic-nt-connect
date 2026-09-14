"""Unit tests for WorkingOrdersDrain (U3): stale authority and row pipeline."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from _stubs import _CacheStub, _Log
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.identifiers import ClientOrderId, VenueOrderId
from rithmic_nt_connect.errors import VenueQueryUnavailable
from rithmic_nt_connect.execution import order_status_from_fields
from rithmic_nt_connect.recon import (
    DrainRowResult,
    WorkingOrdersDrain,
    row_is_trustworthy,
)


def _report(status: OrderStatus = OrderStatus.ACCEPTED) -> OrderStatusReport:
    return cast(OrderStatusReport, SimpleNamespace(order_status=status))


def _row(
    *,
    basket: str = "B1",
    ts_event: int = 10,
    status: OrderStatus = OrderStatus.ACCEPTED,
    bindable: bool = True,
    fields: dict[str, Any] | None = None,
) -> DrainRowResult:
    payload = fields or {"basket_id": basket, "user_tag": "O-1"}
    return DrainRowResult(payload, ts_event, _report(status), bindable)


def _status_report_from_fields(
    fields: dict[str, Any], ts_event: int
) -> OrderStatusReport:
    return _report(order_status_from_fields(fields))


def _host(**over: Any) -> SimpleNamespace:
    status_builder = over.pop(
        "_order_status_report_from_fields", _status_report_from_fields
    )
    host = SimpleNamespace(
        _log=_Log(),
        _cache=_CacheStub(),
        _matches_instrument=lambda fields, instrument_id, venue_order_id: True,
        _drain_client_order_id=lambda fields: ClientOrderId(
            str(fields.get("user_tag") or "O-1")
        ),
        _publish_order_status_report=lambda report, *, context: True,
        _bind_venue_id=lambda cid, basket: host._cache.add_venue_order_id(
            cid, VenueOrderId(basket)
        ),
    )
    for key, value in over.items():
        setattr(host, key, value)
    # Iterator calls through host._drain_row_from_fields (R12 spy seam).
    # Status builder is bound on the drain instance to avoid thin-delegate
    # recursion once the host method points at WorkingOrdersDrain.
    if "_drain_row_from_fields" not in over:
        drain = WorkingOrdersDrain(host)
        drain.order_status_report_from_fields = status_builder  # type: ignore[method-assign]
        host._drain_row_from_fields = drain.drain_row_from_fields
        host._order_status_report_from_fields = status_builder
        host._working_orders_drain_inst = drain
    return host


def _raw_row(
    *,
    basket: str = "B1",
    kind: str = "accepted",
    status: str = "OPEN",
    ssboe: int = 1,
    tag: str = "O-1",
) -> dict[str, object]:
    return {
        "type": "order_notification",
        "source": "rithmic",
        "basket_id": basket,
        "user_tag": tag,
        "symbol": "NQU6",
        "account_id": "ACC1",
        "quantity": 1,
        "total_fill_size": 0,
        "transaction_type": 1,
        "price_type": 1,
        "duration": 1,
        "kind": kind,
        "status": status,
        "ssboe": ssboe,
        "usecs": 0,
    }


def test_row_is_trustworthy_requires_real_closed_set_terms() -> None:
    good = {
        "transaction_type": 1,
        "price_type": 1,
        "duration": 1,
        "kind": "accepted",
        "status": "OPEN",
        "quantity": 1,
    }
    assert row_is_trustworthy(good)
    missing = dict(good)
    del missing["price_type"]
    assert not row_is_trustworthy(missing)
    bools = dict(good)
    bools["price_type"] = True
    bools["duration"] = True
    assert not row_is_trustworthy(bools)


def test_latest_prefers_higher_ts_then_last_arrived() -> None:
    drain = WorkingOrdersDrain(_host())
    events = [
        _raw_row(kind="accepted", status="OPEN", ssboe=1),
        _raw_row(kind="canceled", status="CANCELED", ssboe=2),
    ]
    latest = drain.latest_drain_rows(events)
    assert set(latest) == {"B1"}
    assert latest["B1"].report is not None
    assert latest["B1"].report.order_status is OrderStatus.CANCELED  # type: ignore[union-attr]


def test_iter_skips_missing_basket_and_malformed() -> None:
    drain = WorkingOrdersDrain(_host())
    events = [
        _raw_row(basket=""),
        {**_raw_row(), "quantity": 0},
        _raw_row(basket="B-OK"),
    ]

    # Empty basket skipped by iterator; qty=0 yields report None via real builder
    # only when using the client's builder — our stub always returns a report, so
    # also stub None for zero qty.
    def _status_or_none(fields: dict[str, Any], ts: int) -> OrderStatusReport | None:
        if int(fields.get("quantity") or 0) <= 0:
            return None
        return _status_report_from_fields(fields, ts)

    host = _host(_order_status_report_from_fields=_status_or_none)
    drain = WorkingOrdersDrain(host)
    rows = list(drain.iter_drain_rows(events))
    assert [str(r.fields["basket_id"]) for r in rows] == ["B-OK"]


def test_rearm_stale_closed_local_and_older_than_ts_last() -> None:
    host = _host()
    cid = ClientOrderId("O-1")
    host._cache._orders["O-1"] = SimpleNamespace(is_closed=True, ts_last=0)
    drain = WorkingOrdersDrain(host)
    assert (
        drain.row_stale_reason(cid, _row(), live_stream_authoritative=True)
        == "order closed locally"
    )

    host._cache._orders["O-1"] = SimpleNamespace(is_closed=False, ts_last=100)
    assert (
        drain.row_stale_reason(cid, _row(ts_event=50), live_stream_authoritative=True)
        == "live stream advanced past the snapshot"
    )
    # Missing ts (0) is not stale-skipped under re-arm.
    assert (
        drain.row_stale_reason(cid, _row(ts_event=0), live_stream_authoritative=True)
        is None
    )


def test_bulk_stale_suppresses_non_terminal_for_closed_only() -> None:
    host = _host()
    cid = ClientOrderId("O-1")
    host._cache._orders["O-1"] = SimpleNamespace(is_closed=True, ts_last=0)
    drain = WorkingOrdersDrain(host)
    assert (
        drain.row_stale_reason(
            cid,
            _row(status=OrderStatus.ACCEPTED),
            live_stream_authoritative=False,
        )
        == "non-terminal snapshot for locally closed order"
    )
    assert (
        drain.row_stale_reason(
            cid,
            _row(status=OrderStatus.CANCELED),
            live_stream_authoritative=False,
        )
        is None
    )


def test_apply_publishes_before_bind_and_aborts_on_publish_failure() -> None:
    published: list[object] = []
    bound: list[tuple[object, str]] = []
    cid = ClientOrderId("O-1")
    host = _host(
        _publish_order_status_report=lambda report, *, context: (
            published.append(report) or True
        ),
        _bind_venue_id=lambda c, basket: bound.append((c, basket)),
        _drain_client_order_id=lambda fields: cid,
    )
    host._cache._orders["O-1"] = SimpleNamespace(is_closed=False, ts_last=0)
    drain = WorkingOrdersDrain(host)
    events = [_raw_row()]
    drain.apply_drain_rows(events)
    assert len(published) == 1
    assert bound == [(cid, "B1")]

    bound.clear()
    host2 = _host(
        _publish_order_status_report=lambda report, *, context: False,
        _bind_venue_id=lambda c, basket: bound.append((c, basket)),
        _drain_client_order_id=lambda fields: cid,
    )
    host2._cache._orders["O-1"] = SimpleNamespace(is_closed=False, ts_last=0)
    with pytest.raises(VenueQueryUnavailable, match="aborted"):
        WorkingOrdersDrain(host2).apply_drain_rows(events)
    assert bound == []
