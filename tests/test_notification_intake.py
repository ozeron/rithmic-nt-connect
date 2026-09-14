"""Unit tests for VenueNotificationIntake (U2)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from _stubs import _CacheStub, _Log, _TestClient
from nautilus_trader.model.enums import OrderSide, OrderStatus, OrderType
from nautilus_trader.model.identifiers import (
    ClientOrderId,
    InstrumentId,
    StrategyId,
    VenueOrderId,
)
from nautilus_trader.model.objects import Money, Price, Quantity
from rithmic_nt_connect._orders import (
    FillDedupStore,
    UntrackedStatusBook,
    fill_dedup_key,
)
from rithmic_nt_connect.execution import RithmicExecutionClient
from rithmic_nt_connect.notification_intake import VenueNotificationIntake


def _drain_row_result(status: object) -> SimpleNamespace:
    return SimpleNamespace(report=status, bindable=False, fields={}, ts_event=0)


def _host(**over: Any) -> SimpleNamespace:
    host = SimpleNamespace(
        account_id="RITHMIC-ACC1",
        _clock=SimpleNamespace(timestamp_ns=lambda: 2),
        _log=_Log(),
        _cache=_CacheStub(),
        _seen_fill_keys=FillDedupStore(),
        _untracked_status_keys=UntrackedStatusBook(),
        _seed_account_if_needed=lambda account_raw: None,
        _fill_key_seen=lambda key: False,
        _mark_fill_key=lambda key: None,
        _basket_client_id=lambda fields: None,
        _send_order_status_report=lambda report: None,
        _send_fill_report=lambda report: None,
        _fill_report_from_fields=lambda fields, ts_event: None,
        _drain_row_from_fields=lambda fields, ts_event: _drain_row_result(None),
    )
    for key, value in over.items():
        setattr(host, key, value)

    def _fill_key_seen(key: str) -> bool:
        return host._seen_fill_keys.has_seen(key)

    def _mark_fill_key(key: str) -> None:
        host._seen_fill_keys.mark(key)

    def _publish_order_status_report(report: object, *, context: str) -> bool:
        try:
            host._send_order_status_report(report)
        except Exception as exc:
            host._log.exception(f"{context}: status report publication failed", exc)
            return False
        return True

    host._fill_key_seen = _fill_key_seen
    host._mark_fill_key = _mark_fill_key
    if "_publish_order_status_report" not in over:
        host._publish_order_status_report = _publish_order_status_report
    return host


def _accepted_order(status: OrderStatus) -> SimpleNamespace:
    return SimpleNamespace(
        status=status,
        strategy_id=StrategyId("STRATEGY-1"),
        instrument_id=InstrumentId.from_str("NQ.GLBX"),
    )


def test_emit_accepted_only_while_submitted() -> None:
    accepted: list[tuple[object, ...]] = []
    host = _host(generate_order_accepted=lambda *args: accepted.append(args))
    intake = VenueNotificationIntake(host)

    intake.emit_accepted(
        _accepted_order(OrderStatus.SUBMITTED),
        ClientOrderId("O-1"),
        VenueOrderId("B1"),
        5,
    )
    assert len(accepted) == 1

    accepted.clear()
    intake.emit_accepted(
        _accepted_order(OrderStatus.ACCEPTED),
        ClientOrderId("O-1"),
        VenueOrderId("B1"),
        5,
    )
    assert accepted == []


def test_untracked_suppresses_unchanged_and_republishes_on_qty_change() -> None:
    published: list[object] = []
    status = SimpleNamespace(
        venue_order_id="B-EXT",
        order_status="OPEN",
        quantity="1",
        price="100.0",
        trigger_price="",
        filled_qty="0",
        avg_px="",
    )
    host = _host(
        _send_order_status_report=published.append,
        _drain_row_from_fields=lambda fields, ts_event: _drain_row_result(status),
    )
    intake = VenueNotificationIntake(host)
    fields = {
        "basket_id": "B-EXT",
        "symbol": "MNQU6",
        "account_id": "ACC1",
        "status": "OPEN",
        "kind": "accepted",
    }

    intake.handle_untracked_notification(fields)
    intake.handle_untracked_notification(fields)
    assert len(published) == 1

    changed = SimpleNamespace(
        venue_order_id="B-EXT",
        order_status="OPEN",
        quantity="2",
        price="100.0",
        trigger_price="",
        filled_qty="0",
        avg_px="",
    )
    host._drain_row_from_fields = lambda fields, ts_event: _drain_row_result(changed)
    intake.handle_untracked_notification(fields)
    assert len(published) == 2


def test_untracked_status_failure_suppresses_fill() -> None:
    fills: list[object] = []
    status = SimpleNamespace(
        venue_order_id="B-EXT", order_status="FILLED", filled_qty="1", avg_px="100.5"
    )

    def _fail_publish(report: object) -> None:
        raise RuntimeError("bus down")

    host = _host(
        _send_order_status_report=_fail_publish,
        _send_fill_report=fills.append,
        _drain_row_from_fields=lambda fields, ts_event: _drain_row_result(status),
        _fill_report_from_fields=lambda fields, ts_event: object(),
    )
    intake = VenueNotificationIntake(host)
    intake.handle_untracked_notification(
        {
            "basket_id": "B-EXT",
            "symbol": "MNQU6",
            "account_id": "ACC1",
            "kind": "filled",
            "status": "FILLED",
            "fill_price": 21000.0,
            "fill_size": 1,
            "fill_id": "F-EXT",
        }
    )
    assert fills == []


def test_untracked_fill_marks_dedup_after_publish() -> None:
    fills: list[object] = []
    status = SimpleNamespace(
        venue_order_id="B-EXT",
        order_status="FILLED",
        quantity="1",
        price="",
        trigger_price="",
        filled_qty="1",
        avg_px="21000.0",
    )
    host = _host(
        _send_order_status_report=lambda report: None,
        _send_fill_report=fills.append,
        _drain_row_from_fields=lambda fields, ts_event: _drain_row_result(status),
        _fill_report_from_fields=lambda fields, ts_event: object(),
    )
    intake = VenueNotificationIntake(host)
    fields = {
        "basket_id": "B-EXT",
        "symbol": "MNQU6",
        "account_id": "ACC1",
        "kind": "filled",
        "status": "FILLED",
        "fill_price": 21000.0,
        "fill_size": 1,
        "fill_id": "F-EXT",
        "instrument_id": "MNQU6.RITHMIC",
    }
    intake.handle_untracked_notification(fields)
    assert len(fills) == 1
    dedup = fill_dedup_key(fields, ts_event=2)
    assert host._fill_key_seen(dedup)

    intake.handle_untracked_notification(fields)
    assert len(fills) == 1


def test_tracked_fill_marks_dedup_after_publish() -> None:
    filled: list[object] = []
    latches: list[str] = []
    order = SimpleNamespace(
        strategy_id=StrategyId("STRATEGY-1"),
        instrument_id=InstrumentId.from_str("MNQU6.RITHMIC"),
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        leaves_qty=Quantity.from_int(1),
    )
    host = _host(
        generate_order_filled=lambda *args, **kwargs: filled.append((args, kwargs)),
        _price_for_instrument=lambda instrument_id, value: Price.from_str("21000.0"),
        _commission_money=lambda product_code, qty: Money.from_str("0.00 USD"),
        _product_code_for_fill=lambda instrument_id, symbol: "MNQ",
        _latch_order_plant=lambda action, reason: latches.append(action),
    )
    intake = VenueNotificationIntake(host)
    fields = {
        "basket_id": "B1",
        "symbol": "MNQU6",
        "account_id": "ACC1",
        "kind": "filled",
        "fill_id": "F1",
        "fill_price": 21000.0,
        "fill_size": 1,
    }
    action = SimpleNamespace(fill_qty=1, fill_px=21000.0, trade_id="F1")
    intake.handle_tracked_fill(
        order,
        ClientOrderId("O-1"),
        VenueOrderId("B1"),
        fields,
        5,
        action,
    )
    assert len(filled) == 1
    dedup = fill_dedup_key(fields, ts_event=5)
    assert host._fill_key_seen(dedup)
    assert latches == []

    intake.handle_tracked_fill(
        order,
        ClientOrderId("O-1"),
        VenueOrderId("B1"),
        fields,
        5,
        action,
    )
    assert len(filled) == 1


def test_unpriceable_tracked_fill_does_not_mark_dedup() -> None:
    filled: list[object] = []
    latches: list[str] = []
    order = SimpleNamespace(
        strategy_id=StrategyId("STRATEGY-1"),
        instrument_id=InstrumentId.from_str("MNQU6.RITHMIC"),
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        leaves_qty=Quantity.from_int(1),
    )
    host = _host(
        generate_order_filled=lambda *args, **kwargs: filled.append((args, kwargs)),
        _price_for_instrument=lambda instrument_id, value: (_ for _ in ()).throw(
            ValueError("fill price missing (pending/sentinel)")
        ),
        _commission_money=lambda product_code, qty: Money.from_str("0.00 USD"),
        _product_code_for_fill=lambda instrument_id, symbol: "MNQ",
        _latch_order_plant=lambda action, reason: latches.append(action),
    )
    intake = VenueNotificationIntake(host)
    fields = {
        "basket_id": "B1",
        "symbol": "MNQU6",
        "account_id": "ACC1",
        "kind": "filled",
        "fill_id": "F-UNPRICED",
        "fill_size": 1,
    }
    action = SimpleNamespace(fill_qty=1, fill_px=None, trade_id="F-UNPRICED")
    intake.handle_tracked_fill(
        order,
        ClientOrderId("O-1"),
        VenueOrderId("B1"),
        fields,
        5,
        action,
    )
    assert filled == []
    assert latches == ["fill suppressed"]
    assert not host._fill_key_seen(fill_dedup_key(fields, ts_event=5))


def test_client_thin_delegates_forward_to_intake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R12: private method names remain callable on the client class."""
    client = _TestClient.__new__(_TestClient)
    client._log = _Log()
    client._clock = SimpleNamespace(timestamp_ns=lambda: 2)
    client._cache = _CacheStub()
    accepted: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        client, "generate_order_accepted", lambda *args: accepted.append(args)
    )

    RithmicExecutionClient._emit_accepted(
        cast(RithmicExecutionClient, client),
        _accepted_order(OrderStatus.SUBMITTED),
        ClientOrderId("O-1"),
        VenueOrderId("B1"),
        5,
    )
    assert len(accepted) == 1
    assert isinstance(
        client._notification_intake_inst,  # ty: ignore[unresolved-attribute]
        VenueNotificationIntake,
    )


@pytest.mark.parametrize(
    ("status", "emits"),
    [
        (OrderStatus.SUBMITTED, True),
        (OrderStatus.ACCEPTED, False),
    ],
)
def test_client_delegate_lap42_accepted(
    monkeypatch: pytest.MonkeyPatch, status: OrderStatus, emits: bool
) -> None:
    client = _TestClient.__new__(_TestClient)
    client._log = _Log()
    accepted: list[object] = []
    monkeypatch.setattr(
        client, "generate_order_accepted", lambda *args: accepted.append(args)
    )
    RithmicExecutionClient._emit_accepted(
        cast(RithmicExecutionClient, client),
        _accepted_order(status),
        ClientOrderId("O-1"),
        VenueOrderId("B1"),
        5,
    )
    assert bool(accepted) is emits
