"""Engine-level repro of the two MY043-001 live-log warnings (2026-08-21).

Drives a real Nautilus ``LiveExecutionEngine`` (no creds) to pin the exact
mechanisms behind the production lines, so adapter changes cannot silently
re-introduce or hide them:

- ``report.avg_px was 'None' when a value was expected``: the cache-backed
  status-query answer for a bracket stop whose OPEN Rithmic defers (local
  SUBMITTED, unfilled, avg_px None) carries a non-terminal report status,
  which falls through the engine's status-transition table into fill
  reconciliation where the warning lives (live/execution_engine.py).
- ``InvalidStateTrigger: CANCELED -> ACCEPTED``: an ACCEPTED-status report
  (the venue still lists a just-canceled working order in a drain/open-check)
  lands on a locally CANCELED leg and is applied by the engine without a
  monotonic guard — the #27 LAP-42 notification-path guard does not cover
  this recon entry point.

Each scenario runs in a subprocess: the nautilus Rust logger writes to the
process stdout outside pytest's capture layers, so asserting on captured
subprocess output is deterministic (an in-process fd redirect raced pytest's
nested capture and flaked).
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
from typing import Any

import pytest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus, init_logging
from nautilus_trader.common.factories import OrderFactory
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.live.config import LiveExecEngineConfig
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.model.enums import (
    OrderSide,
    OrderStatus,
    TimeInForce,
    TriggerType,
)
from nautilus_trader.model.events import OrderAccepted, OrderCanceled, OrderSubmitted
from nautilus_trader.model.identifiers import (
    AccountId,
    StrategyId,
    TraderId,
    VenueOrderId,
)
from nautilus_trader.model.objects import Price, Quantity
from rithmic_nt_connect.providers import future_from_reference

TRADER_ID = TraderId("QGW-000")
STRATEGY_ID = StrategyId("MY043-001")
ACCOUNT_ID = AccountId("RITHMIC-LFE050-N9M415CZ-TEST001")

_NQU6_REF = {
    "trading_symbol": "NQU6",
    "trading_exchange": "CME",
    "underlying": "NQ",
    "product_code": "NQ",
    "currency": "USD",
    "tick_size": 0.25,
    "point_value": 2.0,
    "price_precision": 2,
    "expiration_date": "20260918",
    "is_tradable": True,
}

_AVG_PX_WARN = "report.avg_px was `None` when a value was expected"
_INVALID_STATE_WARN = "InvalidStateTrigger: CANCELED -> ACCEPTED"


def _init_logging() -> None:
    """Nautilus Logger emits nothing until the logging subsystem is up."""
    from nautilus_trader.core.nautilus_pyo3 import LogLevel

    # NOTE: nautilus orders LogLevel by increasing verbosity
    # (ERROR < WARN < INFO), so INFO passes WARN records through.
    init_logging(level_stdout=LogLevel.INFO, colors=False)


def _stop_order() -> tuple[LiveExecutionEngine, Any]:
    instrument = future_from_reference(_NQU6_REF)
    cache = Cache()
    cache.add_instrument(instrument)
    clock = LiveClock()
    msgbus = MessageBus(trader_id=TRADER_ID, clock=clock)  # ty: ignore[missing-argument, unknown-argument]
    engine = LiveExecutionEngine(
        loop=asyncio.new_event_loop(),
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        config=LiveExecEngineConfig(),
    )
    factory = OrderFactory(trader_id=TRADER_ID, strategy_id=STRATEGY_ID, clock=clock)  # ty: ignore[missing-argument, unknown-argument]
    # Positional per the stubbed cython signature (kwargs do not resolve).
    order = factory.stop_market(
        instrument.id,
        OrderSide.SELL,
        Quantity.from_int(1),
        Price.from_str("29255.50"),
        TriggerType.DEFAULT,
        TimeInForce.DAY,
    )
    cache.add_order(order)
    return engine, order


def _event(order: Any, cls: Any, ts: int, **kwargs: Any) -> Any:
    return cls(
        trader_id=TRADER_ID,
        strategy_id=order.strategy_id,
        instrument_id=order.instrument_id,
        client_order_id=order.client_order_id,
        account_id=ACCOUNT_ID,
        event_id=UUID4(),
        ts_event=ts,
        ts_init=ts,
        **kwargs,
    )


def _status_report(order: Any, status: OrderStatus) -> OrderStatusReport:
    return OrderStatusReport(
        account_id=ACCOUNT_ID,
        instrument_id=order.instrument_id,
        venue_order_id=VenueOrderId("201087037"),
        order_side=order.side,
        order_type=order.order_type,
        time_in_force=TimeInForce.DAY,
        order_status=status,
        quantity=order.quantity,
        filled_qty=order.filled_qty,
        report_id=UUID4(),
        ts_accepted=1,
        ts_last=1,
        ts_init=2,
        client_order_id=order.client_order_id,
        trigger_price=Price.from_str("29255.50"),
        trigger_type=TriggerType.DEFAULT,
        reduce_only=True,
        avg_px=None,
    )


def _scenario_deferred_open_stop() -> None:
    """The 15:21:09 / 15:51:05 MY043-001 WARNs: querying a bracket stop while
    Rithmic still defers its OPEN answers SUBMITTED + avg_px None, which the
    engine reconciles through its fill path and warns about."""
    _init_logging()
    engine, order = _stop_order()
    order.apply(_event(order, OrderSubmitted, 1))
    assert order.status is OrderStatus.SUBMITTED

    engine.reconcile_execution_report(_status_report(order, OrderStatus.SUBMITTED))

    # The engine force-accepts the in-flight order from the fall-through branch
    # ("must have been accepted from this point") before reaching the warning.
    assert order.status is OrderStatus.ACCEPTED


def _scenario_accepted_over_canceled() -> None:
    """The 15:30:48.500 MY043-001 WARN: an open-order drain row taken before a
    cancel propagated reports ACCEPTED against a locally CANCELED leg; the
    engine applies it without any monotonic guard."""
    _init_logging()
    engine, order = _stop_order()
    order.apply(_event(order, OrderSubmitted, 1))
    order.apply(
        _event(order, OrderAccepted, 2, venue_order_id=VenueOrderId("201087037"))
    )
    order.apply(
        _event(order, OrderCanceled, 3, venue_order_id=VenueOrderId("201087037"))
    )
    assert order.status is OrderStatus.CANCELED

    engine.reconcile_execution_report(_status_report(order, OrderStatus.ACCEPTED))

    assert order.status is OrderStatus.CANCELED  # not regressed


_SCENARIOS = {
    "deferred-open-stop": (_scenario_deferred_open_stop, _AVG_PX_WARN),
    "accepted-over-canceled": (_scenario_accepted_over_canceled, _INVALID_STATE_WARN),
}


def _run_scenario(name: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, __file__, name],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


@pytest.mark.parametrize("name", sorted(_SCENARIOS))
def test_engine_warning_scenario(name: str) -> None:
    _fn, warn_line = _SCENARIOS[name]

    result = _run_scenario(name)

    assert result.returncode == 0, (
        f"scenario {name!r} failed:\n{result.stdout}\n{result.stderr}"
    )
    # The nautilus Rust logger splits streams by level; accept either.
    assert warn_line in (result.stdout + result.stderr)


if __name__ == "__main__":
    scenario_name = sys.argv[1]
    run, _expected = _SCENARIOS[scenario_name]
    run()
    # Only reachable when every in-scenario assertion held.
    print(f"SCENARIO-OK {scenario_name}")
