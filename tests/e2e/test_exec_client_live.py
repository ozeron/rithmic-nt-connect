"""Live integration tests: Nautilus Execution Testing Spec (TC-E*) suite.

Drives the **Rithmic execution client** through a real ``TradingNode`` using a
minimal ``OrderDriver`` strategy. Maps 1:1 to
``docs/references/nautilus-exec-testing-matrix.md``; conversion/mapping
assertions (TC-E72/E73, recon dedup) live in the unit suites.

Marker: ``live`` — needs credentials + network + trading.

**SAFETY:** ``exec_front_month_instrument`` (conftest) refuses production plants
and requires ``RITHMIC_ENABLE_TRADING=1`` before opening any session. One
Rithmic login only. ``cancel_all_orders`` is plant-wide — never for a single
smoke order; TC-E41 is collection-skipped for this reason.

Usage:
  uv run pytest tests/e2e/test_exec_client_live.py -v          # full sweep
  uv run pytest tests/e2e/test_exec_client_live.py -v -k E10   # single TC
"""

from __future__ import annotations

import asyncio
import itertools
import time
import uuid

import pytest

from nautilus_trader.config import (
    LiveDataEngineConfig,
    LiveExecEngineConfig,
    LiveRiskEngineConfig,
    LoggingConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId, TraderId

from rithmic_nt_connect import (
    ADAPTER_NAME,
    VENUE,
    RithmicLiveDataClientConfig,
    RithmicLiveDataClientFactory,
    RithmicLiveExecClientConfig,
    RithmicLiveExecClientFactory,
    session_config_from_explicit_test_env,
)

from order_dsl import above, below, far_above, far_below, limit, market, relative, stop_limit, stop_market
from exec_harness import ExecHarness, OrderDriver, OrderDriverConfig


@pytest.fixture
def live_exec(exec_front_month_instrument):
    """Build a TradingNode (data + exec) with an OrderDriver; start on demand.

    Depends on ``exec_front_month_instrument``, so the safety gates run first.
    """
    test_session = session_config_from_explicit_test_env()
    instrument = exec_front_month_instrument

    driver = OrderDriver(OrderDriverConfig(instrument_id=str(instrument.id)), instrument)
    # A prior test disposed its loop; give the next node a fresh one so it does
    # not bind to a closed loop ("Event loop is closed").
    asyncio.set_event_loop(asyncio.new_event_loop())
    node = TradingNode(
        config=TradingNodeConfig(
            trader_id=TraderId("TESTER-001"),
            logging=LoggingConfig(log_level="WARNING", print_config=False),
            data_engine=LiveDataEngineConfig(graceful_shutdown_on_exception=False),
            exec_engine=LiveExecEngineConfig(
                reconciliation=True,
                reconciliation_startup_delay_secs=0.0,
                # The ``load_orders`` drain is best-effort and not provably
                # complete. ``open_check_open_only=True`` (the 1.231.x knob; the
                # old ``death_policy=trust_stop`` no longer exists) keeps a
                # cached order missing from an empty drain as an advisory log
                # instead of letting the engine cancel a tracked working order.
                open_check_open_only=True,
            ),
            risk_engine=LiveRiskEngineConfig(bypass=True),
            data_clients={ADAPTER_NAME: RithmicLiveDataClientConfig(session=test_session)},
            exec_clients={
                VENUE: RithmicLiveExecClientConfig(
                    session=test_session, enable_trading=True
                )
            },
            timeout_connection=45.0,
            timeout_reconciliation=10.0,
            timeout_portfolio=10.0,
            timeout_disconnection=10.0,
            timeout_post_stop=10.0,
        )
    )
    node.add_data_client_factory(ADAPTER_NAME, RithmicLiveDataClientFactory)
    node.add_exec_client_factory(VENUE, RithmicLiveExecClientFactory)
    node.trader.add_strategy(driver)
    node.build()

    harness = ExecHarness(node, driver, instrument)
    try:
        yield harness
    finally:
        harness.shutdown()


def _tc(tc_id: str, *values):
    """``pytest.param`` whose node id is the TC id (e.g. ``TC-E10``)."""
    return pytest.param(*values, id=tc_id)


# Unique per-run CIDs: stale venue working orders replay on reconnect and would
# collide with fixed ids (duplicate ClientOrderId denial). Run token + counter —
# epoch seconds alone would collide across same-second runs.
_RUN_TOKEN = uuid.uuid4().hex[:8]
_CID_SEQ = itertools.count(1)


def _unique(cid: str) -> str:
    return f"{cid}-{_RUN_TOKEN}-{next(_CID_SEQ):x}"


# Terminal event per expected order status; venue-conditional rejections are
# classified by ``wait_for_venue_outcome``.
_TERMINAL_EVENT = {
    OrderStatus.ACCEPTED: "OrderAccepted",
    OrderStatus.FILLED: "OrderFilled",
    OrderStatus.CANCELED: "OrderCanceled",
}


# ══════════════════════════════════════════════════════════════════════
# Group 1: Market orders (TC-E01..E04 fill; E05 unsupported; E06 position)
# ══════════════════════════════════════════════════════════════════════


@pytest.mark.live
class TestMarketOrders:
    @pytest.mark.parametrize(
        ("side", "tif"),
        [
            _tc("TC-E01", OrderSide.BUY, TimeInForce.GTC),
            _tc("TC-E02", OrderSide.SELL, TimeInForce.GTC),
            _tc("TC-E03", OrderSide.BUY, TimeInForce.IOC),
            _tc("TC-E04", OrderSide.BUY, TimeInForce.FOK),
        ],
    )
    def test_TC_E0X_market_fill(self, live_exec, side, tif):
        live_exec.driver.initial.append(market(side, tif=tif))
        live_exec.start()
        live_exec.wait_for_venue_outcome("OrderFilled", timeout=40)
        assert "OrderSubmitted" in live_exec.event_types()
        assert "OrderAccepted" in live_exec.event_types()

    def test_TC_E06_close_position_on_stop(self, live_exec):
        """TC-E06 — close position on stop."""
        live_exec.driver.initial.append(market(OrderSide.BUY))
        live_exec.start()
        live_exec.wait_for_venue_outcome("OrderFilled", timeout=40)
        # Capture the driver-owned open positions (the ``on_stop`` predicate) —
        # not every position on the instrument (external residuals excluded).
        owned = live_exec.cache.positions_open(
            instrument_id=live_exec.instrument.id, strategy_id=live_exec.driver.id
        )
        assert owned, "market buy opened a driver-owned position"
        position_ids = {p.id for p in owned}

        # stop_and_wait has joined the thread; flattening is settled — assert directly.
        live_exec.stop_and_wait()

        for position_id in position_ids:
            position = live_exec.cache.position(position_id)
            assert position is not None, f"position {position_id} missing from cache"
            assert not position.is_open, f"position {position_id} still open"


# ══════════════════════════════════════════════════════════════════════
# Group 2: Limit orders (TC-E10..E19)
# ══════════════════════════════════════════════════════════════════════


@pytest.mark.live
class TestLimitOrders:
    @pytest.mark.parametrize(
        ("side", "price_fn", "tif", "cid", "expected", "expect_no_fill"),
        [
            _tc("TC-E10", OrderSide.BUY, below, TimeInForce.GTC, "L10", OrderStatus.ACCEPTED, True),
            _tc("TC-E11", OrderSide.SELL, above, TimeInForce.GTC, "L11", OrderStatus.ACCEPTED, True),
            _tc("TC-E13", OrderSide.BUY, above, TimeInForce.IOC, "L13", OrderStatus.FILLED, False),
            _tc("TC-E14", OrderSide.BUY, below, TimeInForce.IOC, "L14", OrderStatus.CANCELED, False),
            _tc("TC-E15", OrderSide.BUY, above, TimeInForce.FOK, "L15", OrderStatus.FILLED, False),
            _tc("TC-E16", OrderSide.BUY, below, TimeInForce.FOK, "L16", OrderStatus.CANCELED, False),
            _tc("TC-E19", OrderSide.BUY, below, TimeInForce.DAY, "L19", OrderStatus.ACCEPTED, True),
        ],
    )
    def test_TC_ElX_limit_order(
        self, live_exec, side, price_fn, tif, cid, expected, expect_no_fill
    ):
        cid = _unique(cid)
        live_exec.driver.initial.append(
            limit(side, price_fn, tif, client_order_id=cid)
        )
        live_exec.start()
        # Terminal-event wait; venue-conditional rejections skip, others fail.
        live_exec.wait_for_venue_outcome(
            _TERMINAL_EVENT[expected], timeout=40, client_order_id=ClientOrderId(cid)
        )
        if expect_no_fill:
            assert "OrderFilled" not in live_exec.event_types()

    def test_TC_E12_limit_pair(self, live_exec):
        """TC-E12 — limit BUY+SELL pair resting simultaneously."""
        cid_b = ClientOrderId(_unique("L12B"))
        cid_s = ClientOrderId(_unique("L12S"))
        live_exec.driver.initial.append(
            limit(OrderSide.BUY, below, TimeInForce.GTC, client_order_id=cid_b.value)
        )
        live_exec.driver.initial.append(
            limit(OrderSide.SELL, above, TimeInForce.GTC, client_order_id=cid_s.value)
        )
        live_exec.start()
        live_exec.wait_for_venue_outcome(
            "OrderAccepted", timeout=40, client_order_id=cid_b
        )
        live_exec.wait_for_venue_outcome(
            "OrderAccepted", timeout=40, client_order_id=cid_s
        )


# ══════════════════════════════════════════════════════════════════════
# Group 3: Stop & conditional orders (TC-E20..E27)
# ══════════════════════════════════════════════════════════════════════


@pytest.mark.live
class TestStopConditionalOrders:
    @pytest.mark.parametrize(
        ("side", "trigger_fn", "limit_fn", "cid"),
        [
            _tc("TC-E20", OrderSide.BUY, far_above, None, "S20"),
            _tc("TC-E21", OrderSide.SELL, far_below, None, "S21"),
            _tc("TC-E22", OrderSide.BUY, far_above, relative(550), "S22"),
            _tc("TC-E23", OrderSide.SELL, far_below, relative(-550), "S23"),
        ],
    )
    def test_TC_E2X_stop(self, live_exec, side, trigger_fn, limit_fn, cid):
        # Trigger far from market — rests without triggering.
        cid = _unique(cid)
        if limit_fn is None:
            spec = stop_market(side, trigger_fn, client_order_id=cid)
        else:
            spec = stop_limit(side, trigger_fn, limit_fn, client_order_id=cid)
        live_exec.driver.initial.append(spec)
        live_exec.driver.cancel_on_accept.add(ClientOrderId(cid))
        live_exec.start()
        # Accept or skip on venue-conditional rejection (wait_for_venue_outcome).
        live_exec.wait_for_venue_outcome(
            "OrderAccepted", timeout=40, client_order_id=ClientOrderId(cid)
        )
        live_exec.wait_order_status(ClientOrderId(cid), OrderStatus.CANCELED, timeout=20)


# ══════════════════════════════════════════════════════════════════════
# Group 5: Order cancellation (TC-E40..E44)
# ══════════════════════════════════════════════════════════════════════


@pytest.mark.live
class TestOrderCancellation:
    def test_TC_E40_cancel_single_limit(self, live_exec):
        """TC-E40 — cancel single limit on accept."""
        cid = _unique("C40")
        live_exec.driver.initial.append(
            limit(OrderSide.BUY, below, TimeInForce.GTC, client_order_id=cid)
        )
        live_exec.driver.cancel_on_accept.add(ClientOrderId(cid))
        live_exec.start()
        live_exec.wait_for_venue_outcome(
            "OrderCanceled", timeout=40, client_order_id=ClientOrderId(cid)
        )
        assert "OrderAccepted" in live_exec.event_types()

    def test_TC_E42_individual_cancels_on_stop(self, live_exec):
        """TC-E42 — individual cancels on stop (on_stop cancels each resting order)."""
        cid_a = ClientOrderId(_unique("C42A"))
        cid_b = ClientOrderId(_unique("C42B"))
        live_exec.driver.initial.append(
            limit(OrderSide.BUY, below, TimeInForce.GTC, client_order_id=cid_a.value)
        )
        live_exec.driver.initial.append(
            limit(OrderSide.SELL, above, TimeInForce.GTC, client_order_id=cid_b.value)
        )
        live_exec.start()
        live_exec.wait_for_venue_outcome("OrderAccepted", timeout=40, client_order_id=cid_a)
        live_exec.wait_for_venue_outcome("OrderAccepted", timeout=40, client_order_id=cid_b)

        # on_stop cancels each resting order individually (never plant-wide).
        live_exec.stop_and_wait()

        for cid in (cid_a, cid_b):
            deadline = time.monotonic() + 5.0
            while True:
                order = live_exec.cache.order(cid)
                if order is not None and order.status == OrderStatus.CANCELED:
                    break
                if time.monotonic() > deadline:
                    actual = order.status if order is not None else None
                    pytest.fail(f"{cid} not canceled by on_stop (actual={actual})")
                time.sleep(0.2)

    def test_TC_E44_cancel_already_canceled(self, live_exec):
        """TC-E44 — cancel already-canceled is a local no-op (no new events)."""
        cid = _unique("C44")
        live_exec.driver.initial.append(
            limit(OrderSide.BUY, below, TimeInForce.GTC, client_order_id=cid)
        )
        live_exec.start()
        live_exec.wait_for_venue_outcome(
            "OrderAccepted", timeout=40, client_order_id=ClientOrderId(cid)
        )
        order = live_exec.cache.order(ClientOrderId(cid))
        assert order is not None
        live_exec.driver.cancel_order(order)
        live_exec.wait_for_venue_outcome(
            "OrderCanceled", timeout=40, client_order_id=ClientOrderId(cid)
        )

        # Nautilus refuses the second cancel locally — no venue command/events.
        cursor = live_exec.event_cursor()
        order = live_exec.cache.order(ClientOrderId(cid))
        live_exec.driver.cancel_order(order)
        time.sleep(1.0)
        assert len(live_exec.driver.events) == cursor, (
            "second cancel of a canceled order must not emit events"
        )
        assert live_exec.cache.order(ClientOrderId(cid)).status == OrderStatus.CANCELED


# ══════════════════════════════════════════════════════════════════════
# Group 9: Lifecycle & reconciliation


@pytest.mark.live
class TestReconciliation:
    def test_TC_E84_reconcile_resting_stop_preserves_trigger(self, live_exec):
        """TC-E84 — reconcile open orders (stop trigger preserved; order stays working).

        Regression guard for the stop ``query_order``/recon report failure (the
        report previously carried ``trigger_type=None`` / raised ``AttributeError``).
        """
        # Unique CID per run (venue replays stale working orders on reconnect).
        cid = ClientOrderId(_unique("S84"))
        # Resting stop far from market — survives reconciliation.
        live_exec.driver.initial.append(
            stop_market(OrderSide.BUY, far_above, client_order_id=cid.value)
        )
        live_exec.start()
        # Accept or venue-conditional rejection (e.g. market closed) — the latter skips.
        live_exec.wait_for_venue_outcome("OrderAccepted", timeout=45, client_order_id=cid)
        order = live_exec.cache.order(cid)
        assert order is not None, "stop order missing from cache after accept"

        reports = live_exec.check_orders_consistency(timeout_secs=20.0)

        stop_reports = [r for r in reports if getattr(r, "client_order_id", None) == cid]
        assert stop_reports, "reconciliation emitted no report for the resting stop"
        report = stop_reports[0]
        assert report.trigger_type.name == "DEFAULT", (
            f"stop recon report lost trigger_type: {report.trigger_type}"
        )
        assert report.trigger_price is not None, "stop recon report lost trigger_price"

        # Reconciliation must not have canceled the tracked order.
        order = live_exec.cache.order(cid)
        assert order is not None and order.status == OrderStatus.ACCEPTED, (
            "reconciliation disturbed the tracked order: "
            f"{order.status if order is not None else None}"
        )

        # Cancel, then prove the best-effort drain no longer reports it working
        # (at most a terminal CANCELED row); an empty drain is advisory.
        live_exec.driver.cancel_order(order)
        live_exec.wait_order_status(cid, OrderStatus.CANCELED, timeout=20)
        post_cancel = [
            r
            for r in live_exec.check_orders_consistency(timeout_secs=20.0)
            if getattr(r, "client_order_id", None) == cid
        ]
        assert not post_cancel or all(
            r.order_status == OrderStatus.CANCELED for r in post_cancel
        ), f"venue still reports the canceled stop as a working order: {post_cancel}"

    def test_TC_E85_filled_order_status_query_carries_avg_px(self, live_exec):
        """TC-E85 — a status query for a FILLED order reports its average fill price.

        Nautilus ExecEngine logs ``report.avg_px was None`` when it reconciles a
        filled order through this exact pull path (QueryOrderStatus -> client
        -> cache-backed report), so the report must not drop the fill price.
        """
        live_exec.driver.initial.append(market(OrderSide.BUY))
        live_exec.start()
        fill = live_exec.wait_for_venue_outcome("OrderFilled", timeout=45)
        cid = fill.client_order_id
        order = live_exec.wait_order_status(cid, OrderStatus.FILLED, timeout=20)

        report = live_exec.order_status_report(cid)

        assert report is not None, f"no status report for filled order {cid}"
        assert report.order_status == OrderStatus.FILLED
        assert report.avg_px is not None, (
            "filled order status report lost avg_px "
            "(Nautilus ExecEngine warns 'report.avg_px was None')"
        )
        assert abs(float(report.avg_px) - float(order.avg_px)) < 0.01, (
            f"status report avg_px {report.avg_px} != order avg_px {order.avg_px}"
        )


# ══════════════════════════════════════════════════════════════════════
# Scaffolded / venue-unsupported TCs
#
# Kept as named params so the ids stay greppable against the exec matrix, but
# both groups are skipped at collection time — no TradingNode is built for
# them (the old one-line scaffolds spun a node up just to skip).
# ══════════════════════════════════════════════════════════════════════

SCAFFOLDED_TCS = [
    # Group 4: order modification
    "TC-E30", "TC-E31", "TC-E32", "TC-E33", "TC-E34", "TC-E35", "TC-E36",
    # Group 6: bracket orders
    "TC-E50", "TC-E51", "TC-E52", "TC-E53",
    # Group 7: order flags
    "TC-E60", "TC-E61", "TC-E62", "TC-E63",
    # Group 8: rejection handling
    "TC-E70", "TC-E71", "TC-E74", "TC-E75", "TC-E76", "TC-E77", "TC-E78",
    # Group 9: lifecycle & reconciliation
    "TC-E80", "TC-E81", "TC-E82", "TC-E83", "TC-E86", "TC-E87",
]

UNSUPPORTED_TCS = [
    ("TC-E05", "Rithmic is contract-qty only; quote quantity unsupported"),
    ("TC-E17", "Rithmic has no GTD duration"),
    ("TC-E18", "Rithmic has no GTD duration"),
    ("TC-E24", "Rithmic MIT wire mapping is not verified; fail closed"),
    ("TC-E25", "Rithmic MIT wire mapping is not verified; fail closed"),
    ("TC-E26", "Rithmic LIT wire mapping is not verified; fail closed"),
    ("TC-E27", "Rithmic LIT wire mapping is not verified; fail closed"),
    ("TC-E41", "plant-wide cancel_all — live-testing would cancel unrelated orders; unit/fake-plant boundary only"),
    ("TC-E43", "Rithmic has no batch-cancel API"),
]


@pytest.mark.live
@pytest.mark.skip(reason="scaffold not implemented — see nautilus-exec-testing-matrix.md")
@pytest.mark.parametrize("tc", SCAFFOLDED_TCS)
def test_scaffolded_tc(tc: str) -> None:
    raise AssertionError(f"{tc} should be collection-skipped")


@pytest.mark.live
@pytest.mark.parametrize(
    ("tc", "reason"),
    [
        pytest.param(tc, reason, id=tc, marks=pytest.mark.skip(reason=reason))
        for tc, reason in UNSUPPORTED_TCS
    ],
)
def test_unsupported_tc(tc: str, reason: str) -> None:
    raise AssertionError(f"{tc} should be collection-skipped")
