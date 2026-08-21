"""TradingNode harness for the live exec suite (tests/e2e).

``OrderDriver`` is a minimal strategy that submits/cancels orders on command:
- ``initial`` specs submit on start (quote-independent) or the first quote tick.
- ``cancel_on_accept`` cancels on accept.
- ``on_stop`` cancels resting orders and flattens driver-owned positions.
- Every order event is recorded in ``events``.

``ExecHarness`` wraps the node thread. ``wait_for_venue_outcome`` centralizes the
venue-conditional skip rule (market closed / permission denied / not entitled).
"""

from __future__ import annotations

import contextlib
import threading
import time

import pytest
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.trading.strategy import Strategy, StrategyConfig

_VENUE_CONDITIONAL_MARKERS = ("market is closed", "permission denied", "not entitled")


def venue_conditional_reason(event) -> str | None:
    """Reason of a venue-conditional ``OrderRejected`` (a legitimate skip),
    else None.
    """
    reason = (getattr(event, "reason", "") or "").casefold()
    for marker in _VENUE_CONDITIONAL_MARKERS:
        if marker in reason:
            return reason
    return None


class OrderDriverConfig(StrategyConfig, frozen=True):
    instrument_id: str


class OrderDriver(Strategy):
    """Minimal strategy that submits/cancels orders on command."""

    def __init__(self, config: OrderDriverConfig, instrument):
        super().__init__(config)
        self.instrument = instrument
        self.initial: list = []
        self.cancel_on_accept: set[ClientOrderId] = set()
        self.events: list = []
        self._bid = None
        self._ask = None
        self._got_quote = False

    # -- order-event recording -------------------------------------------------
    def on_order_initialized(self, event):
        self.events.append(event)

    def on_order_submitted(self, event):
        self.events.append(event)

    def on_order_accepted(self, event):
        self.events.append(event)
        cid = event.client_order_id
        order = self.cache.order(cid)
        if cid in self.cancel_on_accept and order is not None:
            self.cancel_order(order)

    def on_order_rejected(self, event):
        self.events.append(event)

    def on_order_canceled(self, event):
        self.events.append(event)

    def on_order_expired(self, event):
        self.events.append(event)

    def on_order_filled(self, event):
        self.events.append(event)

    def on_order_cancel_rejected(self, event):
        self.events.append(event)

    def on_order_modify_rejected(self, event):
        self.events.append(event)

    # -- submission ------------------------------------------------------------
    def on_start(self):
        self.subscribe_quote_ticks(self.instrument.id)
        # ``None`` from a spec means "needs a quote" — submit those on the first
        # tick; quote-independent specs run once here, never again.
        pending = []
        for spec in self.initial:
            order = spec(self, self.instrument, None, None)
            if order is None:
                pending.append(spec)
            else:
                self.submit_order(order)
        self.initial[:] = pending

    def on_quote_tick(self, tick):
        self._bid = tick.bid_price
        self._ask = tick.ask_price
        if not self._got_quote:
            self._got_quote = True
            for spec in self.initial:
                order = spec(self, self.instrument, self._bid, self._ask)
                if order is not None:
                    self.submit_order(order)
            self.initial.clear()

    def on_stop(self):
        # Cancel resting orders and flatten driver-owned positions during the
        # kernel's post-stop window. Per-order/per-position only — the adapter's
        # ``CancelAllOrders`` is plant-wide and never for smoke cleanup. Each
        # cleanup is attempted so one failure cannot abort the rest.
        for order in self.cache.orders_open(
            instrument_id=self.instrument.id, strategy_id=self.id
        ):
            try:
                self.cancel_order(order)
            except Exception as exc:
                self.log.error(
                    f"on_stop cancel failed for {order.client_order_id}: {exc}"
                )
        for position in self.cache.positions_open(
            instrument_id=self.instrument.id, strategy_id=self.id
        ):
            try:
                self.close_position(position)
            except Exception as exc:
                self.log.error(f"on_stop close failed for {position.id}: {exc}")


class ExecHarness:
    def __init__(self, node, driver: OrderDriver, instrument):
        self.node = node
        self.driver = driver
        self.instrument = instrument
        self.cache = driver.cache
        self._thread: threading.Thread | None = None
        self._stopped = False

    def start(self, timeout: float = 30.0) -> None:
        self._thread = threading.Thread(target=self.node.run, daemon=True)
        self._thread.start()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            loop = self.node.get_event_loop()
            if loop is not None and loop.is_running():
                return
            time.sleep(0.05)
        raise RuntimeError("TradingNode did not reach running state")

    def stop_and_wait(self, timeout: float = 30.0) -> None:
        if self._stopped:
            return
        # ``node.stop()`` already catches its own ``RuntimeError`` and logs it.
        self.node.stop()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                raise RuntimeError("TradingNode thread did not stop")
        # ``_stopped`` means *confirmed dead* (join succeeded) — teardown must
        # not dispose or release the credential flock while the thread may live.
        self._stopped = True

    def shutdown(self) -> None:
        self.stop_and_wait()
        self._assert_no_residual_exposure()
        # A failed dispose is a teardown failure — do not swallow it.
        self.node.dispose()
        self._release_session_locks()

    def _assert_no_residual_exposure(self) -> None:
        """Fail loudly if ``on_stop`` left driver-owned orders/positions behind."""
        open_orders = self.cache.orders_open(strategy_id=self.driver.id)
        open_positions = self.cache.positions_open(strategy_id=self.driver.id)
        if open_orders or open_positions:
            raise RuntimeError(
                "OrderDriver residual exposure after stop: "
                f"{len(open_orders)} open order(s), {len(open_positions)} open "
                f"position(s)"
            )

    def _release_session_locks(self) -> None:
        """Release the credential flock (``disconnect()`` does not); best-effort."""
        with contextlib.suppress(Exception):
            from rithmic_nt_connect.session import _SESSION_CACHE

            for _sess in _SESSION_CACHE.values():
                _lock = getattr(_sess, "_lock", None)
                if _lock is not None:
                    with contextlib.suppress(Exception):
                        _lock.close()
            _SESSION_CACHE.clear()

    # -- reconciliation ---------------------------------------------------------
    def check_orders_consistency(self, timeout_secs: float = 20.0) -> list[object]:
        """Run the engine's open-order consistency check; return the client's reports.

        Drives ``LiveExecutionEngine._check_orders_consistency`` (the path that
        sends ``GenerateOrderStatusReports``), spying on the client's
        ``generate_order_status_reports`` to capture the constructed reports.
        ``reconcile_execution_state`` is *not* used — it covers mass status and
        positions only, not order-status queries.
        """
        import asyncio

        from rithmic_nt_connect.execution import RithmicExecutionClient

        engine = self.node.kernel.exec_engine
        orders = self.cache.orders()
        clients = engine.get_clients_for_orders(orders) if orders else set()
        client = next(
            (c for c in clients if isinstance(c, RithmicExecutionClient)), None
        )
        if client is None:
            raise RuntimeError("Rithmic exec client not reachable from the engine")

        captured: list[object] = []
        original = client.generate_order_status_reports

        async def spy(command):
            reports = await original(command)
            captured.extend(reports)
            return reports

        client.generate_order_status_reports = spy
        future = None
        try:
            loop = self.node.get_event_loop()
            if loop is None or not loop.is_running():
                raise RuntimeError("TradingNode event loop is not running")
            future = asyncio.run_coroutine_threadsafe(
                engine._check_orders_consistency(),
                loop,
            )
            # Engine tolerances (threshold/retries) add to the drain time.
            future.result(timeout=timeout_secs + 30.0)
        finally:
            # Never leave the engine coroutine running after a timeout.
            if future is not None and not future.done():
                future.cancel()
            client.generate_order_status_reports = original
        return captured

    def order_status_report(
        self, client_order_id: ClientOrderId, timeout_secs: float = 20.0
    ):
        """Drive a singular ``GenerateOrderStatusReport`` and return the adapter report.

        This is the exact pull path Nautilus ExecEngine uses when it queries an
        order's status (e.g. after a fill) — the path that warned
        ``report.avg_px was None`` for a filled order.
        """
        import asyncio

        from nautilus_trader.core.uuid import UUID4
        from nautilus_trader.execution.messages import GenerateOrderStatusReport
        from rithmic_nt_connect.execution import RithmicExecutionClient

        engine = self.node.kernel.exec_engine
        orders = self.cache.orders()
        clients = engine.get_clients_for_orders(orders) if orders else set()
        client = next(
            (c for c in clients if isinstance(c, RithmicExecutionClient)), None
        )
        if client is None:
            raise RuntimeError("Rithmic exec client not reachable from the engine")

        loop = self.node.get_event_loop()
        if loop is None or not loop.is_running():
            raise RuntimeError("TradingNode event loop is not running")
        future = asyncio.run_coroutine_threadsafe(
            client.generate_order_status_report(
                GenerateOrderStatusReport(None, client_order_id, None, UUID4(), 1)
            ),
            loop,
        )
        return future.result(timeout=timeout_secs)

    # -- assertions ------------------------------------------------------------
    def event_types(self) -> list[str]:
        return [type(evt).__name__ for evt in self.driver.events]

    def event_cursor(self) -> int:
        """Snapshot of the recorded-event list length, for ``after=`` waits."""
        return len(self.driver.events)

    def wait_event(
        self,
        *type_names: str,
        timeout: float = 30.0,
        after: int = 0,
        client_order_id: ClientOrderId | None = None,
    ):
        """Return the first event of ``type_names`` at/after cursor ``after``
        (optionally per ``client_order_id``); raise with events seen on timeout.

        Multi-step tests pass the cursor captured before the action, else an
        earlier step's event would match.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for event in self.driver.events[after:]:
                if type(event).__name__ in type_names and (
                    client_order_id is None
                    or getattr(event, "client_order_id", None) == client_order_id
                ):
                    return event
            time.sleep(0.1)
        raise AssertionError(
            f"no {type_names} event within {timeout}s; seen={self.event_types()}"
        )

    def wait_for_venue_outcome(
        self,
        *outcome_names: str,
        timeout: float = 40.0,
        after: int = 0,
        client_order_id: ClientOrderId | None = None,
    ):
        """Like ``wait_event``, but an ``OrderRejected`` is classified: a
        venue-conditional rejection skips, any other fails with its reason."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for event in self.driver.events[after:]:
                if (
                    client_order_id is not None
                    and getattr(event, "client_order_id", None) != client_order_id
                ):
                    continue
                name = type(event).__name__
                if name == "OrderRejected":
                    reason = venue_conditional_reason(event)
                    if reason is not None:
                        pytest.skip(f"venue-conditional rejection: {reason}")
                    raise AssertionError(
                        f"unexpected venue rejection: "
                        f"{getattr(event, 'reason', '') or event}"
                    )
                if name in outcome_names:
                    return event
            time.sleep(0.1)
        raise AssertionError(
            f"no {outcome_names} event within {timeout}s; seen={self.event_types()}"
        )

    def wait_order_status(
        self, client_order_id: ClientOrderId, status: OrderStatus, timeout: float = 30.0
    ):
        """Return the order once it reaches ``status``; raise with its actual
        status on timeout."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            order = self.cache.order(client_order_id)
            if order is not None and order.status == status:
                return order
            time.sleep(0.1)
        order = self.cache.order(client_order_id)
        actual = order.status if order is not None else None
        raise AssertionError(
            f"order {client_order_id} did not reach {status} within {timeout}s "
            f"(actual={actual})"
        )
