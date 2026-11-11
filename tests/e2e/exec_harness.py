"""LiveNode harness for the live exec suite (tests/e2e).

``OrderDriver`` is a minimal strategy that submits/cancels orders on command:
- ``initial`` specs submit on start (quote-independent) or the first quote tick.
- ``cancel_on_accept`` cancels on accept.
- ``on_stop`` cancels resting orders and flattens driver-owned positions.
- Every order event is recorded in ``events``.

``ExecHarness`` owns the node thread. NautilusTrader 2 nodes and strategies are
bound to the thread that built them (touching one from another thread aborts
the process), so the harness builds, runs and disposes the node on its own
thread and runs every node, cache, strategy and client call on that thread's
loop. ``harness.cache`` and ``harness.driver`` are proxies that do this.
Orders, events and reports are plain model values and cross threads freely.

``wait_for_venue_outcome`` centralizes the venue-conditional skip rule
(market closed / permission denied / not entitled).
"""

from __future__ import annotations

import asyncio
import gc
import threading
import time
import traceback
from collections.abc import Awaitable, Callable
from concurrent.futures import Future
from typing import Any, Self

import pytest
from nautilus_trader.live import LiveNode
from nautilus_trader.model import ClientOrderId, OrderStatus
from nautilus_trader.trading import Strategy
from rithmic_nt_connect.execution import ReportQuery, RithmicExecutionClient
from rithmic_nt_connect.factories import RithmicLiveExecClientFactory

_VENUE_CONDITIONAL_MARKERS = (
    "market is closed",
    "permission denied",
    "not entitled",
    "no such route exists",  # nothing rests
)

Spec = Callable[..., Any]


def venue_conditional_reason(event: Any) -> str | None:
    """Reason of a venue-conditional ``OrderRejected`` (a legitimate skip),
    else None.
    """
    reason = (getattr(event, "reason", "") or "").casefold()
    for marker in _VENUE_CONDITIONAL_MARKERS:
        if marker in reason:
            return reason
    return None


class OrderDriver(Strategy):
    """Minimal strategy that submits/cancels orders on command."""

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        # The native base's ``__new__`` takes only ``config``.
        return super().__new__(cls)

    def __init__(
        self,
        instrument: Any,
        *,
        initial: list[Spec] | None = None,
        cancel_on_accept: set[ClientOrderId] | None = None,
        events: list[Any] | None = None,
    ) -> None:
        super().__init__()
        self.instrument = instrument
        self.initial: list[Spec] = initial if initial is not None else []
        self.cancel_on_accept: set[ClientOrderId] = (
            cancel_on_accept if cancel_on_accept is not None else set()
        )
        self.events: list[Any] = events if events is not None else []
        self._bid = None
        self._ask = None
        self._got_quote = False

    # -- order-event recording -------------------------------------------------
    def on_order_initialized(self, event: Any) -> None:
        self.events.append(event)

    def on_order_submitted(self, event: Any) -> None:
        self.events.append(event)

    def on_order_accepted(self, event: Any) -> None:
        self.events.append(event)
        cid = event.client_order_id
        if cid in self.cancel_on_accept and self.cache.order(cid) is not None:
            self.cancel_order(cid)

    def on_order_rejected(self, event: Any) -> None:
        self.events.append(event)

    def on_order_canceled(self, event: Any) -> None:
        self.events.append(event)

    def on_order_expired(self, event: Any) -> None:
        self.events.append(event)

    def on_order_filled(self, event: Any) -> None:
        self.events.append(event)

    def on_order_cancel_rejected(self, event: Any) -> None:
        self.events.append(event)

    def on_order_modify_rejected(self, event: Any) -> None:
        self.events.append(event)

    # -- submission ------------------------------------------------------------
    def on_start(self) -> None:
        self.subscribe_quotes(self.instrument.id)
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

    def on_quote(self, quote: Any) -> None:
        self._bid = quote.bid_price
        self._ask = quote.ask_price
        if not self._got_quote:
            self._got_quote = True
            for spec in self.initial:
                order = spec(self, self.instrument, self._bid, self._ask)
                if order is not None:
                    self.submit_order(order)
            self.initial.clear()

    def on_stop(self) -> None:
        # Cancel resting orders and flatten driver-owned positions during the
        # node's post-stop window. Per-order/per-position only — the adapter's
        # ``CancelAllOrders`` is plant-wide and never for smoke cleanup. Each
        # cleanup is attempted so one failure cannot abort the rest.
        for order in self.cache.orders_open(
            instrument_id=self.instrument.id, strategy_id=self.strategy_id
        ):
            try:
                self.cancel_order(order.client_order_id)
            except Exception as exc:
                self.log.error(
                    f"on_stop cancel failed for {order.client_order_id}: {exc}"
                )
        for position in self.cache.positions_open(
            instrument_id=self.instrument.id, strategy_id=self.strategy_id
        ):
            try:
                self.close_position(position)
            except Exception as exc:
                self.log.error(f"on_stop close failed for {position.id}: {exc}")


class NodeThreadError(RuntimeError):
    """A failure raised on the node thread, carried over as formatted text."""


class _OnNodeThread:
    """Proxy: attribute reads and method calls run on the node thread."""

    def __init__(self, harness: ExecHarness, target: Callable[[], Any]) -> None:
        self._harness = harness
        self._target = target

    def __getattr__(self, name: str) -> Any:
        # Decide on the node thread: a bound method of a native object must
        # never reach this thread (dropping it here aborts the process).
        def read() -> tuple[bool, Any]:
            value = getattr(self._target(), name)
            return (True, None) if callable(value) else (False, value)

        is_method, value = self._harness.call(read)
        if not is_method:
            return value

        def method(*args: Any, **kwargs: Any) -> Any:
            return self._harness.call(
                lambda: getattr(self._target(), name)(*args, **kwargs)
            )

        return method


RegisterClient = Callable[[RithmicExecutionClient], None]
NodeBuilder = Callable[[RegisterClient], LiveNode]


def reporting_exec_factory(
    register: RegisterClient,
) -> type[RithmicLiveExecClientFactory]:
    """The Rithmic exec factory, reporting the client it creates."""

    class Factory(RithmicLiveExecClientFactory):
        @staticmethod
        def create(**kwargs: Any) -> Any:
            client = RithmicLiveExecClientFactory.create(**kwargs)
            register(client)
            return client

    return Factory


class ExecHarness:
    """Run one node + ``OrderDriver`` on a dedicated thread.

    ``build`` makes the node on that thread. It receives a callback that its
    exec client factory calls with the client it creates (see
    ``reporting_exec_factory``), so the harness can reach that client. Fill
    ``initial`` / ``cancel_on_accept`` before ``start``.
    """

    def __init__(self, build: NodeBuilder, instrument: Any) -> None:
        self._build = build
        self.instrument = instrument
        self.initial: list[Spec] = []
        self.cancel_on_accept: set[ClientOrderId] = set()
        self.events: list[Any] = []
        self.cache = _OnNodeThread(self, lambda: self._driver_obj().cache)
        self.driver = _OnNodeThread(self, self._driver_obj)
        self._driver: OrderDriver | None = None
        self._node: LiveNode | None = None
        self._client: RithmicExecutionClient | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._running = threading.Event()
        self._node_done = threading.Event()
        self._release: asyncio.Event | None = None
        self._error: str | None = None
        self._stopped = False
        self._disposed = False

    # -- node thread -------------------------------------------------------------
    def _driver_obj(self) -> OrderDriver:
        if self._driver is None:
            raise RuntimeError("OrderDriver not built (call start first)")
        return self._driver

    def _register_client(self, client: RithmicExecutionClient) -> None:
        self._client = client

    def _thread_main(self) -> None:
        try:
            self._node = self._build(self._register_client)
            self._driver = OrderDriver(
                self.instrument,
                initial=self.initial,
                cancel_on_accept=self.cancel_on_accept,
                events=self.events,
            )
            self._node.add_strategy(self._driver)
            asyncio.run(self._serve())
        except Exception as exc:
            self._fail(exc)
        finally:
            # The node and strategy must be dropped on the thread that made them.
            self._node = None
            self._driver = None
            self._client = None
            # Clients sit in reference cycles (tasks, callbacks); collect them
            # here, or a later collection on another thread drops them there.
            gc.collect()
            self._node_done.set()
            self._running.set()  # unblock start() on a failed build

    def _fail(self, exc: BaseException) -> None:
        # Keep the text, not the traceback: its frames would hold the node
        # alive past this thread.
        self._error = "".join(traceback.format_exception(exc))
        exc.__traceback__ = None

    async def _serve(self) -> None:
        node = self._node
        assert node is not None
        self._loop = asyncio.get_running_loop()
        self._release = asyncio.Event()
        handle = node.handle()

        async def watch_running() -> None:
            while not handle.is_running:
                await asyncio.sleep(0.05)
            self._running.set()

        watcher = asyncio.create_task(watch_running())
        try:
            await node.run_async()
        except Exception as exc:
            self._fail(exc)
        finally:
            watcher.cancel()
            self._node_done.set()
        # Keep the loop serving cache reads until shutdown releases it.
        await self._release.wait()
        node.dispose()

    def call(self, fn: Callable[[], Any], timeout: float = 30.0) -> Any:
        """Run ``fn`` on the node thread and return its result."""
        return self.call_async(lambda: _as_coroutine(fn), timeout)

    def call_async(
        self, make: Callable[[], Awaitable[Any]], timeout: float = 30.0
    ) -> Any:
        """Await ``make()`` on the node loop (built there) and return its result."""
        loop = self._loop
        if loop is None or loop.is_closed():
            raise RuntimeError("node event loop is not running")

        async def run() -> Any:
            text: str | None = None
            try:
                return await make()
            except Exception as exc:
                text = "".join(traceback.format_exception(exc))
                # Free the frames here, on the node thread: they can hold node
                # objects that must not be dropped on the caller's thread.
                traceback.clear_frames(exc.__traceback__)
                exc.__traceback__ = None
            # Raised outside the handler, so the original is not kept as
            # ``__context__`` of what reaches the caller.
            raise NodeThreadError(text)

        future: Future[Any] = asyncio.run_coroutine_threadsafe(run(), loop)
        try:
            return future.result(timeout=timeout)
        finally:
            if not future.done():
                future.cancel()

    # -- lifecycle ---------------------------------------------------------------
    def start(self, timeout: float = 60.0) -> None:
        self._thread = threading.Thread(target=self._thread_main, daemon=True)
        self._thread.start()
        if not self._running.wait(timeout):
            raise RuntimeError("LiveNode did not reach running state")
        if self._error is not None or self._node_done.is_set():
            raise RuntimeError(f"LiveNode failed to start:\n{self._error}")

    def stop_and_wait(self, timeout: float = 30.0) -> None:
        if self._stopped or self._loop is None:
            return
        # Touch the node only on its own thread (never hold it here).
        self._loop.call_soon_threadsafe(self._stop_on_node_thread)
        if not self._node_done.wait(timeout):
            raise RuntimeError("LiveNode did not stop")
        # ``_stopped`` means *confirmed stopped* — teardown must not release
        # the credential flock while the node may still run.
        self._stopped = True
        if self._error is not None:
            raise RuntimeError(f"LiveNode run failed:\n{self._error}")

    def _stop_on_node_thread(self) -> None:
        if self._node is not None:
            self._node.handle().stop()

    def shutdown(self, timeout: float = 30.0) -> None:
        if self._disposed:
            return
        try:
            self.stop_and_wait()
            self._assert_no_residual_exposure()
        finally:
            loop, release = self._loop, self._release
            if loop is not None and release is not None and not loop.is_closed():
                loop.call_soon_threadsafe(release.set)
            if self._thread is not None:
                self._thread.join(timeout=timeout)
                if self._thread.is_alive():
                    raise RuntimeError("LiveNode thread did not exit")
            self._disposed = True

    def _assert_no_residual_exposure(self) -> None:
        """Fail loudly if ``on_stop`` left driver-owned orders/positions behind."""
        if self._driver is None:
            return
        sid = self.driver.strategy_id
        open_orders = self.cache.orders_open(strategy_id=sid)
        open_positions = self.cache.positions_open(strategy_id=sid)
        if open_orders or open_positions:
            raise RuntimeError(
                "OrderDriver residual exposure after stop: "
                f"{len(open_orders)} open order(s), {len(open_positions)} open "
                f"position(s)"
            )

    # -- order / reconciliation commands -------------------------------------------
    def cancel(self, client_order_id: ClientOrderId) -> None:
        """Cancel one driver order by id (looked up on the node thread)."""

        def run() -> None:
            driver = self._driver_obj()
            if driver.cache.order(client_order_id) is None:
                raise LookupError(f"order {client_order_id} not in cache")
            driver.cancel_order(client_order_id)

        self.call(run)

    def _exec_client(self) -> RithmicExecutionClient:
        if self._client is None:
            raise RuntimeError("Rithmic exec client was not created")
        return self._client

    def drain_open_orders(self, timeout_secs: float = 20.0) -> list[Any]:
        """The client's open-orders drain (``open_only`` status reports).

        This is the hook the engine's open-order check calls on its interval;
        it is called directly here, so it proves what the drain reports, not
        what the engine then does with it.
        """
        return self.call_async(
            lambda: self._exec_client()._generate_order_status_reports(
                ReportQuery(open_only=True)
            ),
            timeout=timeout_secs + 30.0,
        )

    def order_status_report(
        self, client_order_id: ClientOrderId, timeout_secs: float = 20.0
    ) -> Any:
        """The single-order status pull the engine makes when it queries an order
        (e.g. after a fill) — the path that once dropped ``avg_px``."""
        return self.call_async(
            lambda: self._exec_client()._generate_order_status_report(
                ReportQuery(client_order_id=client_order_id)
            ),
            timeout=timeout_secs,
        )

    # -- assertions ------------------------------------------------------------
    def event_types(self) -> list[str]:
        return [type(evt).__name__ for evt in list(self.events)]

    def event_cursor(self) -> int:
        """Snapshot of the recorded-event list length, for ``after=`` waits."""
        return len(self.events)

    def wait_event(
        self,
        *type_names: str,
        timeout: float = 30.0,
        after: int = 0,
        client_order_id: ClientOrderId | None = None,
    ) -> Any:
        """Return the first event of ``type_names`` at/after cursor ``after``
        (optionally per ``client_order_id``); raise with events seen on timeout.

        Multi-step tests pass the cursor captured before the action, else an
        earlier step's event would match.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for event in list(self.events)[after:]:
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
    ) -> Any:
        """Like ``wait_event``, but an ``OrderRejected`` is classified: a
        venue-conditional rejection skips, any other fails with its reason."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for event in list(self.events)[after:]:
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
    ) -> Any:
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


async def _as_coroutine(fn: Callable[[], Any]) -> Any:
    return fn()
