"""Live execution client for Rithmic (Phase 1 read-only + Phase 2 trading)."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import OrderedDict
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    CancelAllOrders,
    CancelOrder,
    GenerateFillReports,
    GenerateOrderStatusReport,
    GenerateOrderStatusReports,
    GeneratePositionStatusReports,
    ModifyOrder,
    SubmitOrder,
    SubmitOrderList,
)
from nautilus_trader.execution.reports import (
    FillReport,
    OrderStatusReport,
    PositionStatusReport,
)
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import (
    AccountType,
    LiquiditySide,
    OmsType,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    TimeInForce,
    TriggerType,
)
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientId,
    ClientOrderId,
    InstrumentId,
    TradeId,
    Venue,
    VenueOrderId,
)
from nautilus_trader.model.objects import (
    AccountBalance,
    Currency,
    Money,
    Price,
    Quantity,
)
from nautilus_trader.model.orders import Order

from rithmic_nt_connect._convert import (
    account_pnl_to_fields,
    format_price_str,
    instrument_pnl_to_fields,
    rithmic_route_from_info,
)
from rithmic_nt_connect._order_plant import OrderPlantPolicy, OrderPlantState
from rithmic_nt_connect._orders import (
    DEFAULT_TRAIL_BY_PRICE_ID,
    OrderMapError,
    fill_dedup_key,
    nautilus_order_type_to_rithmic,
    nautilus_side_to_rithmic,
    nautilus_tif_to_rithmic,
    notification_action,
    order_notification_to_fields,
    order_side_from_notification,
    slim_order_fields,
    trade_id_from_fill_fields,
    trailing_ticks_from_order,
)
from rithmic_nt_connect.config import (
    RithmicExecClientConfig,
    RithmicLiveExecClientConfig,
)
from rithmic_nt_connect.constants import ADAPTER_NAME, DEFAULT_ACCOUNT_CURRENCY, VENUE
from rithmic_nt_connect.errors import (
    CHANNEL_ERRORS,
    ReconciliationUnavailableError,
    VenueQueryUnavailable,
)
from rithmic_nt_connect.providers import RithmicInstrumentProvider
from rithmic_nt_connect.session import WireSession

_POSITION_SIDE = {
    "LONG": PositionSide.LONG,
    "SHORT": PositionSide.SHORT,
    "FLAT": PositionSide.FLAT,
}

# Order types that support the TRIGGERED order status (Nautilus #3812, ported
# from upstream 2f7d3947). Market-style stops (STOP_MARKET, MARKET_IF_TOUCHED,
# TRAILING_STOP_MARKET) execute immediately on trigger and have no intermediate
# TRIGGERED state, so a venue TRIGGER notification for them must not emit
# OrderTriggered (the 1.231.x model rejects it).
_TRIGGERABLE_ORDER_TYPES = frozenset(
    {
        OrderType.STOP_LIMIT,
        OrderType.TRAILING_STOP_LIMIT,
        OrderType.LIMIT_IF_TOUCHED,
    }
)

# Rithmic price_type enum (1=Limit, 2=Market, 3=StopLimit, 4=StopMarket) -> Nautilus.
_RITHMIC_PRICE_TYPE_TO_ORDER_TYPE: dict[int, OrderType] = {
    1: OrderType.LIMIT,
    2: OrderType.MARKET,
    3: OrderType.STOP_LIMIT,
    4: OrderType.STOP_MARKET,
}

# Rithmic duration enum (1=Day, 2=Gtc, 3=Ioc, 4=Fok) -> Nautilus.
_RITHMIC_DURATION_TO_TIF: dict[int, TimeInForce] = {
    1: TimeInForce.DAY,
    2: TimeInForce.GTC,
    3: TimeInForce.IOC,
    4: TimeInForce.FOK,
}

_TERMINAL_ORDER_STATUSES = frozenset(
    {
        OrderStatus.FILLED,
        OrderStatus.CANCELED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    }
)


ACCOUNT_CACHE_TIMEOUT_S = 10.0

# Recon windows are clamped to this many days so a units mistake (epoch seconds
# passed where ns-epoch is expected) cannot build a decades-wide query window.
_MAX_RECON_SPAN_S = 32 * 86_400


async def wait_account_in_cache(
    cache: Any,
    account_id: AccountId,
    *,
    venue: str = VENUE,
    timeout_s: float = ACCOUNT_CACHE_TIMEOUT_S,
) -> None:
    """Yield until Portfolio has applied AccountState (official connect postcondition).

    ``generate_account_state`` publishes on the bus; ``_connect`` must not return
    (and thus ``_set_connected``) until ``cache.account`` / venue lookup is set.
    """
    deadline = time.monotonic() + max(0.05, timeout_s)
    venue_id = Venue(venue)
    while True:
        if cache.account(account_id) is not None:
            return
        by_venue = getattr(cache, "account_for_venue", None)
        if callable(by_venue):
            venue_account = by_venue(venue_id)
            if venue_account is not None and str(
                getattr(venue_account, "id", "")
            ) == str(account_id):
                return
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"account {account_id} not in cache after {timeout_s:.0f}s "
                "(AccountState never applied; refuse set_connected)"
            )
        await asyncio.sleep(0.05)


def _price(value: float | Decimal | str, precision: int | None = None) -> Price:
    if precision is None:
        return Price.from_str(format_price_str(value))
    return Price.from_str(f"{float(value):.{int(precision)}f}")


class RithmicExecutionClient(LiveExecutionClient):
    """Rithmic execution client (PnL always; order plant when ``enable_trading``)."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: RithmicInstrumentProvider,
        config: RithmicExecClientConfig | RithmicLiveExecClientConfig,
        session: WireSession,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or f"{ADAPTER_NAME}-EXEC"),
            venue=Venue(VENUE),
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            base_currency=Currency.from_str("USD"),
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=None,
        )
        self._config_local = config
        self._session = session
        self._poll_task: asyncio.Task | None = None
        self._order_poll_task: asyncio.Task | None = None
        self._positions: dict[str, dict[str, Any]] = {}
        # Client<->venue correlation lives solely on the Nautilus Cache
        # (add_venue_order_id / client_order_id / venue_order_id / order); there
        # is no parallel tag dictionary to keep in sync. user_tag ==
        # client_order_id.value for orders this client placed, so tracked-ness is
        # decided by whether the order is present in the cache.
        # Venue-stable fill ids; retained across reconnect so snapshot replays
        # stay idempotent.
        self._seen_fill_keys: OrderedDict[str, None] = OrderedDict()
        # Last published untracked-status key per venue order, so Rithmic's
        # frequent re-pushes of unchanged order state do not re-emit status
        # reports for external orders.
        self._untracked_status_keys: dict[str, tuple[object, ...]] = {}
        self._order_plant = OrderPlantPolicy(OrderPlantState.DISCONNECTED)
        # Latched by ``_mark_order_plant_failed``: a transport resync must not
        # restore the plant to LIVE while an operation's venue outcome is still
        # unknown (re-enabling commands could duplicate/conflict with it).
        # Cleared only on a full ``_connect`` (engine reconciliation runs then).
        self._order_plant_latched = False
        # Fresh per-connect activity gate: the PnL/account stream delivered at
        # least one parseable account/position snapshot since (re)connect. This
        # proves stream activity, not position completeness; the re-arm barrier
        # requires it before clearing the latch (positions ride that stream).
        self._pnl_snapshot_observed = asyncio.Event()
        self._account_seeded = False
        # PnL is streamed frequently; avoid publishing identical AccountState
        # events (and making Nautilus Portfolio log each one).
        self._last_account_state_key: tuple[str, str, Decimal] | None = None

    _MAX_SEEN_FILL_KEYS = 10_000
    _REARM_PNL_SNAPSHOT_TIMEOUT_S = 5.0

    def _fill_key_seen(self, key: str) -> bool:
        if key in self._seen_fill_keys:
            self._seen_fill_keys.move_to_end(key)
            return True
        return False

    def _mark_fill_key(self, key: str) -> None:
        self._seen_fill_keys[key] = None
        self._seen_fill_keys.move_to_end(key)
        while len(self._seen_fill_keys) > self._MAX_SEEN_FILL_KEYS:
            self._seen_fill_keys.popitem(last=False)

    @property
    def enable_trading(self) -> bool:
        return bool(self._config_local.enable_trading)

    async def _connect(self) -> None:
        from rithmic_nt_connect.session import ensure_connected

        # A reconnect must re-observe the PnL/account stream before the order
        # plant can re-arm: positions ride that stream, and a stale observation
        # from before the drop is not proof of current exposure.
        self._pnl_snapshot_observed.clear()
        await asyncio.to_thread(ensure_connected, self._session)
        self._log.info("Rithmic exec session ready (shared with data client)")

        # Whether the PnL poll loop is running for THIS connect: the re-arm
        # PnL gate applies only when the stream is actually connected — with
        # ``soft_fail_pnl`` there may be no stream to observe, and requiring an
        # observation would block trading with no recovery path.
        self._pnl_connected = False
        try:
            await asyncio.to_thread(self._session.subscribe_pnl)
            self._apply_resolved_account_id()
            self._seed_account_if_needed()
            self._poll_task = self.create_task(
                self._plant_poll_loop(
                    name="pnl",
                    poll_fn=self._session.poll_pnl_event,
                    on_event=self._dispatch_pnl_event,
                    on_resync=self._resync_pnl_subscription,
                ),
                log_msg="rithmic_pnl_poll",
            )
            self._pnl_connected = True
        except Exception as exc:
            if self._config_local.soft_fail_pnl:
                self._log.warning(f"PnL/account path soft-failed: {exc}")
            else:
                raise
        self._seed_account_if_needed()

        if self.enable_trading:
            self._order_plant.state = OrderPlantState.CONNECTING
            try:
                await asyncio.to_thread(self._session.subscribe_order_updates)
            except Exception:
                self._order_plant.state = OrderPlantState.DISCONNECTED
                try:
                    await asyncio.to_thread(self._session.disconnect_order_plant)
                except Exception as teardown_exc:
                    self._log.warning(
                        f"order plant teardown after subscribe fail: {teardown_exc}"
                    )
                raise
            self._apply_resolved_account_id()
            self._seed_account_if_needed()
            # The poll loop starts before the re-arm barrier (never-drop):
            # notifications must keep flowing even while the plant is
            # un-armed, so a slow or failed drain cannot drop venue events.
            self._order_poll_task = self.create_task(
                self._plant_poll_loop(
                    name="order",
                    poll_fn=self._session.poll_order_event,
                    on_event=self._dispatch_order_event,
                    on_resync=self._resync_order_subscription,
                ),
                log_msg="rithmic_order_poll",
            )
            # Every (re)connect re-observes venue state before arming — the
            # disconnect window can hide venue-side accepts/fills/terminal
            # outcomes and position changes even when nothing was latched. The
            # adapter owns readiness; the engine's reconciliation is repair,
            # not readiness. A mid-session transport resync does not clear the
            # latch (see ``_resync_order_subscription``).
            try:
                await self._rearm_after_reconnect()
            except Exception as exc:
                self._order_plant.state = OrderPlantState.DISCONNECTED
                self._log.error(
                    f"reconnect re-arm failed; order plant stays un-armed: {exc}"
                )
            else:
                if self._order_plant.state is not OrderPlantState.CONNECTING or (
                    self._order_poll_task is not None and self._order_poll_task.done()
                ):
                    # The order poll loop kept running during the drain
                    # (never-drop). Any anomaly in that window leaves
                    # ``CONNECTING``: a newer latch or a stream failure
                    # (handler break / resync failure) sets DISCONNECTED, a
                    # mid-drain resync ends DISCONNECTED while latched, and
                    # a dead poll task cannot deliver. The re-arm must not
                    # clear a latch or re-arm a dead/broken stream it did
                    # not observe.
                    self._order_plant.state = OrderPlantState.DISCONNECTED
                    self._log.error(
                        "reconnect re-arm finished but the order plant was "
                        "re-latched or its stream failed during the drain; "
                        "staying un-armed"
                    )
                else:
                    self._order_plant_latched = False
                    self._order_plant.state = OrderPlantState.LIVE
        await self._await_account_registered()

    async def _await_account_registered(
        self, timeout_secs: float = ACCOUNT_CACHE_TIMEOUT_S, log_registered: bool = True
    ) -> None:
        deadline = time.monotonic() + timeout_secs
        while self.account_id is None and time.monotonic() < deadline:
            self._apply_resolved_account_id()
            self._seed_account_if_needed()
            if self.account_id is not None:
                break
            await asyncio.sleep(0.05)
        if self.account_id is None:
            if self.enable_trading:
                raise RuntimeError(
                    "no Rithmic account id after connect — "
                    "set RITHMIC_ACCOUNT_ID or wait for plant resolve"
                )
            self._log.warning(
                "no Rithmic account id; exec connected without AccountState"
            )
            return
        remaining = max(0.5, deadline - time.monotonic())
        await wait_account_in_cache(
            self._cache, self.account_id, venue=VENUE, timeout_s=remaining
        )
        if log_registered:
            self._log.info(f"account observable in cache {self.account_id}")

    def _apply_resolved_account_id(self) -> None:
        raw = self._account_raw()
        if raw and self.account_id is None:
            self._set_account_id(AccountId(f"{VENUE}-{raw}"))

    def _account_raw(self, hint: str | None = None) -> str | None:
        if hint:
            return str(hint)
        resolved = self._session.resolved_account()
        if resolved is not None and resolved.get("account_id"):
            return str(resolved["account_id"])
        session = getattr(self._config_local, "session", None)
        cfg_id = getattr(session, "account_id", None)
        if cfg_id:
            return str(cfg_id)
        if self.account_id is not None:
            value = str(self.account_id)
            prefix = f"{VENUE}-"
            return value.removeprefix(prefix)
        return None

    def _seed_account_if_needed(self, account_raw: str | None = None) -> None:
        """Register a USD margin account so Portfolio can apply fills.

        Lucid PnL often omits currency; without generate_account_state the
        engine drops fills (``no account registered``) and flatten is a no-op.
        """
        raw = self._account_raw(account_raw)
        if not raw:
            return
        if self.account_id is None:
            self._set_account_id(AccountId(f"{VENUE}-{raw}"))
        if self._account_seeded:
            return
        usd = Currency.from_str(DEFAULT_ACCOUNT_CURRENCY)
        zero = Money(Decimal(0), usd)
        self.generate_account_state(
            balances=[AccountBalance(zero, zero, zero)],
            margins=[],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
            info={"rithmic_account_id": raw, "seeded": "true"},
        )
        self._last_account_state_key = (str(self.account_id), str(usd), Decimal(0))
        self._account_seeded = True
        self._log.info(
            f"seeded account {self.account_id} currency={DEFAULT_ACCOUNT_CURRENCY}"
        )

    async def _resync_order_subscription(self) -> None:
        self._order_plant.state = OrderPlantState.RESYNCING
        await asyncio.to_thread(self._session.disconnect_order_plant)
        await asyncio.to_thread(self._session.subscribe_order_updates)
        # A latched failure (unknown venue outcome) must survive the resync:
        # the channel recovered, but the original operation was never resolved,
        # so commands stay blocked until explicit recovery/reconciliation.
        if self._order_plant_latched:
            self._order_plant.state = OrderPlantState.DISCONNECTED
        else:
            self._order_plant.state = OrderPlantState.LIVE

    async def _resync_pnl_subscription(self) -> None:
        await asyncio.to_thread(self._session.disconnect_pnl_plant)
        await asyncio.to_thread(self._session.subscribe_pnl)

    async def _rearm_after_reconnect(self) -> None:
        """Adapter-owned readiness: re-arm the order plant only after venue
        state is re-acquired.

        Runs the bounded working-orders drain and waits (bounded) for the PnL
        stream to deliver again (activity gate: the stream is alive and
        delivering venue state — not a proof that every position arrived). Any
        failure raises, so the latch survives.
        """
        start_sec, end_sec = self._recon_window_sec(None, None)
        events = await self._load_orders_events(start_sec, end_sec)
        self._apply_drain_rows(events)
        # The PnL gate applies only when the PnL stream is actually connected
        # this connect: with ``soft_fail_pnl`` there is no stream to observe.
        if self._pnl_connected:
            await self._await_pnl_snapshot()
        self._apply_resolved_account_id()
        self._seed_account_if_needed()

    def _apply_drain_rows(self, events: list[dict[str, Any]]) -> None:
        """Apply a working-orders drain to the local cache before re-arming.

        The drain is a snapshot, not a replay: bind the venue id for tracked
        in-flight orders (so commands target the real venue order and later
        notifications attach), and publish reconciliation status reports for
        the rows (terminal outcomes the live stream missed while
        disconnected). Typed live events are NOT re-emitted — that is the live
        stream's job and would double-emit for rows already seen live.
        Publication failures are logged and skipped; the engine's own
        reconciliation re-runs the drain afterwards.
        """
        latest: dict[str, tuple[int, dict[str, Any]]] = {}
        for raw in events:
            try:
                fields = order_notification_to_fields(raw)
            except Exception:
                continue
            basket = fields.get("basket_id")
            if not basket:
                continue
            ts_event = int(fields.get("ts_event") or 0)
            key = str(basket)
            if key not in latest or ts_event >= latest[key][0]:
                latest[key] = (ts_event, fields)
        for ts_event, fields in latest.values():
            basket = str(fields["basket_id"])
            client_order_id = self._resolve_client_order_id(fields)
            if client_order_id is not None:
                order = self._cache.order(client_order_id)
                if (
                    order is not None
                    and self._cache.venue_order_id(client_order_id) is None
                ):
                    self._bind_venue_id(client_order_id, basket)
            report = self._order_status_report_from_fields(fields, ts_event)
            if report is not None:
                RithmicExecutionClient._publish_order_status_report(
                    self,
                    report,
                    context="reconnect re-arm drain",
                )

    async def _await_pnl_snapshot(self, timeout_s: float | None = None) -> None:
        """Wait (bounded) for the PnL stream to deliver account/position
        context. This is an activity gate, not a completeness proof: Rithmic
        pushes account PnL on a short interval even when the state is
        unchanged, so an observation proves the stream is alive and delivering
        venue state again; silence aborts the re-arm (raises), preserving the
        latch.
        """
        timeout_s = (
            self._REARM_PNL_SNAPSHOT_TIMEOUT_S if timeout_s is None else timeout_s
        )
        try:
            await asyncio.wait_for(self._pnl_snapshot_observed.wait(), timeout_s)
        except TimeoutError:
            raise VenueQueryUnavailable(
                "order plant re-arm aborted: no account/position PnL snapshot "
                "observed after reconnect"
            ) from None

    async def _poll_session_event(
        self,
        poll_fn: Callable[[], dict[str, Any] | None],
    ) -> dict[str, Any] | None:
        """Return next event, or None on transient errors; re-raise channel failures."""
        try:
            return await asyncio.to_thread(poll_fn)
        except CHANNEL_ERRORS:
            raise
        except Exception as exc:
            self._log.warning(f"poll transient error: {exc}")
            await asyncio.sleep(0.1)
            return None

    async def _plant_poll_loop(
        self,
        *,
        name: str,
        poll_fn: Callable[[], dict[str, Any] | None],
        on_event: Callable[[dict[str, Any]], None],
        on_resync: Callable[[], Any],
    ) -> None:
        backoff = 0.05
        while True:
            try:
                event = await self._poll_session_event(poll_fn)
            except Exception as exc:
                self._log.error(f"{name} poll channel error: {exc}")
                if name == "order":
                    self._order_plant.state = OrderPlantState.RESYNCING
                try:
                    await on_resync()
                    self._log.warning(
                        f"{name} subscription resynced after channel error"
                    )
                    backoff = 0.05
                except Exception as resync_exc:
                    self._log.error(f"{name} subscription resync failed: {resync_exc}")
                    if name == "order":
                        # A failed resync is a dead stream: DISCONNECTED so the
                        # reconnect re-arm barrier (keyed on plant state) can
                        # never clear a latch over it.
                        self._order_plant.state = OrderPlantState.DISCONNECTED
                    backoff = min(backoff * 2, 2.0)
                await asyncio.sleep(backoff)
                continue
            if event is None:
                await asyncio.sleep(0.05)
                continue
            try:
                on_event(event)
            except Exception as exc:
                self._log.exception(f"{name} event handler error (suppressed)", exc)
                if name == "order":
                    # A handler failure can leave venue and cache state divergent.
                    # Stop the order stream and fail closed instead of continuing
                    # to accept commands against stale execution state. Latch
                    # (not just DISCONNECTED): the re-arm barrier (keyed on
                    # plant state) must never clear a latch over a dead stream.
                    self._latch_order_plant(
                        "order handler failure",
                        f"order stream stopped; venue/cache state may be "
                        f"divergent: {exc}",
                    )
                    break

    def _dispatch_pnl_event(self, event: dict[str, Any]) -> None:
        etype = event.get("type")
        if etype == "account_pnl":
            self._publish_account(event)
        elif etype == "instrument_pnl":
            self._cache_position(event)

    def _dispatch_order_event(self, event: dict[str, Any]) -> None:
        if event.get("type") != "order_notification":
            return
        try:
            fields = order_notification_to_fields(event)
        except Exception as exc:
            self._log.error(f"invalid order_notification: {exc}")
            self._order_plant.state = OrderPlantState.DISCONNECTED
            raise
        self._handle_order_notification(fields)

    async def _disconnect(self) -> None:
        self._order_plant.state = OrderPlantState.DISCONNECTED
        for task_attr in ("_order_poll_task", "_poll_task"):
            task = getattr(self, task_attr)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                setattr(self, task_attr, None)
        try:
            await asyncio.to_thread(self._session.disconnect)
        except Exception as exc:
            self._log.warning(f"disconnect warning: {exc}")

    def _publish_account(self, event: dict[str, Any]) -> None:
        try:
            fields = account_pnl_to_fields(event)
        except Exception as exc:
            self._log.error(f"invalid account_pnl: {exc}")
            return
        # Validate the full payload BEFORE mutating any state (account id, seed).
        # A malformed balance must not register a fabricated zero-balance account
        # that would then block the later, valid AccountState via _account_seeded.
        free_raw = fields.get("cash_on_hand")
        if free_raw is None:
            free_raw = fields.get("account_balance")
        if free_raw is None:
            self._log.error("account_pnl missing cash_on_hand and account_balance")
            return
        try:
            free_dec = Decimal(str(free_raw))
        except Exception as exc:
            self._log.error(f"account_pnl balance not numeric ({free_raw!r}): {exc}")
            return
        if not free_dec.is_finite():
            self._log.error(f"account_pnl balance must be finite ({free_raw!r})")
            return
        currency_raw = fields.get("currency") or DEFAULT_ACCOUNT_CURRENCY
        currency = Currency.from_str(str(currency_raw))
        account_id = AccountId(f"{VENUE}-{fields['account_id']}")
        if self.account_id is None:
            self._set_account_id(account_id)
        self._seed_account_if_needed(str(fields["account_id"]))
        free = Money(free_dec, currency)
        locked = Money(Decimal(0), currency)
        total = free
        balances = [AccountBalance(total, locked, free)]
        # Rithmic streams account PnL on a short interval even when the
        # account state is unchanged. Nautilus assigns a new event id to every
        # generated state, so suppress identical states at the adapter boundary.
        state_key = (str(account_id), str(currency), free_dec)
        if state_key == getattr(self, "_last_account_state_key", None):
            # Identical re-push: still a successful observation of the stream.
            self._pnl_snapshot_observed.set()
            return
        self.generate_account_state(
            balances=balances,
            margins=[],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
            info={"rithmic_account_id": fields["account_id"]},
        )
        self._last_account_state_key = state_key
        # Marked only after every fallible step (currency, account id, seed,
        # account state publication) succeeded: a failed handler must not
        # satisfy the re-arm gate (activity gate, not completeness proof).
        self._pnl_snapshot_observed.set()

    def _cache_position(self, event: dict[str, Any]) -> None:
        try:
            fields = instrument_pnl_to_fields(event)
        except Exception as exc:
            self._log.error(f"invalid instrument_pnl: {exc}")
            return
        self._positions[str(fields["instrument_id"])] = fields
        account_raw = fields.get("account_id")
        if account_raw:
            self._seed_account_if_needed(str(account_raw))
        # Marked only after the position write and seeding succeeded (activity
        # gate, not completeness proof).
        self._pnl_snapshot_observed.set()

    def _position_report_from_fields(
        self,
        fields: dict[str, Any],
        ts_init: int,
    ) -> PositionStatusReport | None:
        if self.account_id is None:
            account_raw = fields.get("account_id")
            if not account_raw:
                return None
            self._set_account_id(AccountId(f"{VENUE}-{account_raw}"))
        assert self.account_id is not None
        side = _POSITION_SIDE.get(str(fields["position_side"]), PositionSide.FLAT)
        avg = fields.get("avg_px_open")
        avg_dec = Decimal(str(avg)) if avg is not None else None
        return PositionStatusReport(
            account_id=self.account_id,
            instrument_id=InstrumentId.from_str(str(fields["instrument_id"])),
            position_side=side,
            quantity=Quantity.from_int(int(fields["quantity"])),
            report_id=UUID4(),
            ts_last=ts_init,
            ts_init=ts_init,
            avg_px_open=avg_dec,
        )

    def _price_for_instrument(
        self,
        instrument_id: InstrumentId,
        value: float | Decimal | str,
    ) -> Price:
        instrument = self._cache.instrument(instrument_id)
        if instrument is not None:
            return instrument.make_price(value)
        return _price(value)

    def _route(self, instrument_id: InstrumentId) -> tuple[str, str]:
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            raise ValueError(f"instrument not in cache: {instrument_id}")
        info = getattr(instrument, "info", None) or {}
        return rithmic_route_from_info(info, instrument_id=str(instrument_id))

    def _log_trading_disabled(self, action: str) -> None:
        self._log.error(f"Rithmic exec client trading disabled: {action}")

    def _latch_order_plant(self, action: str, reason: str) -> None:
        """Block further commands after an anomaly that needs a recon cycle.

        The latch is cleared only by a full ``_connect`` whose bounded re-arm
        drain succeeds (``_rearm_after_reconnect``); a mid-session transport
        resync does not clear it (``_resync_order_subscription``). The plant
        leaves ``CONNECTING`` (DISCONNECTED), which is the token the re-arm
        barrier uses to detect an anomaly raised while its drain was running.
        """
        self._order_plant_latched = True
        self._order_plant.state = OrderPlantState.DISCONNECTED
        self._log.error(f"{action}; order plant latched disconnected: {reason}")

    def _mark_order_plant_failed(self, action: str, exc: BaseException) -> None:
        """Block further commands after an operation has an unknown venue outcome."""
        self._latch_order_plant(action, f"transport outcome unknown: {exc}")

    def _resolve_client_order_id(self, fields: dict[str, Any]) -> ClientOrderId | None:
        """Resolve a notification to a tracked order, or None (external).

        The Nautilus cache is the single source of truth for tracked orders.
        Prefer the venue basket id (strongest identity). Fall back to the
        user_tag, which equals ``client_order_id.value`` for orders this client
        placed and is only treated as tracked when that order is actually
        present in the cache — an unknown external tag therefore never
        resolves here and routes to the untracked path.
        """
        basket = fields.get("basket_id")
        if basket:
            cached = self._cache.client_order_id(VenueOrderId(str(basket)))
            if cached is not None:
                cached_order = self._cache.order(cached)
                # Mirror the user_tag guard: a terminal order remains in the
                # cache, and a stale notification for a closed incarnation must
                # not be attributed to it (it would inherit its
                # strategy/instrument/side and never reach the untracked path).
                if cached_order is None or not cached_order.is_closed:
                    return cached
        tag = fields.get("user_tag")
        if tag:
            cached_order = self._cache.order(ClientOrderId(str(tag)))
            # Only attribute the tag to a tracked order that is still open. A
            # terminal order remains in the cache; a later external order that
            # happens to reuse that tag must NOT be attributed to the old order
            # (it would inherit its strategy/instrument/side and never reach the
            # untracked path).
            if cached_order is not None and not cached_order.is_closed:
                return ClientOrderId(str(tag))
        return None

    def _bind_venue_id(self, client_order_id: ClientOrderId, venue_id: str) -> None:
        # Record the venue -> client mapping in the cache (authoritative source).
        self._cache.add_venue_order_id(client_order_id, VenueOrderId(venue_id))

    def _venue_id_for(
        self, fields: dict[str, Any], client_order_id: ClientOrderId | None
    ) -> str:
        basket = fields.get("basket_id")
        if basket:
            return str(basket)
        # No basket in the notification: recover the real venue order id from
        # the cache (bound at accept) so modify_rejected/cancel_rejected
        # reference the venue order, not the client/tag id.
        if client_order_id is not None:
            cached = self._cache.venue_order_id(client_order_id)
            if cached is not None:
                return cached.value
        tag = fields.get("user_tag")
        return str(
            tag or (client_order_id.value if client_order_id is not None else "")
        )

    def _handle_order_notification(self, fields: dict[str, Any]) -> None:
        account_hint = fields.get("account_id")
        if account_hint:
            self._seed_account_if_needed(str(account_hint))
        client_order_id = self._resolve_client_order_id(fields)
        if client_order_id is None:
            self._handle_untracked_notification(fields)
            return
        order = self._cache.order(client_order_id)
        if order is None:
            self._log.warning(
                f"cached order missing for tracked {client_order_id}; "
                f"notification suppressed: {slim_order_fields(fields)}"
            )
            return
        ts_event = fields.get("ts_event")
        ts_event = int(ts_event) if ts_event is not None else self._clock.timestamp_ns()
        basket = fields.get("basket_id")
        if basket:
            self._bind_venue_id(client_order_id, str(basket))
        venue_order_id = VenueOrderId(self._venue_id_for(fields, client_order_id))
        action = notification_action(fields, order)
        if action is None:
            return
        strategy_id = order.strategy_id
        instrument_id = order.instrument_id
        if action.kind == "accepted":
            self.generate_order_accepted(
                strategy_id, instrument_id, client_order_id, venue_order_id, ts_event
            )
        elif action.kind == "rejected":
            self.generate_order_rejected(
                strategy_id,
                instrument_id,
                client_order_id,
                str(action.reason),
                ts_event,
            )
        elif action.kind == "modify_rejected":
            self.generate_order_modify_rejected(
                strategy_id,
                instrument_id,
                client_order_id,
                venue_order_id,
                str(action.reason),
                ts_event,
            )
        elif action.kind == "cancel_rejected":
            self.generate_order_cancel_rejected(
                strategy_id,
                instrument_id,
                client_order_id,
                venue_order_id,
                str(action.reason),
                ts_event,
            )
        elif action.kind == "updated":
            qty = (
                Quantity.from_int(int(action.quantity))
                if action.quantity is not None
                else order.quantity
            )
            prec = int(order.price.precision) if order.has_price else None
            price = (
                _price(action.price, prec)
                if action.price is not None
                else (order.price if order.has_price else None)
            )
            trigger = (
                _price(action.trigger, prec)
                if action.trigger is not None
                else (order.trigger_price if order.has_trigger_price else None)
            )
            self.generate_order_updated(
                strategy_id,
                instrument_id,
                client_order_id,
                venue_order_id,
                qty,
                price,
                trigger,
                ts_event,
            )
        elif action.kind == "canceled":
            self.generate_order_canceled(
                strategy_id, instrument_id, client_order_id, venue_order_id, ts_event
            )
        elif action.kind == "triggered":
            # Producer guard (#3812 / upstream 2f7d3947): only limit-style
            # stops have a TRIGGERED state. Market-style stops go straight to
            # FILLED on trigger; emitting OrderTriggered for them is rejected by
            # the Nautilus model, which would kill the order event stream.
            # A1: a duplicate TRIGGER for an already-TRIGGERED order is
            # suppressed too (terminal-state monotonicity). A closed order
            # never reaches this branch: ``_resolve_client_order_id`` routes it
            # to the untracked report path.
            already_triggered = getattr(order, "status", None) is OrderStatus.TRIGGERED
            if order.order_type not in _TRIGGERABLE_ORDER_TYPES or already_triggered:
                self._log.debug(
                    f"skipping OrderTriggered for {order.order_type} order "
                    f"{client_order_id} (market-style stop or already triggered)"
                )
            else:
                self.generate_order_triggered(
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    venue_order_id,
                    ts_event,
                )
        elif action.kind == "filled":
            if action.fill_qty is None or action.trade_id is None:
                self._log.error(
                    f"fill action missing fields: {slim_order_fields(fields)}"
                )
                return
            dedup = fill_dedup_key(fields, ts_event=ts_event)
            if self._fill_key_seen(dedup):
                return
            try:
                fill_qty = Quantity.from_int(int(action.fill_qty))
                if action.fill_px is None:
                    raise ValueError("fill price missing (pending/sentinel)")
                fill_px = self._price_for_instrument(instrument_id, action.fill_px)
            except (TypeError, ValueError, OverflowError) as exc:
                # A definitive venue fill we cannot price (absent price or the
                # -1.0 pending-price sentinel) means local exposure is known to
                # be incomplete. The dedup key is NOT consumed, so a later
                # priced replay of the same fill id can still recover; the
                # plant is latched (fail-closed) until a recon re-syncs.
                self._latch_order_plant(
                    "fill suppressed",
                    f"tracked {client_order_id} fill unpriceable ({exc}); "
                    "exposure may be incomplete; recon will re-sync",
                )
                return
            # A2: a unique venue fill beyond the local remaining qty is real
            # (never-drop, never cap): Nautilus 1.231.x clamps ``leaves_qty``
            # to zero and accumulates the excess in ``overfill_qty``. Latch so
            # a recon cycle re-syncs the cache (a missed partial is usually the
            # cause); the fill is still emitted at its true size.
            leaves = order.leaves_qty
            if leaves is not None and fill_qty > leaves:
                self._latch_order_plant(
                    "overfill",
                    f"tracked {client_order_id} fill qty {fill_qty} exceeds "
                    f"leaves {leaves} by {fill_qty - leaves}; Nautilus clamps "
                    "leaves_qty and tracks overfill_qty; recon will re-sync",
                )
            commission = Money(Decimal(0), Currency.from_str("USD"))
            self.generate_order_filled(
                strategy_id,
                instrument_id,
                client_order_id,
                venue_order_id,
                None,
                TradeId(str(action.trade_id)),
                order.side,
                order.order_type,
                fill_qty,
                fill_px,
                Currency.from_str("USD"),
                commission,
                LiquiditySide.NO_LIQUIDITY_SIDE,
                ts_event,
                info={"rithmic": dict(fields)},
            )
            self._mark_fill_key(dedup)

    def _fill_report_from_fields(
        self, fields: dict[str, Any], ts_event: int
    ) -> FillReport | None:
        """Build a FillReport from a normalized order_notification fields dict."""
        basket = fields.get("basket_id")
        instrument_raw = fields.get("instrument_id")
        symbol = fields.get("symbol")
        if not basket or not (instrument_raw or symbol):
            return None
        try:
            instrument_id = InstrumentId.from_str(
                str(instrument_raw or f"{symbol}.{VENUE}")
            )
        except Exception:
            return None
        account_raw = fields.get("account_id")
        if account_raw:
            self._seed_account_if_needed(str(account_raw))
        if self.account_id is None:
            return None
        fill_px = fields.get("fill_price")
        fill_sz = fields.get("fill_size")
        side = order_side_from_notification(fields)
        if fill_px is None or fill_sz is None or side is None:
            return None
        try:
            last_qty = Quantity.from_int(int(fill_sz))
            last_px = self._price_for_instrument(instrument_id, fill_px)
        except (TypeError, ValueError, OverflowError):
            return None
        return FillReport(
            account_id=self.account_id,
            instrument_id=instrument_id,
            venue_order_id=VenueOrderId(str(basket)),
            trade_id=TradeId(trade_id_from_fill_fields(fields, ts_event)),
            order_side=side,
            last_qty=last_qty,
            last_px=last_px,
            commission=Money(Decimal(0), Currency.from_str("USD")),
            liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
            report_id=UUID4(),
            ts_event=ts_event,
            ts_init=self._clock.timestamp_ns(),
        )

    def _handle_untracked_notification(self, fields: dict[str, Any]) -> None:
        """Report external venue activity without assigning it to a strategy order."""
        ts_event = fields.get("ts_event")
        ts_event = int(ts_event) if ts_event is not None else self._clock.timestamp_ns()
        basket = fields.get("basket_id")
        symbol = fields.get("symbol")
        instrument_raw = fields.get("instrument_id")
        if not basket or not (instrument_raw or symbol):
            self._log.warning(
                f"untracked order notification missing identity: "
                f"{slim_order_fields(fields)}"
            )
            return
        account_raw = fields.get("account_id")
        if account_raw:
            self._seed_account_if_needed(str(account_raw))
        if self.account_id is None:
            self._log.warning(
                f"untracked order notification missing account: "
                f"{slim_order_fields(fields)}"
            )
            return

        status_report = self._order_status_report_from_fields(fields, ts_event)
        if status_report is not None:
            # Rithmic re-pushes order state frequently; skip an unchanged
            # re-push of an external order (fills below are deduped separately).
            # Include the mutable order terms — an ACCEPTED re-push that changes
            # quantity/price/trigger must update Nautilus, not be discarded.
            status_key = (
                str(status_report.venue_order_id),
                str(getattr(status_report, "order_status", "")),
                str(getattr(status_report, "quantity", "")),
                str(getattr(status_report, "price", "")),
                str(getattr(status_report, "trigger_price", "")),
                str(getattr(status_report, "filled_qty", "")),
                str(getattr(status_report, "avg_px", "")),
            )
            if (
                self._untracked_status_keys.get(str(status_report.venue_order_id))
                == status_key
            ):
                status_report = None
            elif not RithmicExecutionClient._publish_order_status_report(
                self,
                status_report,
                context="untracked notification",
            ):
                # Record only on success so a later re-push can retry.
                return
            else:
                if (
                    len(self._untracked_status_keys)
                    >= RithmicExecutionClient._MAX_SEEN_FILL_KEYS
                ):
                    self._untracked_status_keys.clear()
                self._untracked_status_keys[str(status_report.venue_order_id)] = (
                    status_key
                )
        else:
            self._log.warning(
                f"untracked order status could not be built: "
                f"{slim_order_fields(fields)}"
            )

        if fields.get("kind") != "filled":
            return

        dedup = fill_dedup_key(fields, ts_event=ts_event)
        if self._fill_key_seen(dedup):
            return
        report = self._fill_report_from_fields(fields, ts_event)
        if report is None:
            self._log.error(
                f"untracked fill suppressed (build failed): {slim_order_fields(fields)}"
            )
            return
        self._send_fill_report(report)
        self._mark_fill_key(dedup)

    def _publish_order_status_report(
        self,
        report: OrderStatusReport,
        *,
        context: str,
    ) -> bool:
        """Publish a status report without killing the order event stream.

        Returns ``False`` on publication failure. Callers that need the status
        as a reconciliation prerequisite (fills) treat ``False`` as
        "cannot reconcile, skip this row" — a venue fill is suppressed rather
        than published without an order prerequisite (fail-closed, logged).
        """
        try:
            self._send_order_status_report(report)
        except Exception as exc:
            self._log.exception(
                f"{context}: status report publication failed; skipping stale report",
                exc,
            )
            return False
        return True

    def _order_status_report_for(
        self,
        order: Order,
        ts_init: int,
    ) -> OrderStatusReport | None:
        venue_id = None
        cached_venue = self._cache.venue_order_id(order.client_order_id)
        if cached_venue is not None:
            venue_id = cached_venue.value
        if venue_id is None and order.venue_order_id is not None:
            venue_id = order.venue_order_id.value
        if venue_id is None:
            return None
        return OrderStatusReport(
            account_id=self.account_id,
            instrument_id=order.instrument_id,
            venue_order_id=VenueOrderId(venue_id),
            order_side=order.side,
            order_type=order.order_type,
            time_in_force=order.time_in_force,
            order_status=order.status,
            quantity=order.quantity,
            filled_qty=order.filled_qty,
            report_id=UUID4(),
            ts_accepted=order.ts_accepted,
            ts_last=order.ts_last,
            ts_init=ts_init,
            client_order_id=order.client_order_id,
            price=order.price if order.has_price else None,
            trigger_price=order.trigger_price if order.has_trigger_price else None,
            # Only stop orders expose ``trigger_type`` on the 1.231.x model;
            # plain limit/market orders must not be touched (AttributeError).
            # ``getattr`` keeps this statically honest: the base ``Order`` stub
            # deliberately does not declare the attribute.
            trigger_type=getattr(order, "trigger_type", TriggerType.NO_TRIGGER),
            reduce_only=order.is_reduce_only,
            avg_px=Decimal(str(order.avg_px)) if order.avg_px else None,
        )

    def _cache_backed_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        ts_init = self._clock.timestamp_ns()
        reports: list[OrderStatusReport] = []
        orders = (
            self._cache.orders_open(
                venue=self.venue, instrument_id=command.instrument_id
            )
            if command.open_only
            else self._cache.orders(
                venue=self.venue, instrument_id=command.instrument_id
            )
        )
        for order in orders:
            report = self._order_status_report_for(order, ts_init)
            if report is not None:
                reports.append(report)
        return reports

    async def _submit_order(self, command: SubmitOrder) -> None:
        order: Order = command.order
        if not self.enable_trading:
            self._log_trading_disabled("submit_order")
            self.generate_order_denied(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                "Rithmic trading disabled (enable_trading=False)",
                self._clock.timestamp_ns(),
            )
            return
        if not self._order_plant.allow_submit():
            self.generate_order_denied(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                self._order_plant.reject_reason("submit"),
                self._clock.timestamp_ns(),
            )
            return
        try:
            symbol, exchange = self._route(order.instrument_id)
            side = nautilus_side_to_rithmic(order.side)
            price_type = nautilus_order_type_to_rithmic(order.order_type)
            duration = nautilus_tif_to_rithmic(order.time_in_force)
            trail_by_ticks = trailing_ticks_from_order(order)
        except (OrderMapError, ValueError) as exc:
            self.generate_order_denied(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                str(exc),
                self._clock.timestamp_ns(),
            )
            return

        # Gateway parent trading gate: deny locally before OrderSubmitted.
        if getattr(self._session, "trading_enabled", True) is False:
            self._log_trading_disabled("submit_order (parent gateway)")
            self.generate_order_denied(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                "Rithmic parent gateway trading disabled",
                self._clock.timestamp_ns(),
            )
            return

        user_tag = order.client_order_id.value
        price = float(order.price) if order.has_price else None
        trigger = float(order.trigger_price) if order.has_trigger_price else None
        qty = int(order.quantity)
        trail_by_price_id = (
            DEFAULT_TRAIL_BY_PRICE_ID if trail_by_ticks is not None else None
        )

        self.generate_order_submitted(
            order.strategy_id,
            order.instrument_id,
            order.client_order_id,
            self._clock.timestamp_ns(),
        )
        try:
            await asyncio.to_thread(
                self._session.place_order,
                symbol,
                exchange,
                side,
                price_type,
                qty,
                user_tag,
                price,
                trigger,
                duration,
                trail_by_ticks,
                trail_by_price_id,
            )
        except Exception as exc:
            self._mark_order_plant_failed("place_order", exc)

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        # NOTE: this loops independent legs — NOT a Rithmic plant bracket.
        # Plant brackets use place_bracket_order (stop_ticks/target_ticks) via
        # rithmic-plants; see docs/STATUS.md Brackets / OCO Partial.
        if not self.enable_trading:
            self._log_trading_disabled("submit_order_list")
            for order in command.order_list.orders:
                self.generate_order_denied(
                    order.strategy_id,
                    order.instrument_id,
                    order.client_order_id,
                    "Rithmic trading disabled (enable_trading=False)",
                    self._clock.timestamp_ns(),
                )
            return
        for order in command.order_list.orders:
            await self._submit_order(
                SubmitOrder(
                    trader_id=command.trader_id,
                    strategy_id=command.strategy_id,
                    order=order,
                    command_id=UUID4(),
                    ts_init=self._clock.timestamp_ns(),
                    client_id=command.client_id,
                )
            )

    async def _modify_order(self, command: ModifyOrder) -> None:
        if not self.enable_trading:
            self._log_trading_disabled("modify_order")
            self.generate_order_modify_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                VenueOrderId("UNKNOWN"),
                "Rithmic trading disabled (enable_trading=False)",
                self._clock.timestamp_ns(),
            )
            return
        if not self._order_plant.allow_modify():
            self.generate_order_modify_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                VenueOrderId("UNKNOWN"),
                self._order_plant.reject_reason("modify"),
                self._clock.timestamp_ns(),
            )
            return
        order = self._cache.order(command.client_order_id)
        if order is None:
            self.generate_order_modify_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                VenueOrderId("UNKNOWN"),
                "order not in cache",
                self._clock.timestamp_ns(),
            )
            return
        venue_id = None
        cached_venue = self._cache.venue_order_id(command.client_order_id)
        if cached_venue is not None:
            venue_id = cached_venue.value
        if not venue_id and order.venue_order_id is not None:
            venue_id = order.venue_order_id.value
        if not venue_id:
            self.generate_order_modify_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                VenueOrderId("UNKNOWN"),
                "no venue basket_id for modify",
                self._clock.timestamp_ns(),
            )
            return
        try:
            symbol, exchange = self._route(command.instrument_id)
            price_type = nautilus_order_type_to_rithmic(order.order_type)
            trail_by_ticks = trailing_ticks_from_order(order)
        except (OrderMapError, ValueError) as exc:
            self.generate_order_modify_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                VenueOrderId(venue_id),
                str(exc),
                self._clock.timestamp_ns(),
            )
            return
        qty = (
            int(command.quantity)
            if command.quantity is not None
            else int(order.quantity)
        )
        price = (
            float(command.price)
            if command.price is not None
            else (float(order.price) if order.has_price else None)
        )
        trigger = (
            float(command.trigger_price)
            if command.trigger_price is not None
            else (float(order.trigger_price) if order.has_trigger_price else None)
        )
        try:
            await asyncio.to_thread(
                self._session.modify_order,
                venue_id,
                symbol,
                exchange,
                qty,
                price_type,
                price,
                trigger,
                trail_by_ticks,
            )
        except Exception as exc:
            self._mark_order_plant_failed("modify_order", exc)

    async def _cancel_order(self, command: CancelOrder) -> None:
        if not self.enable_trading:
            self._log_trading_disabled("cancel_order")
            self.generate_order_cancel_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                command.venue_order_id or VenueOrderId("UNKNOWN"),
                "Rithmic trading disabled (enable_trading=False)",
                self._clock.timestamp_ns(),
            )
            return
        if not self._order_plant.allow_cancel():
            self.generate_order_cancel_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                command.venue_order_id or VenueOrderId("UNKNOWN"),
                self._order_plant.reject_reason("cancel"),
                self._clock.timestamp_ns(),
            )
            return
        venue_id = None
        if command.venue_order_id is not None:
            venue_id = command.venue_order_id.value
        if not venue_id:
            cached_venue = self._cache.venue_order_id(command.client_order_id)
            if cached_venue is not None:
                venue_id = cached_venue.value
        if not venue_id:
            self.generate_order_cancel_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                VenueOrderId("UNKNOWN"),
                "no venue basket_id for cancel",
                self._clock.timestamp_ns(),
            )
            return
        try:
            await asyncio.to_thread(self._session.cancel_order, venue_id)
        except Exception as exc:
            self._mark_order_plant_failed("cancel_order", exc)

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        _ = command
        if not self.enable_trading:
            self._log_trading_disabled("cancel_all_orders")
            return
        if not self._order_plant.allow_cancel():
            self._log.error(self._order_plant.reject_reason("cancel_all"))
            return
        try:
            await asyncio.to_thread(self._session.cancel_all_orders)
        except Exception as exc:
            self._mark_order_plant_failed("cancel_all_orders", exc)

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        order = (
            self._cache.order(command.client_order_id)
            if command.client_order_id
            else None
        )
        if order is None and command.venue_order_id is not None:
            mapped = self._cache.client_order_id(command.venue_order_id)
            if mapped is not None:
                order = self._cache.order(mapped)
        if order is None:
            return None
        ts_init = self._clock.timestamp_ns()
        if not self.enable_trading:
            return self._order_status_report_for(order, ts_init)
        # In-flight order with no venue id: the engine's in-flight checker
        # queries this path. Resolve it from the venue first (bounded
        # working-orders drain) — the only adapter-side lever against the
        # engine's terminal UNKNOWN synthesis (plan OQ2). A drain that did not
        # find the order does not prove its fate (best-effort working-orders
        # query), so fail closed: never fabricate a report or terminal state,
        # never return an un-resolved answer the engine could treat as known.
        if (
            order.is_inflight
            and self._cache.venue_order_id(order.client_order_id) is None
        ):
            report = await self._resolve_inflight_by_tag(order, ts_init)
            if report is not None:
                return report
            raise VenueQueryUnavailable(
                "Rithmic order status unavailable: in-flight order unresolved "
                "after the working-orders drain"
            )
        return self._order_status_report_for(order, ts_init)

    async def _resolve_inflight_by_tag(
        self, order: Order, ts_init: int
    ) -> OrderStatusReport | None:
        """Bounded working-orders drain: recover the venue id for an in-flight
        order.

        The drain is awaited, so the live order stream may resolve the order in
        the meantime: re-read the cache first and prefer that newer state over
        a stale drain row (never regress a live terminal/bound order). The
        venue id is bound only after a row builds a report under strict
        validation (``_order_status_report_from_fields(strict=True)``) — a
        malformed row, or one whose closed-set terms would be fabricated, must
        not disable recovery or bind a venue id from fabricated terms.
        """
        start_sec, end_sec = self._recon_window_sec(None, None)
        events = await self._load_orders_events(start_sec, end_sec)
        # Re-read the cached order: the live stream may have resolved it while
        # the drain was in flight (bound venue id and/or terminal state).
        order = self._cache.order(order.client_order_id) or order
        if self._cache.venue_order_id(order.client_order_id) is not None:
            # The live stream bound the venue id while we drained; it owns the
            # authoritative state now.
            return self._order_status_report_for(order, ts_init)
        if getattr(order, "is_closed", False):
            # The live stream resolved the order to a terminal state while we
            # drained; never report a stale drain row over it.
            return self._order_status_report_for(order, ts_init)
        tag = order.client_order_id.value
        for raw in events:
            try:
                fields = order_notification_to_fields(raw)
            except Exception:
                continue
            if str(fields.get("user_tag") or "") != tag:
                continue
            basket = fields.get("basket_id")
            if not basket:
                continue
            # Build the venue report FIRST (strict: no fabricated closed-set
            # terms); bind the venue id only once the row proves usable.
            # ts_event comes from the venue row (receipt time only when
            # genuinely absent); ts_init stays the adapter clock.
            ts_event = fields.get("ts_event")
            ts_event = (
                int(ts_event) if ts_event is not None else self._clock.timestamp_ns()
            )
            report = self._order_status_report_from_fields(
                fields, ts_event, strict=True
            )
            if report is None:
                continue
            self._bind_venue_id(order.client_order_id, str(basket))
            return report
        return None

    def _recon_window_sec(self, start: Any, end: Any) -> tuple[int, int]:
        now_sec = int(time.time())

        def _to_sec(value: Any) -> int | None:
            if value is None:
                return None
            ts = getattr(value, "timestamp", None)
            if callable(ts):
                return max(0, int(ts()))
            if isinstance(value, (int, float)):
                # Nautilus passes datetime; tolerate ns-epoch ints defensively.
                return max(0, int(value) // 1_000_000_000)
            return None

        end_sec = _to_sec(end) if end is not None else now_sec
        if end_sec is None:
            end_sec = now_sec
        start_sec = _to_sec(start)
        if start_sec is None:
            start_sec = int((now_sec // 86_400) * 86_400)  # UTC day start
        # Clamp so a units mistake (epoch seconds vs ns) cannot build a
        # decades-wide window that would issue thousands of history queries.
        if end_sec < start_sec:
            start_sec = end_sec
        if end_sec - start_sec > _MAX_RECON_SPAN_S:
            start_sec = end_sec - _MAX_RECON_SPAN_S
        return start_sec, end_sec

    def _is_recon_unavailable(self, exc: BaseException) -> bool:
        # A definitive "reconciliation cannot be answered" is non-retryable:
        # the venue has no provably-complete order-history retrieval path. Retry
        # only genuine transport/channel failures.
        if isinstance(exc, ReconciliationUnavailableError):
            return True
        if isinstance(exc, VenueQueryUnavailable):
            return True
        return getattr(exc, "code", None) == "reconciliation_unavailable"

    async def _load_orders_events(
        self, start_sec: int, end_sec: int
    ) -> list[dict[str, Any]]:
        # The gateway performs a bounded silence-window drain of the current
        # working orders (`show_orders`). An empty result means "no working
        # orders after the drain" and is a valid best-effort answer, not an
        # error. One bounded attempt per barrier/query: a definitive
        # unavailable result fails immediately, and any other failure is
        # surfaced as unavailable — the next engine query or reconnect is the
        # retry boundary (no hidden retry policy inside recovery paths).
        try:
            return await asyncio.to_thread(
                self._session.load_orders, start_sec, end_sec
            )
        except Exception as exc:
            if self._is_recon_unavailable(exc):
                raise
            raise VenueQueryUnavailable(
                f"load_orders recon failed ({start_sec}..{end_sec}): {exc}"
            ) from exc

    def _matches_instrument(
        self,
        fields: dict[str, Any],
        instrument_id: Any | None,
        venue_order_id: Any | None,
    ) -> bool:
        if venue_order_id is not None:
            return (
                bool(fields.get("basket_id"))
                and str(fields["basket_id"]) == venue_order_id.value
            )
        if instrument_id is None:
            return True
        event_inst = str(fields.get("instrument_id") or "")
        want = str(instrument_id)
        if event_inst == want:
            return True
        root = want.split(".")[0]
        ev_root = event_inst.split(".")[0]
        return bool(
            root and ev_root and (ev_root.startswith(root) or root.startswith(ev_root))
        )

    def _order_type_from_event(self, fields: dict[str, Any]) -> OrderType:
        raw = fields.get("price_type")
        if raw is None:
            return OrderType.MARKET
        try:
            return _RITHMIC_PRICE_TYPE_TO_ORDER_TYPE.get(int(raw), OrderType.MARKET)
        except (TypeError, ValueError):
            return OrderType.MARKET

    def _tif_from_event(self, fields: dict[str, Any]) -> TimeInForce:
        raw = fields.get("duration")
        if raw is None:
            return TimeInForce.GTC
        try:
            return _RITHMIC_DURATION_TO_TIF.get(int(raw), TimeInForce.GTC)
        except (TypeError, ValueError):
            return TimeInForce.GTC

    def _trigger_type_from_event(self, fields: dict[str, Any]) -> TriggerType:
        raw = fields.get("price_type")
        if raw is None:
            return TriggerType.NO_TRIGGER
        try:
            if int(raw) in (3, 4):  # Rithmic StopLimit / StopMarket
                return TriggerType.DEFAULT
        except (TypeError, ValueError):
            pass
        return TriggerType.NO_TRIGGER

    def _order_status_from_event(self, fields: dict[str, Any]) -> OrderStatus:
        kind = fields.get("kind")
        status_u = str(fields.get("status") or "").upper()
        try:
            qty = int(fields.get("quantity") or 0)
            filled = int(fields.get("total_fill_size") or 0)
        except (TypeError, ValueError):
            qty = filled = 0
        if kind == "filled" or (qty > 0 and filled >= qty):
            return OrderStatus.FILLED
        # A rejected cancel means the venue refused the cancel and the order is
        # still working — not rejected, not pending-cancel. Evaluate before the
        # REJECT substring so a cancel-rejected row whose status string contains
        # "REJECT" (e.g. a venue "REJECTED"/"CANCELLATION_REJECTED") is not
        # misreported as terminal REJECTED. Reporting PENDING_CANCEL during
        # recon would leave the OMS believing a cancel is in flight forever.
        if kind == "cancel_rejected":
            return OrderStatus.ACCEPTED
        # Check reject kinds/status before the CANCEL substring so a rejected
        # cancel (e.g. "CANCELLATION_FAILED") is not misreported as CANCELED.
        if kind in ("rejected", "modify_rejected") or "REJECT" in status_u:
            return OrderStatus.REJECTED
        if kind == "canceled" or status_u in ("CANCELLED", "CANCELED"):
            return OrderStatus.CANCELED
        if kind == "expired" or "EXPIRED" in status_u:
            return OrderStatus.EXPIRED
        if qty > 0 and 0 < filled < qty:
            return OrderStatus.PARTIALLY_FILLED
        if kind in ("accepted", "updated") or status_u in ("OPEN", "WORKING"):
            return OrderStatus.ACCEPTED
        return OrderStatus.ACCEPTED

    def _client_order_id_for_tag(self, tag: Any) -> ClientOrderId | None:
        # user_tag == client_order_id.value for orders this client placed, so
        # the tag is the client order id. External orders keep their tag as the
        # report's client_order_id (honest: it is the id on the wire).
        if not tag:
            return None
        return ClientOrderId(str(tag))

    def _order_status_report_from_fields(
        self,
        fields: dict[str, Any],
        ts_event: int,
        *,
        strict: bool = False,
    ) -> OrderStatusReport | None:
        """Build an ``OrderStatusReport`` from normalized wire fields.

        The best-effort drain is deliberately permissive: unknown closed-set
        fields fall back to ``BUY``/``MARKET``/``GTC``/``ACCEPTED`` so one
        malformed row cannot abort the whole recon. ``strict=True`` (the
        in-flight recovery drain) never fabricates closed-set execution terms:
        side, ``price_type``, ``duration``, and a recognisable status must all
        be present and mappable, or the row is unusable.
        """
        basket = fields.get("basket_id")
        instrument_raw = fields.get("instrument_id")
        symbol = fields.get("symbol")
        if not basket or not (instrument_raw or symbol):
            return None
        try:
            instrument_id = InstrumentId.from_str(
                str(instrument_raw or f"{symbol}.{VENUE}")
            )
        except Exception:
            return None
        account_raw = fields.get("account_id")
        if account_raw:
            self._seed_account_if_needed(str(account_raw))
        if self.account_id is None:
            return None
        try:
            side = order_side_from_notification(fields)
            qty = Quantity.from_int(max(0, int(fields.get("quantity") or 0)))
            filled = Quantity.from_int(max(0, int(fields.get("total_fill_size") or 0)))
            price_raw = fields.get("price")
            trigger_raw = fields.get("trigger_price")
            price = _price(price_raw) if price_raw is not None else None
            trigger = _price(trigger_raw) if trigger_raw is not None else None
            avg = fields.get("avg_fill_price")
            avg_px = Decimal(str(avg)) if avg is not None else None
            if qty <= 0:
                # No order terms (e.g. a bare TRIGGER notification): a status
                # report cannot be built. Skip rather than crash the handler
                # (the constructor rejects a zero quantity).
                return None
            order_type = self._order_type_from_event(fields)
            tif = self._tif_from_event(fields)
            status = self._order_status_from_event(fields)
            if strict:
                # Never fabricate closed-set execution terms for recovery: the
                # permissive defaults (side -> BUY, price_type -> MARKET,
                # duration -> GTC, status -> ACCEPTED) must not bind a venue
                # id from a row the adapter cannot actually trust.
                if side is None:
                    return None
                price_type = fields.get("price_type")
                duration = fields.get("duration")
                kind = fields.get("kind")
                status_u = str(fields.get("status") or "").upper()
                recognizable = kind in (
                    "accepted",
                    "updated",
                    "canceled",
                    "filled",
                    "rejected",
                    "modify_rejected",
                    "cancel_rejected",
                    "expired",
                ) or any(
                    marker in status_u
                    for marker in ("OPEN", "WORKING", "CANCEL", "REJECT", "EXPIRED")
                )
                if (
                    price_type is None
                    or duration is None
                    or int(price_type) not in _RITHMIC_PRICE_TYPE_TO_ORDER_TYPE
                    or int(duration) not in _RITHMIC_DURATION_TO_TIF
                    or not recognizable
                ):
                    return None
            return OrderStatusReport(
                account_id=self.account_id,
                instrument_id=instrument_id,
                venue_order_id=VenueOrderId(str(basket)),
                order_side=side or OrderSide.BUY,
                order_type=order_type,
                time_in_force=tif,
                order_status=status,
                quantity=qty,
                filled_qty=filled,
                report_id=UUID4(),
                ts_accepted=ts_event,
                ts_last=ts_event,
                ts_init=self._clock.timestamp_ns(),
                client_order_id=self._client_order_id_for_tag(fields.get("user_tag")),
                price=price,
                trigger_price=trigger,
                trigger_type=self._trigger_type_from_event(fields),
                avg_px=avg_px,
            )
        except (TypeError, ValueError, OverflowError):
            # Skip a malformed row rather than abort the whole recon response.
            return None

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        # Venue recon touches the order plant (a trading capability); a read-only
        # client must not log the order plant in during reconciliation, and
        # reports only its locally cached orders (never claims venue authority).
        if not self.enable_trading:
            return self._cache_backed_order_status_reports(command)
        if not self._order_plant.load_orders_available():
            raise VenueQueryUnavailable(
                "Rithmic order reconciliation unavailable (order plant not ready)"
            )
        start_sec, end_sec = self._recon_window_sec(command.start, command.end)
        events = await self._load_orders_events(start_sec, end_sec)
        if not events:
            # Best-effort drain returned nothing. This is advisory, not proof the
            # venue is empty: a quiet or lossy channel looks identical. With
            # Nautilus' open-order consistency check, a cached order missing from
            # an empty drain is canceled only when open_check_open_only=False, so
            # the operator must keep open_check_open_only=True (the 1.231.x
            # replacement for the removed death_policy=trust_stop) to stay safe.
            self._log.warning(
                "order recon returned no working orders from the best-effort "
                "drain; advisory, not an authoritative venue-empty. Keep "
                "open_check_open_only=True so Nautilus does not cancel tracked "
                "open orders on an empty recon."
            )
        latest: dict[str, tuple[int, OrderStatusReport]] = {}
        for raw in events:
            try:
                fields = order_notification_to_fields(raw)
            except Exception:
                continue
            basket = fields.get("basket_id")
            if not basket:
                continue
            # GenerateOrderStatusReports carries no venue_order_id (only
            # GenerateFillReports does); treat it as absent for filtering.
            venue_order_id = getattr(command, "venue_order_id", None)
            if (
                command.instrument_id is not None or venue_order_id is not None
            ) and not (
                self._matches_instrument(fields, command.instrument_id, venue_order_id)
            ):
                continue
            ts_event = int(fields.get("ts_event") or 0)
            report = self._order_status_report_from_fields(fields, ts_event)
            if report is None:
                continue
            key = str(basket)
            # Keep the latest row; on an equal timestamp (e.g. both 0 when
            # ts_event is missing) prefer the last-arrived row so a terminal
            # status following an earlier non-terminal is not masked.
            if key not in latest or ts_event >= latest[key][0]:
                latest[key] = (ts_event, report)
        reports = [report for _, report in latest.values()]
        if command.open_only:
            reports = [
                r for r in reports if r.order_status not in _TERMINAL_ORDER_STATUSES
            ]
        return reports

    async def generate_fill_reports(
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        if not self.enable_trading or not self._order_plant.load_orders_available():
            raise VenueQueryUnavailable(
                "Rithmic fill reconciliation unavailable (order plant not ready)"
            )
        start_sec, end_sec = self._recon_window_sec(command.start, command.end)
        events = await self._load_orders_events(start_sec, end_sec)
        reports: list[FillReport] = []
        for raw in events:
            try:
                fields = order_notification_to_fields(raw)
            except Exception:
                continue
            if fields.get("kind") != "filled":
                continue
            if not self._matches_instrument(
                fields, command.instrument_id, command.venue_order_id
            ):
                continue
            ts_event = int(fields.get("ts_event") or self._clock.timestamp_ns())
            # Share the adapter-wide fill dedup store (live path + recon) so a
            # fill already emitted live, or duplicated across the summary/today
            # drains, is not re-emitted as a second reconciliation fill.
            dedup = fill_dedup_key(fields, ts_event=ts_event)
            if self._fill_key_seen(dedup):
                continue
            report = self._fill_report_from_fields(fields, ts_event)
            if report is not None:
                # Nautilus cannot reconcile a fill without an order prerequisite.
                # Reconciliation can discover a fill after the live order event
                # was missed, so publish a venue status first; this may create a
                # synthetic external order when no cached strategy order exists.
                status = self._order_status_report_from_fields(fields, ts_event)
                if status is None:
                    # No order-status prerequisite could be built (status-only
                    # fields malformed) — skip the fill rather than emit it
                    # without the order Nautilus needs to reconcile it against.
                    self._log.warning(
                        "fill reconciliation skipped: no order status prerequisite "
                        f"for {slim_order_fields(fields)}"
                    )
                    continue
                if not RithmicExecutionClient._publish_order_status_report(
                    self,
                    status,
                    context="fill reconciliation prerequisite",
                ):
                    continue
                reports.append(report)
                self._mark_fill_key(dedup)
        return reports

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        ts_init = self._clock.timestamp_ns()
        reports: list[PositionStatusReport] = []
        instrument_filter = command.instrument_id
        for fields in self._positions.values():
            instrument_id = InstrumentId.from_str(str(fields["instrument_id"]))
            if instrument_filter is not None and instrument_id != instrument_filter:
                continue
            # The Rithmic account can report contracts this node never loaded
            # (e.g. NQU6 alongside MNQU6). Reconciling an unloaded instrument
            # cannot resolve (no instrument/price precision in cache) and only
            # logs "instrument not found" noise, so skip it — loudly if the
            # venue shows a non-zero position we would otherwise hide.
            if self._cache.instrument(instrument_id) is None:
                qty = int(fields.get("quantity") or 0)
                if qty != 0:
                    self._log.warning(
                        f"venue reports {instrument_id} position qty={qty} but the "
                        "instrument is not loaded by this node — skipping recon"
                    )
                continue
            report = self._position_report_from_fields(fields, ts_init)
            if report is not None:
                reports.append(report)
        return reports


# Back-compat alias used by factories / tests.
RithmicReadOnlyExecutionClient = RithmicExecutionClient
