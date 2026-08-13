"""Live execution client for Rithmic (Phase 1 read-only + Phase 2 trading)."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any, Callable

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import Order

from rithmic_nt_connect._convert import account_pnl_to_fields
from rithmic_nt_connect._convert import format_price_str
from rithmic_nt_connect._convert import instrument_pnl_to_fields
from rithmic_nt_connect._convert import rithmic_route_from_info
from rithmic_nt_connect._order_plant import OrderPlantPolicy
from rithmic_nt_connect._order_plant import OrderPlantState
from rithmic_nt_connect._orders import OrderMapError
from rithmic_nt_connect._orders import DEFAULT_TRAIL_BY_PRICE_ID
from rithmic_nt_connect._orders import nautilus_order_type_to_rithmic
from rithmic_nt_connect._orders import nautilus_side_to_rithmic
from rithmic_nt_connect._orders import nautilus_tif_to_rithmic
from rithmic_nt_connect._orders import notification_action
from rithmic_nt_connect._orders import order_notification_to_fields
from rithmic_nt_connect._orders import trailing_ticks_from_order
from rithmic_nt_connect.config import RithmicExecClientConfig
from rithmic_nt_connect.constants import ADAPTER_NAME
from rithmic_nt_connect.constants import VENUE
from rithmic_nt_connect.errors import CHANNEL_ERRORS
from rithmic_nt_connect.errors import VenueQueryUnavailable
from rithmic_nt_connect.providers import RithmicInstrumentProvider
from rithmic_nt_connect.session import WireSession


_POSITION_SIDE = {
    "LONG": PositionSide.LONG,
    "SHORT": PositionSide.SHORT,
    "FLAT": PositionSide.FLAT,
}


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
        config: RithmicExecClientConfig,
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
        self._client_to_venue: dict[str, str] = {}
        self._venue_to_client: dict[str, ClientOrderId] = {}
        self._tag_to_client: dict[str, ClientOrderId] = {}
        self._order_plant = OrderPlantPolicy(OrderPlantState.DISCONNECTED)

    @property
    def enable_trading(self) -> bool:
        return bool(self._config_local.enable_trading)

    async def _connect(self) -> None:
        await asyncio.to_thread(self._session.connect)

        if self._config_local.session.has_account():
            try:
                await asyncio.to_thread(self._session.subscribe_pnl)
                self._poll_task = self.create_task(
                    self._plant_poll_loop(
                        name="pnl",
                        poll_fn=self._session.poll_pnl_event,
                        on_event=self._dispatch_pnl_event,
                        on_resync=self._resync_pnl_subscription,
                    ),
                    log_msg="rithmic_pnl_poll",
                )
            except Exception as exc:  # noqa: BLE001
                if self._config_local.soft_fail_pnl:
                    self._log.warning(f"PnL/account path soft-failed: {exc}")
                else:
                    raise

        if self.enable_trading:
            if not self._config_local.session.has_account():
                raise ValueError("enable_trading requires account_id/fcm_id/ib_id")
            self._order_plant.state = OrderPlantState.CONNECTING
            try:
                await asyncio.to_thread(self._session.subscribe_order_updates)
            except Exception:
                self._order_plant.state = OrderPlantState.DISCONNECTED
                try:
                    await asyncio.to_thread(self._session.disconnect_order_plant)
                except Exception as teardown_exc:  # noqa: BLE001
                    self._log.warning(f"order plant teardown after subscribe fail: {teardown_exc}")
                raise
            self._order_plant.state = OrderPlantState.LIVE
            self._order_poll_task = self.create_task(
                self._plant_poll_loop(
                    name="order",
                    poll_fn=self._session.poll_order_event,
                    on_event=self._dispatch_order_event,
                    on_resync=self._resync_order_subscription,
                ),
                log_msg="rithmic_order_poll",
            )

    async def _resync_order_subscription(self) -> None:
        self._order_plant.state = OrderPlantState.RESYNCING
        await asyncio.to_thread(self._session.disconnect_order_plant)
        await asyncio.to_thread(self._session.subscribe_order_updates)
        self._order_plant.state = OrderPlantState.LIVE

    async def _resync_pnl_subscription(self) -> None:
        await asyncio.to_thread(self._session.disconnect_pnl_plant)
        await asyncio.to_thread(self._session.ensure_pnl_plant)
        await asyncio.to_thread(self._session.subscribe_pnl)

    async def _poll_session_event(
        self,
        poll_fn: Callable[[], dict[str, Any] | None],
    ) -> dict[str, Any] | None:
        """Return next event, or None on transient errors; re-raise channel failures."""
        try:
            return await asyncio.to_thread(poll_fn)
        except CHANNEL_ERRORS:
            raise
        except Exception as exc:  # noqa: BLE001
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
            except Exception as exc:  # noqa: BLE001
                self._log.error(f"{name} poll channel error: {exc}")
                if name == "order":
                    self._order_plant.state = OrderPlantState.RESYNCING
                try:
                    await on_resync()
                    self._log.warning(f"{name} subscription resynced after channel error")
                    backoff = 0.05
                except Exception as resync_exc:  # noqa: BLE001
                    self._log.error(f"{name} subscription resync failed: {resync_exc}")
                    if name == "order":
                        self._order_plant.state = OrderPlantState.DISCONNECTED
                    backoff = min(backoff * 2, 2.0)
                await asyncio.sleep(backoff)
                continue
            if event is None:
                await asyncio.sleep(0.05)
                continue
            on_event(event)

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
        except Exception as exc:  # noqa: BLE001
            self._log.error(f"invalid order_notification: {exc}")
            return
        self._handle_order_notification(fields)

    async def _disconnect(self) -> None:
        self._order_plant.state = OrderPlantState.DISCONNECTED
        for task_attr in ("_order_poll_task", "_poll_task"):
            task = getattr(self, task_attr)
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                setattr(self, task_attr, None)
        try:
            await asyncio.to_thread(self._session.disconnect)
        except Exception as exc:  # noqa: BLE001
            self._log.warning(f"disconnect warning: {exc}")

    def _publish_account(self, event: dict[str, Any]) -> None:
        try:
            fields = account_pnl_to_fields(event)
        except Exception as exc:  # noqa: BLE001
            self._log.error(f"invalid account_pnl: {exc}")
            return
        account_id = AccountId(f"{VENUE}-{fields['account_id']}")
        if self.account_id is None:
            self._set_account_id(account_id)
        currency_raw = fields.get("currency")
        if currency_raw is None:
            self._log.error("account_pnl missing currency")
            return
        currency = Currency.from_str(str(currency_raw))
        free_raw = fields.get("cash_on_hand")
        if free_raw is None:
            free_raw = fields.get("account_balance")
        if free_raw is None:
            self._log.error("account_pnl missing cash_on_hand and account_balance")
            return
        try:
            free_dec = Decimal(str(free_raw))
        except Exception as exc:  # noqa: BLE001
            self._log.error(f"account_pnl balance not numeric ({free_raw!r}): {exc}")
            return
        free = Money(free_dec, currency)
        locked = Money(Decimal("0"), currency)
        total = free
        balances = [AccountBalance(total, locked, free)]
        self.generate_account_state(
            balances=balances,
            margins=[],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
            info={"rithmic_account_id": fields["account_id"]},
        )

    def _cache_position(self, event: dict[str, Any]) -> None:
        try:
            fields = instrument_pnl_to_fields(event)
        except Exception as exc:  # noqa: BLE001
            self._log.error(f"invalid instrument_pnl: {exc}")
            return
        self._positions[str(fields["instrument_id"])] = fields
        account_raw = fields.get("account_id")
        if account_raw and self.account_id is None:
            self._set_account_id(AccountId(f"{VENUE}-{account_raw}"))

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

    def _resolve_client_order_id(self, fields: dict[str, Any]) -> ClientOrderId | None:
        tag = fields.get("user_tag")
        if tag and str(tag) in self._tag_to_client:
            return self._tag_to_client[str(tag)]
        basket = fields.get("basket_id")
        if basket and str(basket) in self._venue_to_client:
            return self._venue_to_client[str(basket)]
        return None

    def _bind_venue_id(self, client_order_id: ClientOrderId, venue_id: str) -> None:
        self._client_to_venue[client_order_id.value] = venue_id
        self._venue_to_client[venue_id] = client_order_id

    def _venue_id_for(
        self,
        client_order_id: ClientOrderId,
        fields: dict[str, Any],
    ) -> str:
        basket = fields.get("basket_id")
        if basket:
            return str(basket)
        return str(self._client_to_venue.get(client_order_id.value) or client_order_id.value)

    def _handle_order_notification(self, fields: dict[str, Any]) -> None:
        client_order_id = self._resolve_client_order_id(fields)
        if client_order_id is None:
            self._log.debug(f"order notification for unknown order: {fields}")
            return
        order = self._cache.order(client_order_id)
        if order is None:
            self._log.warning(f"cached order missing for {client_order_id}")
            return
        ts_event = fields.get("ts_event")
        ts_event = int(ts_event) if ts_event is not None else self._clock.timestamp_ns()
        basket = fields.get("basket_id")
        if basket:
            self._bind_venue_id(client_order_id, str(basket))
        venue_order_id = VenueOrderId(self._venue_id_for(client_order_id, fields))
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
                strategy_id, instrument_id, client_order_id, str(action.reason), ts_event
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
            self.generate_order_triggered(
                strategy_id, instrument_id, client_order_id, venue_order_id, ts_event
            )
        elif action.kind == "filled":
            if action.fill_px is None or action.fill_qty is None or action.trade_id is None:
                self._log.error(f"fill action missing fields: {fields}")
                return
            commission = Money(Decimal("0"), Currency.from_str("USD"))
            self.generate_order_filled(
                strategy_id,
                instrument_id,
                client_order_id,
                venue_order_id,
                None,
                TradeId(str(action.trade_id)),
                order.side,
                order.order_type,
                Quantity.from_int(int(action.fill_qty)),
                self._price_for_instrument(instrument_id, action.fill_px),
                Currency.from_str("USD"),
                commission,
                LiquiditySide.NO_LIQUIDITY_SIDE,
                ts_event,
                info={"rithmic": dict(fields)},
            )

    def _order_status_report_for(
        self,
        order: Order,
        ts_init: int,
    ) -> OrderStatusReport | None:
        venue_id = self._client_to_venue.get(order.client_order_id.value)
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
        )

    def _cache_backed_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        ts_init = self._clock.timestamp_ns()
        reports: list[OrderStatusReport] = []
        orders = (
            self._cache.orders_open(venue=self.venue, instrument_id=command.instrument_id)
            if command.open_only
            else self._cache.orders(venue=self.venue, instrument_id=command.instrument_id)
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
            self.generate_order_rejected(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                "Rithmic trading disabled (enable_trading=False)",
                self._clock.timestamp_ns(),
            )
            return
        if not self._order_plant.allow_submit():
            self.generate_order_rejected(
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
            self.generate_order_rejected(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                str(exc),
                self._clock.timestamp_ns(),
            )
            return

        user_tag = order.client_order_id.value
        self._tag_to_client[user_tag] = order.client_order_id
        price = float(order.price) if order.has_price else None
        trigger = float(order.trigger_price) if order.has_trigger_price else None
        qty = int(order.quantity)
        trail_by_price_id = DEFAULT_TRAIL_BY_PRICE_ID if trail_by_ticks is not None else None

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
        except Exception as exc:  # noqa: BLE001
            self._tag_to_client.pop(user_tag, None)
            self.generate_order_rejected(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                str(exc),
                self._clock.timestamp_ns(),
            )

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        if not self.enable_trading:
            self._log_trading_disabled("submit_order_list")
            for order in command.order_list.orders:
                self.generate_order_rejected(
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
        venue_id = self._client_to_venue.get(command.client_order_id.value)
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
        qty = int(command.quantity) if command.quantity is not None else int(order.quantity)
        price = float(command.price) if command.price is not None else (
            float(order.price) if order.has_price else None
        )
        trigger = float(command.trigger_price) if command.trigger_price is not None else (
            float(order.trigger_price) if order.has_trigger_price else None
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
        except Exception as exc:  # noqa: BLE001
            self.generate_order_modify_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                VenueOrderId(venue_id),
                str(exc),
                self._clock.timestamp_ns(),
            )

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
            venue_id = self._client_to_venue.get(command.client_order_id.value)
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
        except Exception as exc:  # noqa: BLE001
            self.generate_order_cancel_rejected(
                command.strategy_id,
                command.instrument_id,
                command.client_order_id,
                VenueOrderId(venue_id),
                str(exc),
                self._clock.timestamp_ns(),
            )

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
        except Exception as exc:  # noqa: BLE001
            self._log.error(f"cancel_all_orders failed: {exc}")

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        order = self._cache.order(command.client_order_id) if command.client_order_id else None
        if order is None and command.venue_order_id is not None:
            mapped = self._venue_to_client.get(command.venue_order_id.value)
            if mapped is not None:
                order = self._cache.order(mapped)
        if order is None:
            return None
        return self._order_status_report_for(order, self._clock.timestamp_ns())

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        # No venue snapshot API — always cache-backed. Never return [] as "venue empty".
        return self._cache_backed_order_status_reports(command)

    async def generate_fill_reports(
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        _ = command
        raise VenueQueryUnavailable(
            "Rithmic fill report query unavailable (no venue fill snapshot API)"
        )

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        ts_init = self._clock.timestamp_ns()
        reports: list[PositionStatusReport] = []
        instrument_filter = command.instrument_id
        for fields in self._positions.values():
            if instrument_filter is not None and str(fields["instrument_id"]) != str(
                instrument_filter
            ):
                continue
            report = self._position_report_from_fields(fields, ts_init)
            if report is not None:
                reports.append(report)
        return reports


# Back-compat alias used by factories / tests.
RithmicReadOnlyExecutionClient = RithmicExecutionClient
