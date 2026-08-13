"""Live execution client for Rithmic (Phase 1 read-only + Phase 2 trading)."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

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

from rithmic_connect._convert import account_pnl_to_fields
from rithmic_connect._convert import instrument_pnl_to_fields
from rithmic_connect._orders import OrderMapError
from rithmic_connect._orders import is_exchange_cancel
from rithmic_connect._orders import is_exchange_fill
from rithmic_connect._orders import is_exchange_reject
from rithmic_connect._orders import is_rithmic_cancel_failed
from rithmic_connect._orders import is_rithmic_complete
from rithmic_connect._orders import is_rithmic_modify_failed
from rithmic_connect._orders import is_rithmic_open
from rithmic_connect._orders import nautilus_order_type_to_rithmic
from rithmic_connect._orders import nautilus_side_to_rithmic
from rithmic_connect._orders import nautilus_tif_to_rithmic
from rithmic_connect._orders import order_notification_to_fields
from rithmic_connect.config import RithmicExecClientConfig
from rithmic_connect.constants import ADAPTER_NAME
from rithmic_connect.constants import VENUE
from rithmic_connect.providers import RithmicInstrumentProvider
from rithmic_connect.session import WireSession


_POSITION_SIDE = {
    "LONG": PositionSide.LONG,
    "SHORT": PositionSide.SHORT,
    "FLAT": PositionSide.FLAT,
}


def _price(value: float | Decimal | str) -> Price:
    text = f"{float(value):.8f}".rstrip("0").rstrip(".")
    if "." not in text:
        text = f"{text}.0"
    return Price.from_str(text)


class RithmicExecutionClient(LiveExecutionClient):
    """Rithmic execution client.

    When ``config.enable_trading`` is false (default), order actions are rejected
    and only account/PnL publishing runs (Phase 1). When true, the order plant is
    used for submit/cancel/modify and notifications drive Nautilus order events.
    """

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
        self._order_calls: list[str] = []
        self._positions: dict[str, dict[str, Any]] = {}
        # client_order_id.value -> basket_id / venue id
        self._client_to_venue: dict[str, str] = {}
        self._venue_to_client: dict[str, ClientOrderId] = {}
        self._tag_to_client: dict[str, ClientOrderId] = {}

    @property
    def enable_trading(self) -> bool:
        return bool(self._config_local.enable_trading)

    async def _connect(self) -> None:
        await asyncio.to_thread(self._session.connect)

        if self._config_local.session.has_account():
            try:
                await asyncio.to_thread(self._session.subscribe_pnl)
                self._poll_task = self.create_task(self._poll_loop(), log_msg="rithmic_pnl_poll")
            except Exception as exc:  # noqa: BLE001
                if self._config_local.soft_fail_pnl:
                    self._log.warning(f"PnL/account path soft-failed: {exc}")
                else:
                    raise

        if self.enable_trading:
            if not self._config_local.session.has_account():
                raise ValueError("enable_trading requires account_id/fcm_id/ib_id")
            await asyncio.to_thread(self._session.subscribe_order_updates)
            self._order_poll_task = self.create_task(
                self._order_poll_loop(),
                log_msg="rithmic_order_poll",
            )

    async def _disconnect(self) -> None:
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

    async def _poll_loop(self) -> None:
        while True:
            poll_pnl = getattr(self._session, "poll_pnl_event", None)
            if callable(poll_pnl):
                event = await asyncio.to_thread(poll_pnl)
            else:
                event = await asyncio.to_thread(self._session.poll_event)
            if event is None:
                await asyncio.sleep(0.05)
                continue
            etype = event.get("type")
            if etype == "account_pnl":
                self._publish_account(event)
            elif etype == "instrument_pnl":
                self._cache_position(event)

    async def _order_poll_loop(self) -> None:
        while True:
            poll = getattr(self._session, "poll_order_event", None)
            if not callable(poll):
                await asyncio.sleep(0.05)
                continue
            event = await asyncio.to_thread(poll)
            if event is None:
                await asyncio.sleep(0.05)
                continue
            if event.get("type") != "order_notification":
                continue
            try:
                fields = order_notification_to_fields(event)
            except Exception as exc:  # noqa: BLE001
                self._log.error(f"invalid order_notification: {exc}")
                continue
            self._handle_order_notification(fields)

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

    def _route(self, instrument_id: InstrumentId) -> tuple[str, str]:
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            raise ValueError(f"instrument not in cache: {instrument_id}")
        info = getattr(instrument, "info", None) or {}
        symbol = info.get("rithmic_symbol")
        exchange = info.get("rithmic_exchange")
        if not symbol or not exchange:
            raise ValueError(
                f"instrument {instrument_id} missing rithmic_symbol/rithmic_exchange in info"
            )
        return str(symbol), str(exchange)

    def _reject_order(self, action: str) -> None:
        self._order_calls.append(action)
        mode = "trading disabled" if not self.enable_trading else "rejected"
        self._log.error(f"Rithmic exec client {mode}: {action}")

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
        if ts_event is None:
            ts_event = self._clock.timestamp_ns()
        else:
            ts_event = int(ts_event)
        strategy_id = order.strategy_id
        instrument_id = order.instrument_id
        basket = fields.get("basket_id")
        if basket:
            self._bind_venue_id(client_order_id, str(basket))
        venue_order_id = VenueOrderId(str(basket or self._client_to_venue.get(client_order_id.value) or client_order_id.value))

        if is_rithmic_open(fields):
            self.generate_order_accepted(
                strategy_id,
                instrument_id,
                client_order_id,
                venue_order_id,
                ts_event,
            )
            return

        if is_exchange_reject(fields):
            reason = fields.get("text") or fields.get("report_text") or fields.get("status") or "REJECT"
            self.generate_order_rejected(
                strategy_id,
                instrument_id,
                client_order_id,
                str(reason),
                ts_event,
            )
            return

        if is_rithmic_modify_failed(fields):
            reason = fields.get("text") or fields.get("status") or "MODIFICATION_FAILED"
            self.generate_order_modify_rejected(
                strategy_id,
                instrument_id,
                client_order_id,
                venue_order_id,
                str(reason),
                ts_event,
            )
            return

        if is_rithmic_cancel_failed(fields):
            reason = fields.get("text") or fields.get("status") or "CANCELLATION_FAILED"
            self.generate_order_cancel_rejected(
                strategy_id,
                instrument_id,
                client_order_id,
                venue_order_id,
                str(reason),
                ts_event,
            )
            return

        if is_exchange_cancel(fields) or (
            is_rithmic_complete(fields)
            and str(fields.get("status") or "").upper() in {"CANCELLED", "CANCELED"}
        ):
            self.generate_order_canceled(
                strategy_id,
                instrument_id,
                client_order_id,
                venue_order_id,
                ts_event,
            )
            return

        if is_exchange_fill(fields):
            fill_px = fields.get("fill_price")
            fill_sz = fields.get("fill_size")
            if fill_px is None or fill_sz is None:
                self._log.error(f"fill notification missing fill_price/fill_size: {fields}")
                return
            fill_id = fields.get("fill_id") or fields.get("exchange_order_id") or f"{basket}-{ts_event}"
            commission = Money(Decimal("0"), Currency.from_str("USD"))
            self.generate_order_filled(
                strategy_id,
                instrument_id,
                client_order_id,
                venue_order_id,
                None,
                TradeId(str(fill_id)),
                order.side,
                order.order_type,
                Quantity.from_int(int(fill_sz)),
                _price(fill_px),
                Currency.from_str("USD"),
                commission,
                LiquiditySide.NO_LIQUIDITY_SIDE,
                ts_event,
                info={"rithmic": dict(fields)},
            )
            return

    async def _submit_order(self, command: SubmitOrder) -> None:
        if not self.enable_trading:
            self._reject_order("submit_order")
            return
        order: Order = command.order
        try:
            symbol, exchange = self._route(order.instrument_id)
            side = nautilus_side_to_rithmic(order.side)
            price_type = nautilus_order_type_to_rithmic(order.order_type)
            duration = nautilus_tif_to_rithmic(order.time_in_force)
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
            )
        except Exception as exc:  # noqa: BLE001
            self.generate_order_rejected(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                str(exc),
                self._clock.timestamp_ns(),
            )

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        if not self.enable_trading:
            self._reject_order("submit_order_list")
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
            self._reject_order("modify_order")
            return
        order = self._cache.order(command.client_order_id)
        if order is None:
            self._log.error(f"modify_order: order not in cache {command.client_order_id}")
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
            self._reject_order("cancel_order")
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
            self._reject_order("cancel_all_orders")
            return
        try:
            await asyncio.to_thread(self._session.cancel_all_orders)
        except Exception as exc:  # noqa: BLE001
            self._log.error(f"cancel_all_orders failed: {exc}")

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        _ = command
        return None

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        _ = command
        return []

    async def generate_fill_reports(
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        _ = command
        return []

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
