"""Order notification -> Nautilus event routing (plain Python, testable)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable, Protocol

from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import StrategyId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

from rithmic_connect._convert import format_price_str
from rithmic_connect._orders import is_exchange_cancel
from rithmic_connect._orders import is_exchange_fill
from rithmic_connect._orders import is_exchange_not_cancelled
from rithmic_connect._orders import is_exchange_not_modified
from rithmic_connect._orders import is_exchange_reject
from rithmic_connect._orders import is_exchange_trigger
from rithmic_connect._orders import is_rithmic_cancel_failed
from rithmic_connect._orders import is_rithmic_complete
from rithmic_connect._orders import is_rithmic_modified
from rithmic_connect._orders import is_rithmic_modify_failed
from rithmic_connect._orders import is_rithmic_open
from rithmic_connect._orders import trade_id_from_fill_fields


def _price(value: float | Decimal | str) -> Price:
    return Price.from_str(format_price_str(value))


class OrderEventEmitter(Protocol):
    def generate_order_accepted(
        self,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        ts_event: int,
    ) -> None: ...

    def generate_order_rejected(
        self,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        reason: str,
        ts_event: int,
    ) -> None: ...

    def generate_order_modify_rejected(
        self,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        reason: str,
        ts_event: int,
    ) -> None: ...

    def generate_order_cancel_rejected(
        self,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        reason: str,
        ts_event: int,
    ) -> None: ...

    def generate_order_updated(
        self,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        quantity: Quantity,
        price: Price | None,
        trigger_price: Price | None,
        ts_event: int,
    ) -> None: ...

    def generate_order_canceled(
        self,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        ts_event: int,
    ) -> None: ...

    def generate_order_triggered(
        self,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        ts_event: int,
    ) -> None: ...

    def generate_order_filled(
        self,
        strategy_id: StrategyId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        venue_position_id: Any,
        trade_id: TradeId,
        order_side: Any,
        order_type: Any,
        last_qty: Quantity,
        last_px: Price,
        quote_currency: Currency,
        commission: Money,
        liquidity_side: LiquiditySide,
        ts_event: int,
        info: dict[str, Any] | None = None,
    ) -> None: ...


def route_order_notification(
    fields: dict[str, Any],
    *,
    resolve_client_order_id: Callable[[dict[str, Any]], ClientOrderId | None],
    get_order: Callable[[ClientOrderId], Any],
    bind_venue_id: Callable[[ClientOrderId, str], None],
    venue_id_for: Callable[[ClientOrderId, dict[str, Any]], str],
    clock_ts: Callable[[], int],
    emit: OrderEventEmitter,
    log_debug: Callable[[str], None],
    log_warning: Callable[[str], None],
    log_error: Callable[[str], None],
) -> None:
    """Map a normalized order notification to Nautilus order events."""
    client_order_id = resolve_client_order_id(fields)
    if client_order_id is None:
        log_debug(f"order notification for unknown order: {fields}")
        return
    order = get_order(client_order_id)
    if order is None:
        log_warning(f"cached order missing for {client_order_id}")
        return
    ts_event = fields.get("ts_event")
    if ts_event is None:
        ts_event = clock_ts()
    else:
        ts_event = int(ts_event)
    strategy_id = order.strategy_id
    instrument_id = order.instrument_id
    basket = fields.get("basket_id")
    if basket:
        bind_venue_id(client_order_id, str(basket))
    venue_order_id = VenueOrderId(venue_id_for(client_order_id, fields))

    if is_rithmic_open(fields):
        emit.generate_order_accepted(
            strategy_id, instrument_id, client_order_id, venue_order_id, ts_event
        )
        return

    if is_exchange_reject(fields):
        reason = fields.get("text") or fields.get("report_text") or fields.get("status") or "REJECT"
        emit.generate_order_rejected(strategy_id, instrument_id, client_order_id, str(reason), ts_event)
        return

    if is_rithmic_modify_failed(fields):
        reason = fields.get("text") or fields.get("status") or "MODIFICATION_FAILED"
        emit.generate_order_modify_rejected(
            strategy_id, instrument_id, client_order_id, venue_order_id, str(reason), ts_event
        )
        return

    if is_exchange_not_modified(fields):
        reason = fields.get("text") or fields.get("report_text") or fields.get("status") or "NOT_MODIFIED"
        emit.generate_order_modify_rejected(
            strategy_id, instrument_id, client_order_id, venue_order_id, str(reason), ts_event
        )
        return

    if is_rithmic_cancel_failed(fields):
        reason = fields.get("text") or fields.get("status") or "CANCELLATION_FAILED"
        emit.generate_order_cancel_rejected(
            strategy_id, instrument_id, client_order_id, venue_order_id, str(reason), ts_event
        )
        return

    if is_exchange_not_cancelled(fields):
        reason = fields.get("text") or fields.get("report_text") or fields.get("status") or "NOT_CANCELLED"
        emit.generate_order_cancel_rejected(
            strategy_id, instrument_id, client_order_id, venue_order_id, str(reason), ts_event
        )
        return

    if is_rithmic_modified(fields):
        qty_raw = fields.get("quantity")
        qty = Quantity.from_int(int(qty_raw)) if qty_raw is not None else order.quantity
        price_raw = fields.get("price")
        trigger_raw = fields.get("trigger_price")
        price = (
            _price(price_raw)
            if price_raw is not None
            else (order.price if order.has_price else None)
        )
        trigger = (
            _price(trigger_raw)
            if trigger_raw is not None
            else (order.trigger_price if order.has_trigger_price else None)
        )
        emit.generate_order_updated(
            strategy_id,
            instrument_id,
            client_order_id,
            venue_order_id,
            qty,
            price,
            trigger,
            ts_event,
        )
        return

    if is_exchange_cancel(fields) or (
        is_rithmic_complete(fields)
        and str(fields.get("status") or "").upper() in {"CANCELLED", "CANCELED"}
    ):
        emit.generate_order_canceled(
            strategy_id, instrument_id, client_order_id, venue_order_id, ts_event
        )
        return

    if is_exchange_trigger(fields):
        emit.generate_order_triggered(
            strategy_id, instrument_id, client_order_id, venue_order_id, ts_event
        )
        return

    if is_exchange_fill(fields):
        fill_px = fields.get("fill_price")
        fill_sz = fields.get("fill_size")
        if fill_px is None or fill_sz is None:
            log_error(f"fill notification missing fill_price/fill_size: {fields}")
            return
        fill_id = trade_id_from_fill_fields(fields, ts_event)
        commission = Money(Decimal("0"), Currency.from_str("USD"))
        emit.generate_order_filled(
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
