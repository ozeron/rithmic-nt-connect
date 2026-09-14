"""Venue notification intake: tracked events vs untracked reports.

Owns routing and fill/status publish for live order-plant notifications.
``RithmicExecutionClient`` keeps thin private-method delegates that tests bind.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.model.enums import LiquiditySide, OrderStatus, OrderType
from nautilus_trader.model.identifiers import ClientOrderId, TradeId, VenueOrderId
from nautilus_trader.model.objects import Currency, Price, Quantity

from rithmic_nt_connect._convert import format_price_str
from rithmic_nt_connect._orders import (
    fill_dedup_key,
    is_benign_bare_complete,
    notification_action,
    slim_order_fields,
)

# Order types that support the TRIGGERED order status (Nautilus #3812).
# Market-style stops execute immediately on trigger and have no intermediate
# TRIGGERED state.
TRIGGERABLE_ORDER_TYPES = frozenset(
    {
        OrderType.STOP_LIMIT,
        OrderType.TRAILING_STOP_LIMIT,
        OrderType.LIMIT_IF_TOUCHED,
    }
)


def _price(value: float | Decimal | str, precision: int | None = None) -> Price:
    if precision is None:
        return Price.from_str(format_price_str(value))
    return Price.from_str(f"{float(value):.{int(precision)}f}")


class VenueNotificationIntake:
    """Route venue order notifications to tracked events or untracked reports.

    Holds the host client by reference (cache/clock/stores/emit stay on the
    client). Does not wrap Nautilus cache/clock/msgbus/session.
    """

    __slots__ = ("_host",)

    def __init__(self, host: Any) -> None:
        self._host = host

    def handle_order_notification(self, fields: dict[str, Any]) -> None:
        host = self._host
        account_hint = fields.get("account_id")
        if account_hint:
            host._seed_account_if_needed(str(account_hint))
        client_order_id = host._resolve_client_order_id(fields)
        if client_order_id is None:
            # Prefer host thin delegate so MethodType spies apply; fall back for
            # direct intake unit stubs that lack the client wrappers.
            untracked = getattr(host, "_handle_untracked_notification", None)
            if untracked is not None:
                untracked(fields)
            else:
                self.handle_untracked_notification(fields)
            return
        order = host._cache.order(client_order_id)
        if order is None:
            host._log.warning(
                f"cached order missing for tracked {client_order_id}; "
                f"notification suppressed: {slim_order_fields(fields)}"
            )
            return
        ts_event = fields.get("ts_event")
        ts_event = int(ts_event) if ts_event is not None else host._clock.timestamp_ns()
        basket = fields.get("basket_id")
        if basket:
            host._bind_venue_id(client_order_id, str(basket))
        venue_order_id = VenueOrderId(host._venue_id_for(fields, client_order_id))
        action = notification_action(fields, order)
        if action is None:
            return
        strategy_id = order.strategy_id
        instrument_id = order.instrument_id
        if action.kind == "accepted":
            emit = getattr(host, "_emit_accepted", None)
            if emit is not None:
                emit(order, client_order_id, venue_order_id, ts_event)
            else:
                self.emit_accepted(order, client_order_id, venue_order_id, ts_event)
        elif action.kind == "rejected":
            host.generate_order_rejected(
                strategy_id,
                instrument_id,
                client_order_id,
                str(action.reason),
                ts_event,
            )
        elif action.kind == "modify_rejected":
            host.generate_order_modify_rejected(
                strategy_id,
                instrument_id,
                client_order_id,
                venue_order_id,
                str(action.reason),
                ts_event,
            )
        elif action.kind == "cancel_rejected":
            host.generate_order_cancel_rejected(
                strategy_id,
                instrument_id,
                client_order_id,
                venue_order_id,
                str(action.reason),
                ts_event,
            )
        elif action.kind == "updated":
            resolve = getattr(host, "_resolve_updated_terms", None)
            if resolve is not None:
                qty, price, trigger = resolve(order, action)
            else:
                qty, price, trigger = self.resolve_updated_terms(order, action)
            host.generate_order_updated(
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
            host.generate_order_canceled(
                strategy_id, instrument_id, client_order_id, venue_order_id, ts_event
            )
        elif action.kind == "triggered":
            emit_trig = getattr(host, "_emit_triggered_guarded", None)
            if emit_trig is not None:
                emit_trig(order, client_order_id, venue_order_id, ts_event)
            else:
                self.emit_triggered_guarded(
                    order, client_order_id, venue_order_id, ts_event
                )
        elif action.kind == "filled":
            tracked_fill = getattr(host, "_handle_tracked_fill", None)
            if tracked_fill is not None:
                tracked_fill(
                    order,
                    client_order_id,
                    venue_order_id,
                    fields,
                    ts_event,
                    action,
                )
            else:
                self.handle_tracked_fill(
                    order,
                    client_order_id,
                    venue_order_id,
                    fields,
                    ts_event,
                    action,
                )

    def emit_accepted(
        self,
        order: Any,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        ts_event: int,
    ) -> None:
        """Emit OrderAccepted under the LAP-42 guard."""
        host = self._host
        if order.status is OrderStatus.SUBMITTED:
            host.generate_order_accepted(
                order.strategy_id,
                order.instrument_id,
                client_order_id,
                venue_order_id,
                ts_event,
            )
        else:
            host._log.debug(
                f"skipping late/duplicate OrderAccepted for {client_order_id}: "
                f"local status={order.status}"
            )

    def resolve_updated_terms(
        self, order: Any, action: Any
    ) -> tuple[Quantity, Any, Any]:
        """Resolve UPDATED-branch qty/price/trigger."""
        qty = (
            Quantity.from_int(int(action.quantity))
            if action.quantity is not None
            else order.quantity
        )
        prec = int(order.price.precision) if order.has_price else None
        if action.price is not None:
            price = _price(action.price, prec)
        elif order.has_price:
            price = order.price
        else:
            price = None
        if action.trigger is not None:
            trigger = _price(action.trigger, prec)
        elif order.has_trigger_price:
            trigger = order.trigger_price
        else:
            trigger = None
        return qty, price, trigger

    def emit_triggered_guarded(
        self,
        order: Any,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        ts_event: int,
    ) -> None:
        """Emit OrderTriggered under the #3812 producer guard."""
        host = self._host
        already_triggered = getattr(order, "status", None) is OrderStatus.TRIGGERED
        if order.order_type not in TRIGGERABLE_ORDER_TYPES or already_triggered:
            host._log.debug(
                f"skipping OrderTriggered for {order.order_type} order "
                f"{client_order_id} (market-style stop or already triggered)"
            )
            return
        host.generate_order_triggered(
            order.strategy_id,
            order.instrument_id,
            client_order_id,
            venue_order_id,
            ts_event,
        )

    def handle_tracked_fill(
        self,
        order: Any,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        fields: dict[str, Any],
        ts_event: int,
        action: Any,
    ) -> None:
        """Emit one venue-priced tracked fill; dedup by venue trade id."""
        host = self._host
        strategy_id = order.strategy_id
        instrument_id = order.instrument_id
        if action.fill_qty is None or action.trade_id is None:
            host._log.error(f"fill action missing fields: {slim_order_fields(fields)}")
            return
        dedup = fill_dedup_key(fields, ts_event=ts_event)
        if host._fill_key_seen(dedup):
            return
        try:
            fill_qty = Quantity.from_int(int(action.fill_qty))
            if action.fill_px is None:
                raise ValueError("fill price missing (pending/sentinel)")
            fill_px = host._price_for_instrument(instrument_id, action.fill_px)
        except (TypeError, ValueError, OverflowError) as exc:
            host._latch_order_plant(
                "fill suppressed",
                f"tracked {client_order_id} fill unpriceable ({exc}); "
                "exposure may be incomplete; recon will re-sync",
            )
            return
        leaves = order.leaves_qty
        if leaves is not None and fill_qty > leaves:
            host._latch_order_plant(
                "overfill",
                f"tracked {client_order_id} fill qty {fill_qty} exceeds "
                f"leaves {leaves} by {fill_qty - leaves}; Nautilus clamps "
                "leaves_qty and tracks overfill_qty; recon will re-sync",
            )
        commission = host._commission_money(
            host._product_code_for_fill(instrument_id, fields.get("symbol")),
            int(fill_qty),
        )
        host.generate_order_filled(
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
        host._mark_fill_key(dedup)

    def publish_untracked_status(self, fields: dict[str, Any], ts_event: int) -> bool:
        """Status phase of the untracked path. ``False`` suppresses the fill."""
        host = self._host
        # Call through host so ``_drain_row_from_fields`` overrides apply.
        status_report = host._drain_row_from_fields(fields, ts_event).report
        if status_report is None:
            cid = host._basket_client_id(fields)
            order = host._cache.order(cid) if cid is not None else None
            if is_benign_bare_complete(fields, order):
                host._log.debug(
                    f"skipping benign bare COMPLETE for closed {cid}: "
                    f"{slim_order_fields(fields)}"
                )
            else:
                host._log.warning(
                    f"untracked order status could not be built: "
                    f"{slim_order_fields(fields)}"
                )
            return True

        status_key = (
            str(status_report.venue_order_id),
            str(getattr(status_report, "order_status", "")),
            str(getattr(status_report, "quantity", "")),
            str(getattr(status_report, "price", "")),
            str(getattr(status_report, "trigger_price", "")),
            str(getattr(status_report, "filled_qty", "")),
            str(getattr(status_report, "avg_px", "")),
        )
        venue_key = str(status_report.venue_order_id)
        if host._untracked_status_keys.get(venue_key) == status_key:
            return True
        # Call through host so ``_publish_order_status_report`` overrides apply.
        if not host._publish_order_status_report(
            status_report,
            context="untracked notification",
        ):
            return False
        host._untracked_status_keys.record(venue_key, status_key)
        return True

    def publish_untracked_fill(self, fields: dict[str, Any], ts_event: int) -> None:
        """Fill phase of the untracked path."""
        host = self._host
        if fields.get("kind") != "filled":
            return
        dedup = fill_dedup_key(fields, ts_event=ts_event)
        if host._fill_key_seen(dedup):
            return
        report = host._fill_report_from_fields(fields, ts_event)
        if report is None:
            host._log.error(
                f"untracked fill suppressed (build failed): {slim_order_fields(fields)}"
            )
            return
        host._send_fill_report(report)
        host._mark_fill_key(dedup)

    def handle_untracked_notification(self, fields: dict[str, Any]) -> None:
        """Report external venue activity without strategy ownership."""
        host = self._host
        ts_event = fields.get("ts_event")
        ts_event = int(ts_event) if ts_event is not None else host._clock.timestamp_ns()
        basket = fields.get("basket_id")
        symbol = fields.get("symbol")
        instrument_raw = fields.get("instrument_id")
        if not basket or not (instrument_raw or symbol):
            host._log.warning(
                f"untracked order notification missing identity: "
                f"{slim_order_fields(fields)}"
            )
            return
        account_raw = fields.get("account_id")
        if account_raw:
            host._seed_account_if_needed(str(account_raw))
        if host.account_id is None:
            host._log.warning(
                f"untracked order notification missing account: "
                f"{slim_order_fields(fields)}"
            )
            return
        # Prefer host thin delegates so MethodType spies apply; fall back for
        # direct intake unit stubs.
        publish_status = getattr(host, "_publish_untracked_status", None)
        publish_fill = getattr(host, "_publish_untracked_fill", None)
        if publish_status is not None:
            if not publish_status(fields, ts_event):
                return
        elif not self.publish_untracked_status(fields, ts_event):
            return
        if publish_fill is not None:
            publish_fill(fields, ts_event)
        else:
            self.publish_untracked_fill(fields, ts_event)

    def publish_order_status_report(
        self,
        report: OrderStatusReport,
        *,
        context: str,
    ) -> bool:
        """Publish a status report; ``False`` on failure (fail-closed)."""
        host = self._host
        try:
            host._send_order_status_report(report)
        except Exception as exc:
            host._log.exception(
                f"{context}: status report publication failed; skipping stale report",
                exc,
            )
            return False
        return True


def intake_for(host: Any) -> VenueNotificationIntake:
    """Return the host's intake, creating one lazily."""
    intake = getattr(host, "_notification_intake_inst", None)
    if intake is None:
        intake = VenueNotificationIntake(host)
        host._notification_intake_inst = intake
    return intake
