"""Working-orders drain: interpret / recon reports / soft mass-status helpers.

Owns the single raw-row pipeline, status/fill report-list builders, and empty-
drain / soft-mass honesty rules. ``RithmicExecutionClient`` keeps thin private
method delegates that tests bind and owns Nautilus ``generate_*`` overrides.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import (
    FillReport,
    OrderStatusReport,
    PositionStatusReport,
)
from nautilus_trader.model.enums import OrderSide, OrderStatus
from nautilus_trader.model.identifiers import ClientOrderId, VenueOrderId
from nautilus_trader.model.objects import Price, Quantity

from rithmic_nt_connect._convert import format_price_str
from rithmic_nt_connect._orders import (
    TRUSTWORTHY_DURATIONS as _TRUSTWORTHY_DURATIONS,
)
from rithmic_nt_connect._orders import (
    TRUSTWORTHY_PRICE_TYPES as _TRUSTWORTHY_PRICE_TYPES,
)
from rithmic_nt_connect._orders import (
    enum_int,
    fill_dedup_key,
    order_notification_to_fields,
    order_side_from_notification,
    slim_order_fields,
)
from rithmic_nt_connect.errors import VenueQueryUnavailable

# Canonical notification kinds that describe a real order state (see
# ``kind_from_notify`` in ``_orders.py``) — the strict trust boundary for
# binding a venue id from a drain row.
_RECOGNIZABLE_KINDS = frozenset(
    {
        "accepted",
        "updated",
        "canceled",
        "filled",
        "rejected",
        "modify_rejected",
        "cancel_rejected",
        "expired",
        "triggered",
    }
)

# Status substrings that mark a drain row as a real venue order state.
# "TRIGGER" covers resting stop rows ("TRIGGER_PENDING" / "trigger pending"):
# live-proven on Rithmic Test 2026-08-21 — stops never emit an OPEN
# notification, so this is the only state their drain rows ever carry.
_STATUS_MARKERS = ("OPEN", "WORKING", "CANCEL", "REJECT", "EXPIRED", "TRIGGER")

TERMINAL_ORDER_STATUSES = frozenset(
    {
        OrderStatus.FILLED,
        OrderStatus.CANCELED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    }
)


def row_is_trustworthy(fields: dict[str, Any]) -> bool:
    """A drain row binds a venue id only when its closed-set execution terms
    are real: side present, ``price_type``/``duration`` present and mappable,
    and a recognizable order state. Never fabricates terms (a row with
    missing/unknown closed-set values is advisory-only).
    """
    if order_side_from_notification(fields) is None:
        return False
    # The closed-set values are exact integers or None (``enum_int`` at the
    # convert boundary; the same whitelist defends raw fields here too, so a
    # bool/non-integral value can never coerce into a valid enum).
    price_type = enum_int(fields.get("price_type"))
    duration = enum_int(fields.get("duration"))
    if (
        price_type is None
        or duration is None
        or price_type not in _TRUSTWORTHY_PRICE_TYPES
        or duration not in _TRUSTWORTHY_DURATIONS
    ):
        return False
    kind = fields.get("kind")
    status_u = str(fields.get("status") or "").upper()
    return kind in _RECOGNIZABLE_KINDS or any(
        marker in status_u for marker in _STATUS_MARKERS
    )


def _price(value: float | Decimal | str, precision: int | None = None) -> Price:
    if precision is None:
        return Price.from_str(format_price_str(value))
    return Price.from_str(f"{float(value):.{int(precision)}f}")


def apply_mass_status_report_window(
    mass_status: Any,
    *,
    lookback_start_ns: int | None,
    reports_complete: bool,
) -> bool:
    """Declare NT mass-status history bound when the installed API allows.

    NT 1.231 ``ExecutionMassStatus`` has neither ``lookback_start`` nor
    ``set_report_window`` (those land on master / 2.0.x; Python bindings may
    still omit the setter). Returns whether the contract was applied.
    """
    setter = getattr(mass_status, "set_report_window", None)
    if callable(setter):
        setter(lookback_start_ns, reports_complete)
        return True
    applied = False
    for name, value in (
        ("lookback_start", lookback_start_ns),
        ("reports_complete", reports_complete),
    ):
        if not hasattr(mass_status, name):
            continue
        try:
            setattr(mass_status, name, value)
            applied = True
        except (AttributeError, TypeError):
            continue
    return applied


class DrainRowResult:
    """Tagged result of the drain-row interpretation boundary.

    One boundary decides, for any working-orders drain row, what to publish
    (``report`` — the advisory ``OrderStatusReport``) and whether the row is
    trustworthy enough to bind a venue id from it (``bindable``, strict
    closed-set terms — never fabricated). ``fields``/``ts_event`` let callers
    re-read the winning row (freshness check, venue-id bind). No caller
    re-implements the interpretation.
    """

    __slots__ = ("bindable", "fields", "report", "ts_event")

    def __init__(
        self,
        fields: dict[str, Any],
        ts_event: int,
        report: OrderStatusReport | None,
        bindable: bool,
    ) -> None:
        self.fields = fields
        self.ts_event = ts_event
        self.report = report
        self.bindable = bindable


class WorkingOrdersDrain:
    """Interpret and apply working-orders drain rows.

    Holds the host client by reference (cache/clock/publish/bind stay on the
    client). Does not wrap Nautilus cache/clock/msgbus/session.
    """

    def __init__(self, host: Any) -> None:
        self._host = host

    def order_status_report_from_fields(
        self,
        fields: dict[str, Any],
        ts_event: int,
    ) -> OrderStatusReport | None:
        """Build an advisory ``OrderStatusReport`` from normalized wire fields.

        Deliberately permissive: unknown closed-set fields fall back to
        ``BUY``/``MARKET``/``GTC``/``ACCEPTED`` so one malformed row cannot
        abort the whole recon. Whether a row is trustworthy enough to bind a
        venue id is decided separately by ``row_is_trustworthy`` (the drain
        boundary), not here.
        """
        host = self._host
        basket = fields.get("basket_id")
        instrument_id = host._instrument_id_from_order_fields(fields)
        if not basket or instrument_id is None:
            return None
        account_raw = fields.get("account_id")
        if account_raw:
            host._seed_account_if_needed(str(account_raw))
        if host.account_id is None:
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
            order_type = host._order_type_from_event(fields)
            tif = host._tif_from_event(fields)
            status = host._order_status_from_event(fields)
            # Event-time fallback, owned HERE: the ordering ``ts_event`` is the
            # iterator's 0-default when the venue sent no timestamp — a report
            # published with epoch 0 could be treated as stale (e.g. a fill's
            # order prerequisite). One policy for every report consumer.
            report_ts = ts_event or host._clock.timestamp_ns()
            return OrderStatusReport(
                account_id=host.account_id,
                instrument_id=instrument_id,
                venue_order_id=VenueOrderId(str(basket)),
                order_side=side or OrderSide.BUY,
                order_type=order_type,
                time_in_force=tif,
                order_status=status,
                quantity=qty,
                filled_qty=filled,
                report_id=UUID4(),
                ts_accepted=report_ts,
                ts_last=report_ts,
                ts_init=host._clock.timestamp_ns(),
                client_order_id=host._client_order_id_for_tag(fields.get("user_tag")),
                price=price,
                trigger_price=trigger,
                trigger_type=host._trigger_type_from_event(fields),
                avg_px=avg_px,
            )
        except (TypeError, ValueError, OverflowError):
            # Skip a malformed row rather than abort the whole recon response.
            return None

    def drain_row_from_fields(
        self, fields: dict[str, Any], ts_event: int
    ) -> DrainRowResult:
        """One interpretation boundary for a working-orders drain row.

        Builds a single advisory ``OrderStatusReport`` (permissive: unknown
        closed-set terms fall back to ``BUY``/``MARKET``/``GTC``/``ACCEPTED``
        so one malformed row cannot abort a whole recon) and decides
        ``bindable`` separately — a row binds a venue id only when its
        closed-set execution terms are real (``row_is_trustworthy``). Every
        drain/recon caller consumes this; no caller re-implements "usable".
        """
        # Call the builder on ``self`` (not ``host._order_status_report_from_fields``)
        # to avoid thin-delegate recursion once the host method points here.
        report = self.order_status_report_from_fields(fields, ts_event)
        bindable = report is not None and row_is_trustworthy(fields)
        return DrainRowResult(fields, ts_event, report, bindable)

    def iter_drain_rows(self, events: list[dict[str, Any]]) -> Iterator[DrainRowResult]:
        """Yield one interpretation per usable drain row; skip the malformed.

        The raw-row pipeline — normalize, require a basket, coerce
        ``ts_event`` — lives HERE with every guard inside, so no drain caller
        re-implements "normalize then guard then interpret". A row that cannot
        build an advisory report (``report is None``) is skipped too.
        """
        host = self._host
        for raw in events:
            try:
                fields = order_notification_to_fields(raw)
            except Exception:
                continue
            if not fields.get("basket_id"):
                continue
            try:
                ts_event = int(fields.get("ts_event") or 0)
            except (TypeError, ValueError, OverflowError):
                continue
            # Call through host so ``_drain_row_from_fields`` overrides apply.
            row = host._drain_row_from_fields(fields, ts_event)
            if row.report is None:
                continue
            yield row

    def latest_drain_rows(
        self,
        events: list[dict[str, Any]],
        instrument_id: Any = None,
    ) -> dict[str, DrainRowResult]:
        """Keep only the freshest drain row per basket id (optionally
        filtered to one instrument — ``None`` matches every instrument)."""
        host = self._host
        latest: dict[str, DrainRowResult] = {}
        for row in self.iter_drain_rows(events):
            if not host._matches_instrument(row.fields, instrument_id, None):
                continue
            key = str(row.fields["basket_id"])
            # Keep the latest row; on an equal timestamp (e.g. both 0 when
            # ts_event is missing) prefer the last-arrived row so a terminal
            # status following an earlier non-terminal is not masked.
            if key not in latest or row.ts_event >= latest[key].ts_event:
                latest[key] = row
        return latest

    def row_stale_reason(
        self,
        client_order_id: ClientOrderId | None,
        row: DrainRowResult,
        *,
        live_stream_authoritative: bool,
    ) -> str | None:
        """Why a drain row must not advance local state (``None`` = forward).

        The two drain consumers run under different authority models and the
        difference is deliberate:

        - Re-arm barrier (``live_stream_authoritative=True``): the live
          stream owns tracked-order state, so ANY row for an order it
          already closed is stale — terminal included — as is any row older
          than the order's last local event.
        - Bulk status recon (``False``): the drain is an advisory snapshot;
          arrival order is not causal. A non-terminal row for a locally
          closed order is venue lag and would reconcile ACCEPTED over
          CANCELED/FILLED (the unguarded ``InvalidStateTrigger: CANCELED ->
          ACCEPTED``, MY043-001 2026-08-21), so only that class is
          suppressed. Terminal-vs-terminal still forwards: venue FILLED vs
          local CANCELED is a real fill-after-cancel race the engine must
          see.
        """
        if client_order_id is None:
            return None
        host = self._host
        order = host._cache.order(client_order_id)
        if order is None:
            return None
        if getattr(order, "is_closed", False):
            if live_stream_authoritative:
                return "order closed locally"
            report = row.report
            if report is None or report.order_status in TERMINAL_ORDER_STATUSES:
                return None
            return "non-terminal snapshot for locally closed order"
        if (
            live_stream_authoritative
            and row.ts_event
            and int(getattr(order, "ts_last", 0) or 0) > row.ts_event
        ):
            # ``row.ts_event == 0`` is the iterator's synthetic missing-ts
            # value: skipping on it would drop a valid snapshot whenever the
            # tracked order has any live history.
            return "live stream advanced past the snapshot"
        return None

    def apply_drain_rows(self, events: list[dict[str, Any]]) -> None:
        """Apply a working-orders drain to the local cache before re-arming.

        The drain is a snapshot, not a replay: bind the venue id for tracked
        in-flight orders (so commands target the real venue order and later
        notifications attach), and publish reconciliation status reports for
        the rows (terminal outcomes the live stream missed while
        disconnected). Typed live events are NOT re-emitted — that is the live
        stream's job and would double-emit for rows already seen live.
        Publication happens BEFORE the venue id is bound and a failed
        publication raises: the engine must receive the reconciled status
        before trading resumes, so the barrier aborts (the plant stays
        un-armed).
        """
        host = self._host
        # Call host wrappers when present so MethodType spies apply; wrappers
        # only bounce into this drain (no recursion into apply).
        latest = getattr(host, "_latest_drain_rows", None)
        rows = (
            latest(events).values()
            if latest is not None
            else self.latest_drain_rows(events).values()
        )
        for row in rows:
            fields = row.fields
            basket = str(fields["basket_id"])
            report = row.report
            if report is None:
                # Unreachable (the iterator skips unusable rows); narrows the
                # type for the checker.
                continue
            client_order_id = host._drain_client_order_id(fields)
            if client_order_id is not None:
                stale = getattr(host, "_row_stale_reason", None)
                reason = (
                    stale(client_order_id, row, live_stream_authoritative=True)
                    if stale is not None
                    else self.row_stale_reason(
                        client_order_id, row, live_stream_authoritative=True
                    )
                )
                if reason is not None:
                    continue
            # Apply (publish) BEFORE binding the venue id — commit ordering:
            # the engine must receive the reconciled status before trading
            # resumes, so a failed publication fails the barrier (raises) and
            # the plant stays un-armed; the venue id is bound only afterwards.
            if not host._publish_order_status_report(
                report,
                context="reconnect re-arm drain",
            ):
                raise VenueQueryUnavailable(
                    "reconnect re-arm drain aborted: a reconciliation status "
                    "report failed to publish"
                )
            if (
                client_order_id is not None
                and row.bindable
                and host._cache.order(client_order_id) is not None
                and host._cache.venue_order_id(client_order_id) is None
            ):
                host._bind_venue_id(client_order_id, basket)

    async def load_orders_events(
        self, start_sec: int, end_sec: int
    ) -> list[dict[str, Any]]:
        # The gateway performs a bounded silence-window drain of the current
        # working orders (`show_orders`). An empty result means "no working
        # orders after the drain" and is a valid best-effort answer, not an
        # error. One bounded attempt per barrier/query: a definitive
        # unavailable result fails immediately, and any other failure is
        # surfaced as unavailable — the next engine query or reconnect is the
        # retry boundary (no hidden retry policy inside recovery paths).
        host = self._host
        try:
            return await asyncio.to_thread(
                host._session.load_orders, start_sec, end_sec
            )
        except Exception as exc:
            if host._is_recon_unavailable(exc):
                raise
            raise VenueQueryUnavailable(
                f"load_orders recon failed ({start_sec}..{end_sec}): {exc}"
            ) from exc

    def order_status_reports_from_events(
        self,
        events: list[dict[str, Any]],
        *,
        instrument_id: Any = None,
        open_only: bool = False,
    ) -> list[OrderStatusReport]:
        """Empty-drain honesty + latest-row status report list (R10)."""
        host = self._host
        if not events:
            # Best-effort drain is not a snapshot: empty does not prove venue
            # has no working orders (no end-of-list, 10k cap, quiet channel).
            #
            # Continuous open-check uses ``open_only=True``. With operator
            # ``open_check_open_only=True`` (required for Rithmic), an empty
            # report list is advisory — NT will not cancel tracked opens. Raise
            # would only spam ExecEngine ERROR every open_check interval when
            # the book is flat. Return [] for open_only.
            #
            # Full recon (``open_only=False``, startup mass-status) still raises
            # so soft-complete can continue without treating empty as history.
            if open_only:
                host._log.debug(
                    "order status open_only drain empty — returning [] "
                    "(not a complete venue snapshot; keep open_check_open_only=True)"
                )
                return []
            raise VenueQueryUnavailable(
                "Rithmic order recon unavailable: best-effort drain returned "
                "no working orders (empty does not prove venue empty; no "
                "provably complete snapshot API)"
            )
        reports: list[OrderStatusReport] = []
        for row in self.latest_drain_rows(events, instrument_id=instrument_id).values():
            report = row.report
            if report is None:
                # Unreachable (the iterator skips unusable rows); narrows the
                # type for the checker.
                continue
            reason = host._row_stale_reason(
                host._drain_client_order_id(row.fields),
                row,
                live_stream_authoritative=False,
            )
            if reason is not None:
                host._log.debug(
                    f"suppressing stale drain row ({reason}): "
                    f"{slim_order_fields(row.fields)}"
                )
                continue
            reports.append(report)
        if open_only:
            reports = [
                r for r in reports if r.order_status not in TERMINAL_ORDER_STATUSES
            ]
        return reports

    async def generate_order_status_reports(
        self, command: Any
    ) -> list[OrderStatusReport]:
        """Venue order-status recon body (caller owns enable_trading gate)."""
        host = self._host
        start_sec, end_sec = host._recon_window_sec(command.start, command.end)
        # Use self to avoid recursion through ``host._load_orders_events``.
        events = await self.load_orders_events(start_sec, end_sec)
        return self.order_status_reports_from_events(
            events,
            instrument_id=command.instrument_id,
            open_only=bool(getattr(command, "open_only", False)),
        )

    async def generate_fill_reports(self, command: Any) -> list[FillReport]:
        """Venue fill recon body (caller owns enable_trading gate)."""
        host = self._host
        start_sec, end_sec = host._recon_window_sec(command.start, command.end)
        events = await self.load_orders_events(start_sec, end_sec)
        reports: list[FillReport] = []
        for row in self.iter_drain_rows(events):
            fields = row.fields
            if fields.get("kind") != "filled":
                continue
            if not host._matches_instrument(
                fields, command.instrument_id, command.venue_order_id
            ):
                continue
            # Identity uses the RAW row ts (0 when the venue sent none): the
            # fill's TradeId and dedup key must be stable across recon runs /
            # restarts, so the clock fallback must NOT enter them. The clock
            # fallback applies only to the report/status timestamps below.
            raw_ts = row.ts_event
            event_ts = raw_ts or host._clock.timestamp_ns()
            # Share the adapter-wide fill dedup store (live path + recon) so a
            # fill already emitted live, or duplicated across the summary/today
            # drains, is not re-emitted as a second reconciliation fill.
            dedup = fill_dedup_key(fields, ts_event=raw_ts)
            if host._fill_key_seen(dedup):
                continue
            # The builder applies the clock fallback to the report timestamp
            # itself; identity (TradeId) stays on the raw ts.
            report = host._fill_report_from_fields(fields, raw_ts)
            if report is not None:
                # Nautilus cannot reconcile a fill without an order prerequisite.
                # Reconciliation can discover a fill after the live order event
                # was missed, so publish a venue status first; this may create a
                # synthetic external order when no cached strategy order exists.
                # Rebuild the status with the corrected ``event_ts`` (the
                # iterator's 0-default would publish an epoch-dated
                # prerequisite for a row without a venue timestamp — the
                # status must share the fill's clock-fallback timestamp).
                status = host._drain_row_from_fields(fields, event_ts).report
                if status is None:
                    # Unreachable (the iterator already built a report for this
                    # row); narrows the type for the checker.
                    continue
                if not host._publish_order_status_report(
                    status,
                    context="fill reconciliation prerequisite",
                ):
                    continue
                reports.append(report)
                host._mark_fill_key(dedup)
        return reports

    def augment_soft_mass_flat_for_cache_opens(
        self,
        venue_reports: list[PositionStatusReport],
        *,
        ts_init: int,
    ) -> list[PositionStatusReport]:
        """Add FLAT reports for cache-open instruments the venue did not list."""
        host = self._host
        if host.account_id is None:
            return venue_reports
        reports = list(venue_reports)
        covered = {report.instrument_id for report in reports}
        try:
            opens = host._cache.positions_open(
                venue=host.venue, account_id=host.account_id
            )
        except TypeError:
            opens = host._cache.positions_open(venue=host.venue)
        for position in opens:
            if position.instrument_id in covered:
                continue
            instrument = host._cache.instrument(position.instrument_id)
            if instrument is None:
                continue
            reports.append(
                PositionStatusReport.create_flat(
                    account_id=host.account_id,
                    instrument_id=position.instrument_id,
                    size_precision=instrument.size_precision,
                    ts_init=ts_init,
                )
            )
        return reports

    def warn_soft_mass_cache_vs_venue(
        self, positions: list[PositionStatusReport]
    ) -> None:
        """Warn: NT will not clear Redis ghosts if generate_missing_orders=False."""
        host = self._host
        for report in positions:
            try:
                opens = host._cache.positions_open(
                    venue=None,
                    instrument_id=report.instrument_id,
                    account_id=report.account_id,
                )
            except TypeError:
                opens = [
                    p
                    for p in host._cache.positions_open(venue=host.venue)
                    if p.instrument_id == report.instrument_id
                ]
            cached = sum((p.signed_decimal_qty() for p in opens), Decimal(0))
            venue_qty = report.signed_decimal_qty
            if cached == venue_qty:
                continue
            host._log.warning(
                "soft mass-status: cache open qty "
                f"{cached} != venue {report.instrument_id} qty {venue_qty}; "
                "NT 1.231 with generate_missing_orders=False will not invent "
                "closing fills to clear Redis/OMS ghosts. Flush Redis (or start "
                "cold) when plant is flat before enabling live recon."
            )


def drain_for(host: Any) -> WorkingOrdersDrain:
    """Return the host's working-orders drain, creating one lazily."""
    drain = getattr(host, "_working_orders_drain_inst", None)
    if drain is None:
        drain = WorkingOrdersDrain(host)
        host._working_orders_drain_inst = drain
    return drain
