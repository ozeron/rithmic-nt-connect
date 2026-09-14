"""Working-orders drain: interpret / iterate / latest / stale / apply.

Owns the single raw-row pipeline for recon and reconnect re-arm.
``RithmicExecutionClient`` keeps thin private-method delegates that tests bind.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.identifiers import ClientOrderId

from rithmic_nt_connect._orders import (
    enum_int,
    order_notification_to_fields,
    order_side_from_notification,
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

# Exact Rithmic closed-set ints that may bind a venue id (same keys as the
# execution maps). Membership only — report mapping stays on the client.
_TRUSTWORTHY_PRICE_TYPES = frozenset({1, 2, 3, 4})
_TRUSTWORTHY_DURATIONS = frozenset({1, 2, 3, 4})

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

    __slots__ = ("_host",)

    def __init__(self, host: Any) -> None:
        self._host = host

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
        host = self._host
        report = host._order_status_report_from_fields(fields, ts_event)
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
            # Call through the host so spies / MethodType stubs on
            # ``_drain_row_from_fields`` remain effective (R12).
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
        for row in self.latest_drain_rows(events).values():
            fields = row.fields
            basket = str(fields["basket_id"])
            report = row.report
            if report is None:
                # Unreachable (the iterator skips unusable rows); narrows the
                # type for the checker.
                continue
            client_order_id = host._drain_client_order_id(fields)
            if (
                client_order_id is not None
                and self.row_stale_reason(
                    client_order_id, row, live_stream_authoritative=True
                )
                is not None
            ):
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


def drain_for(host: Any) -> WorkingOrdersDrain:
    """Return the host's working-orders drain, creating one lazily.

    Works for real clients and ``SimpleNamespace`` / MethodType stubs that
    only bind the thin private delegates (no class method lookup on ``self``).
    """
    drain = getattr(host, "_working_orders_drain_inst", None)
    if drain is None:
        drain = WorkingOrdersDrain(host)
        host._working_orders_drain_inst = drain
    return drain
