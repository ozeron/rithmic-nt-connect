"""Live market data client for Rithmic (Phase 1)."""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import time
from typing import Any

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.data.messages import (
    RequestBars,
    RequestTradeTicks,
    SubscribeBars,
    SubscribeOrderBook,
    SubscribeQuoteTicks,
    SubscribeTradeTicks,
    UnsubscribeBars,
    UnsubscribeOrderBook,
    UnsubscribeQuoteTicks,
    UnsubscribeTradeTicks,
)
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import (
    Bar,
    BarType,
    BookOrder,
    OrderBookDelta,
    OrderBookDeltas,
    QuoteTick,
    TradeTick,
)
from nautilus_trader.model.enums import (
    AggressorSide,
    BarAggregation,
    BookAction,
    OrderSide,
    RecordFlag,
)
from nautilus_trader.model.identifiers import ClientId, InstrumentId, TradeId, Venue
from nautilus_trader.model.objects import Price, Quantity

from rithmic_nt_connect._convert import (
    ConvertError,
    bbo_to_fields,
    format_price_str,
    last_trade_to_fields,
    order_book_to_fields,
    time_bar_to_fields,
)
from rithmic_nt_connect.config import RithmicDataClientConfig
from rithmic_nt_connect.constants import ADAPTER_NAME, VENUE
from rithmic_nt_connect.errors import CHANNEL_ERRORS
from rithmic_nt_connect.providers import RithmicInstrumentProvider
from rithmic_nt_connect.session import WireSession


def _reconnectable_poll_error(exc: BaseException) -> bool:
    if isinstance(exc, CHANNEL_ERRORS):
        return True
    text = str(exc).lower()
    return (
        "forced logout" in text
        or "connection closed" in text
        or "not connected" in text
        or "channel closed" in text
        or "channel lagged" in text
    )


_F_SNAPSHOT = int(RecordFlag.F_SNAPSHOT.value)
_F_LAST = int(RecordFlag.F_LAST.value)
_F_SNAPSHOT_LAST = _F_SNAPSHOT | _F_LAST


# Rithmic TimeBarType wire values.
_RITHMIC_SECOND = 1
_RITHMIC_MINUTE = 2
_RITHMIC_DAILY = 3
_RITHMIC_WEEKLY = 4


def _aggressor(value: Any) -> AggressorSide:
    if value in (1, "1", "BUY", "BUYER", "bid"):
        return AggressorSide.BUYER
    if value in (2, "2", "SELL", "SELLER", "ask"):
        return AggressorSide.SELLER
    if value is None:
        return AggressorSide.NO_AGGRESSOR
    raise ValueError(f"unknown aggressor value: {value!r}")


def _price(value: float, precision: int | None = None) -> Price:
    if precision is None:
        return Price.from_str(format_price_str(value))
    if precision < 0:
        raise ConvertError(f"price_precision must be >= 0, got {precision}")
    return Price.from_str(f"{float(value):.{int(precision)}f}")


def _validate_history_identity(
    payload: dict[str, Any],
    *,
    symbol: str,
    exchange: str,
    expected_rtype: int | None = None,
) -> None:
    """Reject a venue payload relabeled from a different symbol/exchange/bar type.

    The wire ``period`` field is deliberately not validated — its unit (native
    vs seconds) is not reliable. A mismatched symbol/exchange/rtype means the
    plant returned data for a different contract or timeframe; silently labeling
    it with the requested ``BarType`` would corrupt the tape.
    """
    raw_symbol = payload.get("symbol")
    if raw_symbol is not None and str(raw_symbol) != symbol:
        raise ConvertError(f"history symbol mismatch: {raw_symbol!r} != {symbol!r}")
    raw_exchange = payload.get("exchange")
    if raw_exchange is not None and str(raw_exchange) != exchange:
        raise ConvertError(
            f"history exchange mismatch: {raw_exchange!r} != {exchange!r}"
        )
    if expected_rtype is not None:
        raw_rtype = payload.get("bar_type")
        if raw_rtype is not None:
            # Reject anything that is not an exact integer representation:
            # ``int(2.5)`` truncates to 2 and bool is an int, so either would
            # let a mismatched timeframe masquerade as the requested rtype.
            if isinstance(raw_rtype, bool):
                rtype_i = -1
            elif isinstance(raw_rtype, int):
                rtype_i = raw_rtype
            elif isinstance(raw_rtype, str):
                try:
                    rtype_i = int(raw_rtype)
                except (TypeError, ValueError):
                    rtype_i = -1
            else:
                rtype_i = -1
            if rtype_i != expected_rtype:
                raise ConvertError(
                    f"history bar type mismatch: {raw_rtype!r} != {expected_rtype}"
                )


def payloads_to_trade_ticks(
    raw_ticks: list[dict[str, Any]],
    *,
    symbol: str,
    exchange: str,
    price_precision: int,
    ts_init: int,
) -> list[TradeTick]:
    """Convert venue history/live dicts to ``TradeTick`` (one convert boundary)."""
    ticks: list[TradeTick] = []
    for raw in raw_ticks:
        payload = dict(raw)
        _validate_history_identity(payload, symbol=symbol, exchange=exchange)
        if payload.get("symbol") is None:
            payload["symbol"] = symbol
        if payload.get("exchange") is None:
            payload["exchange"] = exchange
        fields = last_trade_to_fields(payload)
        ticks.append(
            fields_to_trade_tick(
                fields,
                ts_init=ts_init,
                price_precision=price_precision,
            )
        )
    ticks.sort(key=lambda tick: tick.ts_event)
    return ticks


def payloads_to_bars(
    raw_bars: list[dict[str, Any]],
    *,
    symbol: str,
    exchange: str,
    bar_type: BarType,
    price_precision: int,
    ts_init: int,
) -> list[Bar]:
    """Convert venue history/live dicts to ``Bar`` (one convert boundary)."""
    bars: list[Bar] = []
    expected_rtype = bar_type_to_rithmic(bar_type)[0]
    for raw in raw_bars:
        payload = dict(raw)
        _validate_history_identity(
            payload,
            symbol=symbol,
            exchange=exchange,
            expected_rtype=expected_rtype,
        )
        if payload.get("symbol") is None:
            payload["symbol"] = symbol
        if payload.get("exchange") is None:
            payload["exchange"] = exchange
        fields = time_bar_to_fields(payload)
        bars.append(
            fields_to_bar(
                fields,
                bar_type,
                ts_init,
                price_precision=price_precision,
            )
        )
    bars.sort(key=lambda bar: bar.ts_event)
    return bars


_TRADE_ID_SEQ = itertools.count(1)


def _trade_id(fields: dict[str, Any]) -> TradeId:
    """Synthesize a unique trade id per received message.

    Rithmic's LastTrade carries no venue trade id, and identical fields can be
    either a re-push or a genuine second print, so no field tuple can
    disambiguate them. A per-message sequence guarantees every received print
    has a distinct id (the engine's bar aggregator reads price/size only).
    """
    return TradeId(f"{int(fields['ts_event'])}-{next(_TRADE_ID_SEQ)}")


def fields_to_trade_tick(
    fields: dict[str, Any],
    ts_init: int,
    *,
    price_precision: int | None = None,
) -> TradeTick:
    size = int(fields["size"])
    if size < 1:
        raise ConvertError(f"trade size must be >= 1, got {size}")
    return TradeTick(
        InstrumentId.from_str(fields["instrument_id"]),
        _price(fields["price"], price_precision),
        Quantity.from_int(size),
        _aggressor(fields.get("aggressor")),
        _trade_id(fields),
        int(fields["ts_event"]),
        ts_init,
    )


def fields_to_quote_tick(
    fields: dict[str, Any],
    ts_init: int,
    *,
    price_precision: int | None = None,
) -> QuoteTick:
    bid_size = int(fields["bid_size"])
    ask_size = int(fields["ask_size"])
    if bid_size < 1 or ask_size < 1:
        raise ConvertError(
            f"quote sizes must be >= 1, got bid={bid_size} ask={ask_size}"
        )
    return QuoteTick(
        InstrumentId.from_str(fields["instrument_id"]),
        _price(fields["bid_price"], price_precision),
        _price(fields["ask_price"], price_precision),
        Quantity.from_int(bid_size),
        Quantity.from_int(ask_size),
        int(fields["ts_event"]),
        ts_init,
    )


def _book_delta(
    instrument_id: InstrumentId,
    action: BookAction,
    order: BookOrder | None,
    flags: int,
    ts_event: int,
    ts_init: int,
) -> OrderBookDelta:
    return OrderBookDelta(
        instrument_id,
        action,
        order,
        flags,
        0,
        ts_event,
        ts_init,
    )


def fields_to_order_book_deltas(
    fields: dict[str, Any],
    ts_init: int,
    *,
    price_precision: int | None = None,
) -> OrderBookDeltas:
    """Map a Rithmic order-book **summary** to one snapshot envelope.

    Last delta is always ``F_SNAPSHOT | F_LAST`` (including empty books).
    """
    instrument_id = InstrumentId.from_str(fields["instrument_id"])
    ts_event = int(fields["ts_event"])
    adds: list[BookOrder] = []
    for level in fields["levels"]:
        side = OrderSide.BUY if level["side"] == "BUY" else OrderSide.SELL
        size = int(level["size"])
        if size < 0:
            raise ConvertError(f"order book level size must be >= 0, got {size}")
        if size == 0:
            continue
        adds.append(
            BookOrder(
                side,
                _price(level["price"], price_precision),
                Quantity.from_int(size),
                int(level["order_id"]),
            )
        )
    if not adds:
        return OrderBookDeltas(
            instrument_id=instrument_id,
            deltas=[
                _book_delta(
                    instrument_id,
                    BookAction.CLEAR,
                    None,
                    _F_SNAPSHOT_LAST,
                    ts_event,
                    ts_init,
                )
            ],
        )
    deltas: list[OrderBookDelta] = [
        _book_delta(
            instrument_id,
            BookAction.CLEAR,
            None,
            _F_SNAPSHOT,
            ts_event,
            ts_init,
        )
    ]
    last_i = len(adds) - 1
    for i, order in enumerate(adds):
        flags = _F_SNAPSHOT_LAST if i == last_i else _F_SNAPSHOT
        deltas.append(
            _book_delta(
                instrument_id,
                BookAction.ADD,
                order,
                flags,
                ts_event,
                ts_init,
            )
        )
    return OrderBookDeltas(instrument_id=instrument_id, deltas=deltas)


def _is_duplicate_subscribe_error(exc: BaseException) -> bool:
    """True for venue ``[8] already exists`` (intent survived ticker reset)."""
    msg = str(exc).lower()
    return "[8]" in msg and "already exists" in msg


async def _subscribe_replay(call: Any, *args: Any) -> None:
    try:
        await asyncio.to_thread(call, *args)
    except Exception as exc:
        if _is_duplicate_subscribe_error(exc):
            return
        raise


async def replay_subscription_intent(
    session: WireSession,
    subscriptions: set[tuple[str, str]],
    book_subscriptions: set[tuple[str, str]],
    bar_subscriptions: set[tuple[str, str, int, int]] | None = None,
) -> None:
    """Replay ticker + book + EXTERNAL bar intent on an already-connected wire.

    Every path that re-establishes the wire must go through this single
    boundary so the client can never reconnect with live plants but zero
    subscriptions.
    """
    for symbol, exchange in subscriptions:
        await _subscribe_replay(session.subscribe, symbol, exchange)
    for symbol, exchange in book_subscriptions:
        await _subscribe_replay(session.subscribe_order_book_summary, symbol, exchange)
    for symbol, exchange, rtype, period in bar_subscriptions or ():
        await _subscribe_replay(
            session.subscribe_time_bars, symbol, exchange, rtype, period
        )


async def resync_ticker_session(
    session: WireSession,
    subscriptions: set[tuple[str, str]],
    book_subscriptions: set[tuple[str, str]],
    bar_subscriptions: set[tuple[str, str, int, int]] | None = None,
) -> None:
    """Reset the wire, then replay ticker + book + EXTERNAL bar intent.

    ``session.reset_ticker`` is refcount-blind: a channel-error resync must
    actually recreate the ticker plant even when the session is shared with
    the exec client, where ``disconnect`` alone is a holder-refcounted no-op.
    Direct mode recreates only the ticker plant (PnL/order untouched); the
    gateway detaches + re-dials this client only (parent plants untouched).
    """
    await asyncio.to_thread(session.reset_ticker)
    await replay_subscription_intent(
        session, subscriptions, book_subscriptions, bar_subscriptions
    )


def _bar_type_duration_ns(bar_type: BarType) -> int:
    """Duration in nanoseconds of a Nautilus ``BarType`` spec.

    Used to shift a venue bar CLOSE time back to the OPEN time for intraday
    bars. Derived from the authoritative ``BarType`` (aggregation + step) rather
    than the wire ``period`` field, whose unit (native vs seconds) is not
    reliable. Daily/weekly markers are calendar ``YYYYMMDD`` (not a close epoch),
    so their ``ts_event`` is left untouched.
    """
    aggregation = bar_type.spec.aggregation
    step = int(bar_type.spec.step)
    if aggregation == BarAggregation.SECOND:
        return step * 1_000_000_000
    if aggregation == BarAggregation.MINUTE:
        return step * 60 * 1_000_000_000
    if aggregation == BarAggregation.HOUR:
        return step * 60 * 60 * 1_000_000_000
    return 0  # DAY / WEEK / other: calendar marker, no close→open shift


def fields_to_bar(
    fields: dict[str, Any],
    bar_type: BarType,
    ts_init: int,
    *,
    price_precision: int | None = None,
) -> Bar:
    volume = int(fields["volume"])
    if volume < 0:
        raise ConvertError(f"bar volume must be >= 0, got {volume}")
    # Rithmic ts_event/marker is the bar CLOSE time; Nautilus Bar.ts_event is
    # the OPEN time. Shift intraday bars back by their duration so the tape
    # aligns with the lake / IBKR / Databento open-time convention.
    ts_event = int(fields["ts_event"]) - _bar_type_duration_ns(bar_type)
    return Bar(
        bar_type,
        _price(fields["open"], price_precision),
        _price(fields["high"], price_precision),
        _price(fields["low"], price_precision),
        _price(fields["close"], price_precision),
        Quantity.from_int(volume),
        ts_event,
        ts_init,
    )


def external_bar_advertised(bar_type: BarType) -> bool:
    """Live EXTERNAL subscribe is only advertised for these specs."""
    agg = bar_type.spec.aggregation
    step = int(bar_type.spec.step)
    return (
        (agg == BarAggregation.MINUTE and step in {1, 15, 60})
        or (agg == BarAggregation.HOUR and step == 1)
        or (agg == BarAggregation.DAY and step == 1)
    )


def bar_type_to_rithmic(bar_type: BarType) -> tuple[int, int]:
    """Map a Nautilus BarType to (rithmic_bar_type, period)."""
    aggregation = bar_type.spec.aggregation
    step = int(bar_type.spec.step)
    if step < 1:
        raise ValueError(f"bar step must be >= 1, got {step}")
    if aggregation == BarAggregation.SECOND:
        return _RITHMIC_SECOND, step
    if aggregation == BarAggregation.MINUTE:
        return _RITHMIC_MINUTE, step
    if aggregation == BarAggregation.HOUR:
        return _RITHMIC_MINUTE, step * 60
    if aggregation == BarAggregation.DAY:
        return _RITHMIC_DAILY, step
    if aggregation == BarAggregation.WEEK:
        return _RITHMIC_WEEKLY, step
    raise ValueError(
        f"unsupported bar aggregation {aggregation!r} for Rithmic history "
        "(supported: SECOND, MINUTE, HOUR→minute, DAY, WEEK)"
    )


def bar_wire_period_keys(bar_type: BarType) -> list[int]:
    """Wire ``period`` values under which the venue may echo this bar type.

    The event ``period`` unit is documented unreliable (native vs seconds) and
    the live venue echoes seconds (``"60"`` observed for a 1-MINUTE
    subscription) while the request uses the native unit (1). Registering both
    keeps the dispatch lookup (``bar_types_for_event``) working whichever unit
    the venue emits.
    """
    aggregation = bar_type.spec.aggregation
    step = int(bar_type.spec.step)
    _rtype, period = bar_type_to_rithmic(bar_type)
    seconds = {
        BarAggregation.SECOND: step,
        BarAggregation.MINUTE: step * 60,
        BarAggregation.HOUR: step * 3600,
        BarAggregation.DAY: step * 86400,
        BarAggregation.WEEK: step * 604800,
    }.get(aggregation, 0)
    periods = [period]
    if seconds and seconds != period:
        periods.append(seconds)
    return periods


def bar_types_for_event(bar_types: dict, event: dict[str, Any]) -> set[BarType]:
    """BarTypes registered for a wire ``time_bar`` event (venue echo shape).

    Keys are ``(symbol, exchange, rtype, period)``; the event carries the
    venue's period (seconds for minute/hour bars, observed ``"60"``), which
    matches the seconds-period echo keys registered by
    ``bar_wire_period_keys``.
    """
    symbol = str(event.get("symbol") or "")
    exchange = str(event.get("exchange") or "")
    rtype = int(event.get("bar_type") or 0)
    period_raw = event.get("period")
    try:
        period = int(period_raw) if period_raw not in (None, "") else 1
    except (TypeError, ValueError):
        period = 1
    return set(bar_types.get((symbol, exchange, rtype, period), ()))


def bar_resync_subscriptions(bar_types: dict) -> set[tuple[str, str, int, int]]:
    """Native wire requests to re-issue after a ticker reset, deduped.

    ``bar_types`` holds both native and seconds-period echo keys per BarType;
    re-issuing every key would double-subscribe. Derive one native request per
    registered BarType.
    """
    subs: set[tuple[str, str, int, int]] = set()
    for (symbol, exchange, _rtype, _period), mapped in bar_types.items():
        for bar_type in mapped:
            rtype, period = bar_type_to_rithmic(bar_type)
            subs.add((symbol, exchange, rtype, period))
    return subs


class RithmicDataClient(LiveMarketDataClient):
    """Out-of-tree live market-data client backed by the Rust Rithmic session.

    Nautilus 1.231.x models ``LiveMarketDataClient`` as a ``MarketDataClient``
    rather than a ``LiveDataClient``, yet ``LiveDataClientFactory.create``
    contracts ``-> LiveDataClient``. The 8862b62 attempt to satisfy that
    contract by taking both bases broke at runtime: the two base ``__init__``
    chains are not cooperative in this version (either MRO ordering lands on
    the other base's required ``loop``), so ``TradingNode.build`` raised
    ``TypeError: LiveDataClient.__init__() missing 'loop'``. The factory
    contract is satisfied by the vendored stub declaring
    ``LiveMarketDataClient(LiveDataClient)``; at runtime the node builder
    only registers the client (no ``isinstance`` check).
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: RithmicInstrumentProvider,
        config: RithmicDataClientConfig,
        session: WireSession,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or ADAPTER_NAME),
            venue=Venue(VENUE),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            config=None,
        )
        self._config_local = config
        self._session = session
        self._poll_task: asyncio.Task | None = None
        self._poll_closing = False
        self._subscriptions: set[tuple[str, str]] = set()
        self._ticker_intents: dict[tuple[str, str], set[str]] = {}
        self._book_subscriptions: set[tuple[str, str]] = set()
        # Wire key → one or more Nautilus BarTypes (60-MINUTE and 1-HOUR share
        # a key; each BarType is registered under BOTH the native period and
        # the venue's seconds-period echo key — see ``bar_wire_period_keys``).
        self._bar_types: dict[tuple[str, str, int, int], set[BarType]] = {}
        self._instrument_routes: dict[str, tuple[str, str]] = {}
        self._history_poll_task: asyncio.Task | None = None
        self._resync_lock = asyncio.Lock()
        self._resync_generation = 0
        # One-sided BestBidOffer merge state, keyed by symbol[:exchange].
        self._bbo_state: dict[str, dict[str, Any]] = {}
        # Rate-limited skip diagnostics (Rithmic push feed is presence-bit based).
        self._skip_counts: dict[str, int] = {}
        self._skip_last_flush = 0.0

    async def _connect(self) -> None:
        from rithmic_nt_connect.session import ensure_connected

        await asyncio.to_thread(ensure_connected, self._session)
        await self._instrument_provider.initialize()
        for instrument in self._instrument_provider.list_all():
            self._handle_data(instrument)
            info = instrument.info or {}
            try:
                exchange = str(info["rithmic_exchange"])
                symbol = str(info["rithmic_symbol"])
            except KeyError as exc:
                raise ValueError(
                    f"instrument {instrument.id} missing rithmic route fields in info"
                ) from exc
            self._instrument_routes[str(instrument.id)] = (symbol, exchange)
        # A (re)connect may have recreated the plants with zero subscriptions:
        # re-issue every remembered ticker/book/bar intent through the same
        # single replay boundary as the channel-error resync. Also drop the
        # one-sided BBO accumulators — merging pre-disconnect state with the
        # fresh post-reconnect stream would publish stale-mixed QuoteTicks —
        # and restart the history poll when EXTERNAL bars are registered (the
        # disconnect cancelled it and only _subscribe_bars re-creates it).
        ticker_keys = set(getattr(self, "_ticker_intents", {}).keys())
        if not ticker_keys:
            ticker_keys = set(getattr(self, "_subscriptions", set()))
        await replay_subscription_intent(
            self._session,
            ticker_keys,
            set(self._book_subscriptions),
            bar_resync_subscriptions(self._bar_types),
        )
        self._bbo_state.clear()
        if self._bar_types:
            self._ensure_history_poll_task()
        # Own the task: LiveMarketDataClient.create_task WARNs on our cancel.
        self._poll_closing = False
        self._poll_task = self._loop.create_task(self._poll_loop(), name="rithmic_poll")
        self._poll_task.add_done_callback(
            lambda task: self._on_poll_done(task, name="rithmic_poll")
        )

    def _ensure_history_poll_task(self) -> None:
        if self._history_poll_task is not None:
            return
        self._history_poll_task = self._loop.create_task(
            self._history_poll_loop(), name="rithmic_history_poll"
        )
        self._history_poll_task.add_done_callback(
            lambda task: self._on_poll_done(task, name="rithmic_history_poll")
        )

    def _on_poll_done(self, task: asyncio.Task, *, name: str) -> None:
        if self._poll_closing or task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._log.error(f"{name} died: {exc}")

    async def _disconnect(self) -> None:
        self._poll_closing = True
        for attr in ("_poll_task", "_history_poll_task"):
            task = getattr(self, attr)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                setattr(self, attr, None)
        await asyncio.to_thread(self._session.disconnect)

    async def _resync_ticker_subscription(self) -> None:
        start_gen = self._resync_generation
        async with self._resync_lock:
            if self._resync_generation != start_gen:
                return
            # Drop one-sided BBO accumulators: after a plant reset the venue
            # re-sends fresh bid/ask sides, and merging them with pre-reset
            # state would publish a QuoteTick mixing stale + fresh sides.
            self._bbo_state.clear()
            ticker_keys = set(getattr(self, "_ticker_intents", {}).keys())
            if not ticker_keys:
                ticker_keys = set(getattr(self, "_subscriptions", set()))
            await resync_ticker_session(
                self._session,
                ticker_keys,
                set(self._book_subscriptions),
                bar_resync_subscriptions(self._bar_types),
            )
            self._resync_generation += 1

    async def _poll_channel_loop(
        self,
        *,
        label: str,
        poll,
        idle_sleep: float,
        should_poll=None,
    ) -> None:
        backoff = 0.05
        while True:
            if should_poll is not None and not should_poll():
                await asyncio.sleep(idle_sleep)
                continue
            try:
                event = await asyncio.to_thread(poll)
            except Exception as exc:
                if not _reconnectable_poll_error(exc):
                    self._log.exception(f"{label} poll transient error", exc)
                    await asyncio.sleep(0.1)
                    continue
                self._log.warning(f"{label} poll reconnect: {exc}")
                try:
                    await self._resync_ticker_subscription()
                    self._log.warning(
                        f"{label} subscription resynced after channel error"
                    )
                    backoff = 0.05
                except Exception as resync_exc:
                    self._log.warning(
                        f"{label} subscription resync failed: {resync_exc}"
                    )
                    backoff = min(backoff * 2, 2.0)
                await asyncio.sleep(backoff)
                continue
            if event is None:
                await asyncio.sleep(idle_sleep)
                continue
            self._dispatch_event(event)

    async def _poll_loop(self) -> None:
        await self._poll_channel_loop(
            label="ticker",
            poll=self._session.poll_event,
            idle_sleep=0.01,
        )

    async def _history_poll_loop(self) -> None:
        await self._poll_channel_loop(
            label="history",
            poll=self._session.poll_history_event,
            idle_sleep=0.05,
            should_poll=lambda: bool(self._bar_types),
        )

    def _price_precision(self, instrument_id: InstrumentId) -> int:
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            raise ConvertError(f"instrument not in cache: {instrument_id}")
        return int(instrument.price_precision)

    _skip_log_interval_secs = 60.0

    def _count_skip(self, reason: str) -> None:
        """Rate-limit skip diagnostics to one aggregate line per minute.

        Rithmic's push feed is presence-bit based, so one-sided BBOs and
        stats-only LastTrade messages are expected; per-event DEBUG logging of
        those floods the log with thousands of lines per minute.
        """
        self._skip_counts[reason] = self._skip_counts.get(reason, 0) + 1
        now = time.monotonic()
        if now - self._skip_last_flush < self._skip_log_interval_secs:
            return
        if self._skip_counts:
            summary = ", ".join(
                f"{reason}={count}"
                for reason, count in sorted(self._skip_counts.items())
            )
            self._log.debug(
                f"data skip summary ({self._skip_log_interval_secs:.0f}s): {summary}"
            )
        self._skip_counts.clear()
        self._skip_last_flush = now

    def _dispatch_event(self, event: dict[str, Any]) -> None:
        etype = event.get("type")
        ts_init = self._clock.timestamp_ns()
        try:
            if etype == "last_trade":
                if event.get("trade_price") is None:
                    # Stats-only update (net_change/volume/vwap) — no trade print.
                    self._count_skip("last_trade_summary")
                    return
                fields = last_trade_to_fields(event)
                prec = self._price_precision(
                    InstrumentId.from_str(fields["instrument_id"])
                )
                self._handle_data(
                    fields_to_trade_tick(fields, ts_init, price_precision=prec)
                )
            elif etype == "bbo":
                symbol = str(event.get("symbol") or "")
                if not symbol:
                    raise ConvertError("missing required fields: symbol")
                key = f"{symbol}:{event.get('exchange') or ''}"
                state = self._bbo_state.setdefault(key, {})
                fields = bbo_to_fields(event, state)
                if fields is None:
                    self._count_skip("bbo_one_sided")
                    return
                prec = self._price_precision(
                    InstrumentId.from_str(fields["instrument_id"])
                )
                self._handle_data(
                    fields_to_quote_tick(fields, ts_init, price_precision=prec)
                )
            elif etype == "order_book":
                fields = order_book_to_fields(event)
                prec = self._price_precision(
                    InstrumentId.from_str(fields["instrument_id"])
                )
                self._handle_data(
                    fields_to_order_book_deltas(fields, ts_init, price_precision=prec)
                )
            elif etype == "time_bar":
                bar_types = self._bar_types_for_event(event)
                if not bar_types:
                    self._count_skip("time_bar_unsubscribed")
                    return
                fields = time_bar_to_fields(event)
                for bar_type in bar_types:
                    prec = self._price_precision(bar_type.instrument_id)
                    self._handle_data(
                        fields_to_bar(fields, bar_type, ts_init, price_precision=prec)
                    )
        except ConvertError as exc:
            self._count_skip(f"{etype}: {exc}")
        except Exception as exc:
            self._log.exception(f"failed to dispatch {etype}", exc)

    def _route(self, instrument_id: InstrumentId) -> tuple[str, str]:
        key = str(instrument_id)
        if key not in self._instrument_routes:
            raise ValueError(
                f"no Rithmic route for {instrument_id}; load instruments before "
                f"subscribe/request"
            )
        return self._instrument_routes[key]

    def _bar_types_for_event(self, event: dict[str, Any]) -> set[BarType]:
        return bar_types_for_event(self._bar_types, event)

    async def _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None:
        await self._add_ticker_intent(command.instrument_id, "trade")

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        await self._add_ticker_intent(command.instrument_id, "quote")

    async def _add_ticker_intent(self, instrument_id: Any, kind: str) -> None:
        if not hasattr(self, "_ticker_intents"):
            self._ticker_intents = {}
        if not hasattr(self, "_subscriptions"):
            self._subscriptions = set()
        symbol, exchange = self._route(instrument_id)
        key = (symbol, exchange)
        intents = self._ticker_intents.setdefault(key, set())
        if kind in intents:
            return
        needs_subscribe = key not in self._subscriptions
        intents.add(kind)
        if needs_subscribe:
            try:
                await asyncio.to_thread(self._session.subscribe, symbol, exchange)
                self._subscriptions.add(key)
            except Exception:
                intents.discard(kind)
                if not intents:
                    del self._ticker_intents[key]
                raise

    async def _subscribe_symbol(self, instrument_id: Any) -> None:
        symbol, exchange = self._route(instrument_id)
        key = (symbol, exchange)
        if key not in self._subscriptions:
            await asyncio.to_thread(self._session.subscribe, symbol, exchange)
            self._subscriptions.add(key)

    async def _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None:
        await self._remove_ticker_intent(command.instrument_id, "trade")

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        await self._remove_ticker_intent(command.instrument_id, "quote", clear_bbo=True)

    async def _remove_ticker_intent(
        self, instrument_id: Any, kind: str, *, clear_bbo: bool = False
    ) -> None:
        if not hasattr(self, "_ticker_intents"):
            self._ticker_intents = {}
        if not hasattr(self, "_subscriptions"):
            self._subscriptions = set()
        if not hasattr(self, "_bbo_state"):
            self._bbo_state = {}
        symbol, exchange = self._route(instrument_id)
        key = (symbol, exchange)
        if clear_bbo:
            self._bbo_state.pop(f"{symbol}:{exchange}", None)
        intents = self._ticker_intents.get(key)
        if intents is None or kind not in intents:
            # Legacy fallback: if tests set _subscriptions directly, honour it.
            if key in self._subscriptions:
                try:
                    await asyncio.to_thread(self._session.unsubscribe, symbol, exchange)
                    self._subscriptions.discard(key)
                except Exception:
                    raise
            return
        intents.discard(kind)
        if not intents:
            del self._ticker_intents[key]
            if key in self._subscriptions:
                try:
                    await asyncio.to_thread(self._session.unsubscribe, symbol, exchange)
                    self._subscriptions.discard(key)
                except Exception:
                    self._ticker_intents[key] = {kind}
                    raise

    async def _subscribe_order_book_deltas(self, command: SubscribeOrderBook) -> None:
        symbol, exchange = self._route(command.instrument_id)
        await asyncio.to_thread(
            self._session.subscribe_order_book_summary, symbol, exchange
        )
        self._book_subscriptions.add((symbol, exchange))

    async def _unsubscribe_order_book_deltas(
        self, command: UnsubscribeOrderBook
    ) -> None:
        symbol, exchange = self._route(command.instrument_id)
        await asyncio.to_thread(
            self._session.unsubscribe_order_book_summary, symbol, exchange
        )
        self._book_subscriptions.discard((symbol, exchange))

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        bar_type = command.bar_type
        if not bar_type.is_externally_aggregated():
            return
        if not external_bar_advertised(bar_type):
            raise ValueError(
                f"live EXTERNAL bars not advertised for {bar_type}; "
                "use request_bars for lookback or INTERNAL 1s from ticks"
            )
        symbol, exchange = self._route(bar_type.instrument_id)
        rtype, period = bar_type_to_rithmic(bar_type)
        await asyncio.to_thread(
            self._session.subscribe_time_bars, symbol, exchange, rtype, period
        )
        for p in bar_wire_period_keys(bar_type):
            self._bar_types.setdefault((symbol, exchange, rtype, p), set()).add(
                bar_type
            )
        self._ensure_history_poll_task()

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        bar_type = command.bar_type
        if not bar_type.is_externally_aggregated():
            return
        if not external_bar_advertised(bar_type):
            return
        symbol, exchange = self._route(bar_type.instrument_id)
        rtype, period = bar_type_to_rithmic(bar_type)
        # Save for rollback
        saved: dict[tuple[str, str, int, int], set] = {}
        for p in bar_wire_period_keys(bar_type):
            key = (symbol, exchange, rtype, p)
            saved[key] = set(self._bar_types.get(key, set()))
        for p in bar_wire_period_keys(bar_type):
            key = (symbol, exchange, rtype, p)
            mapped = self._bar_types.get(key)
            if mapped is not None:
                mapped.discard(bar_type)
                if not mapped:
                    del self._bar_types[key]
        still_registered = any(
            k[0] == symbol and k[1] == exchange and k[2] == rtype and self._bar_types[k]
            for k in self._bar_types
        )
        if not still_registered:
            try:
                await asyncio.to_thread(
                    self._session.unsubscribe_time_bars, symbol, exchange, rtype, period
                )
            except Exception:
                # Rollback
                for k, v in saved.items():
                    if v:
                        self._bar_types[k] = v
                    else:
                        self._bar_types.pop(k, None)
                raise

    async def _request_trade_ticks(self, request: RequestTradeTicks) -> None:
        symbol, exchange = self._route(request.instrument_id)
        if request.start is None or request.end is None:
            self._log.error("RequestTradeTicks requires start and end")
            self._handle_trade_ticks(
                request.instrument_id,
                [],
                request.id,
                request.start,
                request.end,
                request.params,
            )
            return
        start = int(request.start.timestamp())
        end = int(request.end.timestamp())
        try:
            ticks_raw = await asyncio.to_thread(
                self._session.load_ticks,
                symbol,
                exchange,
                start,
                end,
            )
        except Exception as exc:
            self._log.error(
                f"Error requesting trade ticks for {request.instrument_id}: {exc}"
            )
            self._handle_trade_ticks(
                request.instrument_id,
                [],
                request.id,
                request.start,
                request.end,
                request.params,
            )
            return

        ts_init = self._clock.timestamp_ns()
        try:
            prec = self._price_precision(request.instrument_id)
            ticks = payloads_to_trade_ticks(
                list(ticks_raw),
                symbol=symbol,
                exchange=exchange,
                price_precision=prec,
                ts_init=ts_init,
            )
        except ValueError as exc:
            self._log.error(f"Invalid history tick for {request.instrument_id}: {exc}")
            self._handle_trade_ticks(
                request.instrument_id,
                [],
                request.id,
                request.start,
                request.end,
                request.params,
            )
            return

        # Order: Rithmic FORWARDS replay + session sorts by ts_event_ns.
        # Nautilus _handle_trade_ticks does not re-sort; last-N needs time order.
        if request.limit:
            ticks = ticks[-request.limit :]
        self._handle_trade_ticks(
            request.instrument_id,
            ticks,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    async def _request_bars(self, request: RequestBars) -> None:
        bar_type = request.bar_type
        symbol, exchange = self._route(bar_type.instrument_id)
        try:
            rithmic_type, period = bar_type_to_rithmic(bar_type)
        except ValueError as exc:
            self._log.error(str(exc))
            self._handle_bars(
                bar_type,
                [],
                request.id,
                request.start,
                request.end,
                request.params,
            )
            return
        if request.start is None or request.end is None:
            self._log.error("RequestBars requires start and end")
            self._handle_bars(
                bar_type,
                [],
                request.id,
                request.start,
                request.end,
                request.params,
            )
            return
        start = int(request.start.timestamp())
        end = int(request.end.timestamp())
        self._log.info(
            f"history {bar_type} → rithmic type={rithmic_type} period={period} "
            f"(not 1s/1m unless that is the requested type)"
        )
        try:
            bars_raw = await asyncio.to_thread(
                self._session.load_time_bars,
                symbol,
                exchange,
                start,
                end,
                rithmic_type,
                period,
            )
        except Exception as exc:
            self._log.error(f"Error requesting bars for {bar_type}: {exc}")
            self._handle_bars(
                bar_type,
                [],
                request.id,
                request.start,
                request.end,
                request.params,
            )
            return

        ts_init = self._clock.timestamp_ns()
        try:
            prec = self._price_precision(bar_type.instrument_id)
            bars = payloads_to_bars(
                list(bars_raw),
                symbol=symbol,
                exchange=exchange,
                bar_type=bar_type,
                price_precision=prec,
                ts_init=ts_init,
            )
        except ValueError as exc:
            self._log.error(f"Invalid history bar for {bar_type}: {exc}")
            self._handle_bars(
                bar_type,
                [],
                request.id,
                request.start,
                request.end,
                request.params,
            )
            return

        # Order: FORWARDS + session sort by ts_event_ns (NT does not re-sort).
        if request.limit:
            bars = bars[-request.limit :]
        self._log.info(
            f"loaded {len(bars)} {bar_type} history bars (raw={len(bars_raw)})"
        )
        self._handle_bars(
            bar_type,
            bars,
            request.id,
            request.start,
            request.end,
            request.params,
        )
