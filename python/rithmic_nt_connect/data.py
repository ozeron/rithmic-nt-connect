"""Live market data client for Rithmic (Phase 1)."""

from __future__ import annotations

import asyncio
from typing import Any

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.data.messages import RequestBars
from nautilus_trader.data.messages import RequestTradeTicks
from nautilus_trader.data.messages import SubscribeOrderBook
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import SubscribeTradeTicks
from nautilus_trader.data.messages import UnsubscribeOrderBook
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeTradeTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import BookOrder
from nautilus_trader.model.data import OrderBookDelta
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import BookAction
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

from rithmic_nt_connect._convert import ConvertError
from rithmic_nt_connect._convert import bbo_to_fields
from rithmic_nt_connect._convert import format_price_str
from rithmic_nt_connect._convert import last_trade_to_fields
from rithmic_nt_connect._convert import order_book_to_fields
from rithmic_nt_connect._convert import time_bar_to_fields
from rithmic_nt_connect.config import RithmicDataClientConfig
from rithmic_nt_connect.constants import ADAPTER_NAME
from rithmic_nt_connect.constants import VENUE
from rithmic_nt_connect.providers import RithmicInstrumentProvider
from rithmic_nt_connect.session import WireSession


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


def _price(value: float) -> Price:
    return Price.from_str(format_price_str(value))


def fields_to_trade_tick(fields: dict[str, Any], ts_init: int) -> TradeTick:
    size = int(fields["size"])
    if size < 1:
        raise ConvertError(f"trade size must be >= 1, got {size}")
    return TradeTick(
        InstrumentId.from_str(fields["instrument_id"]),
        _price(fields["price"]),
        Quantity.from_int(size),
        _aggressor(fields.get("aggressor")),
        TradeId(str(fields["ts_event"])),
        int(fields["ts_event"]),
        ts_init,
    )


def fields_to_quote_tick(fields: dict[str, Any], ts_init: int) -> QuoteTick:
    bid_size = int(fields["bid_size"])
    ask_size = int(fields["ask_size"])
    if bid_size < 1 or ask_size < 1:
        raise ConvertError(f"quote sizes must be >= 1, got bid={bid_size} ask={ask_size}")
    return QuoteTick(
        InstrumentId.from_str(fields["instrument_id"]),
        _price(fields["bid_price"]),
        _price(fields["ask_price"]),
        Quantity.from_int(bid_size),
        Quantity.from_int(ask_size),
        int(fields["ts_event"]),
        ts_init,
    )


def fields_to_order_book_deltas(fields: dict[str, Any], ts_init: int) -> OrderBookDeltas:
    instrument_id = InstrumentId.from_str(fields["instrument_id"])
    ts_event = int(fields["ts_event"])
    deltas: list[OrderBookDelta] = [
        OrderBookDelta.clear(instrument_id, 0, ts_event, ts_init),
    ]
    for level in fields["levels"]:
        side = OrderSide.BUY if level["side"] == "BUY" else OrderSide.SELL
        size = int(level["size"])
        if size < 0:
            raise ConvertError(f"order book level size must be >= 0, got {size}")
        if size == 0:
            continue
        order = BookOrder(
            side,
            _price(level["price"]),
            Quantity.from_int(size),
            int(level["order_id"]),
        )
        deltas.append(
            OrderBookDelta(
                instrument_id,
                BookAction.ADD,
                order,
                0,
                0,
                ts_event,
                ts_init,
            )
        )
    return OrderBookDeltas(instrument_id=instrument_id, deltas=deltas)


def fields_to_bar(fields: dict[str, Any], bar_type: BarType, ts_init: int) -> Bar:
    volume = int(fields["volume"])
    if volume < 0:
        raise ConvertError(f"bar volume must be >= 0, got {volume}")
    return Bar(
        bar_type,
        _price(fields["open"]),
        _price(fields["high"]),
        _price(fields["low"]),
        _price(fields["close"]),
        Quantity.from_int(volume),
        int(fields["ts_event"]),
        ts_init,
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


class RithmicDataClient(LiveMarketDataClient):
    """Out-of-tree LiveMarketDataClient backed by the Rust Rithmic session."""

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
        self._subscriptions: set[tuple[str, str]] = set()
        self._instrument_routes: dict[str, tuple[str, str]] = {}

    async def _connect(self) -> None:
        await asyncio.to_thread(self._session.connect)
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
        self._poll_task = self.create_task(self._poll_loop(), log_msg="rithmic_poll")

    async def _disconnect(self) -> None:
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        await asyncio.to_thread(self._session.disconnect)

    async def _poll_loop(self) -> None:
        while True:
            event = await asyncio.to_thread(self._session.poll_event)
            if event is None:
                await asyncio.sleep(0.01)
                continue
            self._dispatch_event(event)

    def _dispatch_event(self, event: dict[str, Any]) -> None:
        etype = event.get("type")
        ts_init = self._clock.timestamp_ns()
        try:
            if etype == "last_trade":
                fields = last_trade_to_fields(event)
                self._handle_data(fields_to_trade_tick(fields, ts_init))
            elif etype == "bbo":
                fields = bbo_to_fields(event)
                self._handle_data(fields_to_quote_tick(fields, ts_init))
            elif etype == "order_book":
                fields = order_book_to_fields(event)
                self._handle_data(fields_to_order_book_deltas(fields, ts_init))
        except ConvertError as exc:
            self._log.debug(f"skip event {etype}: {exc}")

    def _route(self, instrument_id: InstrumentId) -> tuple[str, str]:
        key = str(instrument_id)
        if key not in self._instrument_routes:
            raise ValueError(
                f"no Rithmic route for {instrument_id}; load instruments before subscribe/request"
            )
        return self._instrument_routes[key]

    async def _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None:
        symbol, exchange = self._route(command.instrument_id)
        await asyncio.to_thread(self._session.subscribe, symbol, exchange)
        self._subscriptions.add((symbol, exchange))

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        symbol, exchange = self._route(command.instrument_id)
        await asyncio.to_thread(self._session.subscribe, symbol, exchange)
        self._subscriptions.add((symbol, exchange))

    async def _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None:
        symbol, exchange = self._route(command.instrument_id)
        await asyncio.to_thread(self._session.unsubscribe, symbol, exchange)
        self._subscriptions.discard((symbol, exchange))

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        symbol, exchange = self._route(command.instrument_id)
        await asyncio.to_thread(self._session.unsubscribe, symbol, exchange)
        self._subscriptions.discard((symbol, exchange))

    async def _subscribe_order_book_deltas(self, command: SubscribeOrderBook) -> None:
        symbol, exchange = self._route(command.instrument_id)
        await asyncio.to_thread(self._session.subscribe_order_book_summary, symbol, exchange)

    async def _unsubscribe_order_book_deltas(self, command: UnsubscribeOrderBook) -> None:
        _ = command
        return

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
        except Exception as exc:  # noqa: BLE001 — always complete RequestTradeTicks
            self._log.error(f"Error requesting trade ticks for {request.instrument_id}: {exc}")
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
        ticks: list[TradeTick] = []
        try:
            for raw in ticks_raw:
                payload = dict(raw)
                if "symbol" not in payload or payload["symbol"] is None:
                    payload["symbol"] = symbol
                if "exchange" not in payload or payload["exchange"] is None:
                    payload["exchange"] = exchange
                fields = last_trade_to_fields(payload)
                ticks.append(fields_to_trade_tick(fields, ts_init))
        except (ConvertError, ValueError) as exc:
            self._log.error(
                f"Invalid history tick for {request.instrument_id}: {exc}"
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
        except Exception as exc:  # noqa: BLE001 — always complete RequestBars
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
        bars: list[Bar] = []
        try:
            for raw in bars_raw:
                payload = dict(raw)
                if "symbol" not in payload or payload["symbol"] is None:
                    payload["symbol"] = symbol
                if "exchange" not in payload or payload["exchange"] is None:
                    payload["exchange"] = exchange
                fields = time_bar_to_fields(payload)
                bars.append(fields_to_bar(fields, bar_type, ts_init))
        except (ConvertError, ValueError) as exc:
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
        self._handle_bars(
            bar_type,
            bars,
            request.id,
            request.start,
            request.end,
            request.params,
        )
