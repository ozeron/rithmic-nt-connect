"""Live market data client for Rithmic (Phase 1)."""

from __future__ import annotations

import asyncio
from typing import Any

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.data.messages import RequestTradeTicks
from nautilus_trader.data.messages import SubscribeOrderBook
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import SubscribeTradeTicks
from nautilus_trader.data.messages import UnsubscribeOrderBook
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeTradeTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

from rithmic_connect._convert import ConvertError
from rithmic_connect._convert import bbo_to_fields
from rithmic_connect._convert import last_trade_to_fields
from rithmic_connect.config import RithmicDataClientConfig
from rithmic_connect.constants import ADAPTER_NAME
from rithmic_connect.constants import VENUE
from rithmic_connect.providers import RithmicInstrumentProvider
from rithmic_connect.session import WireSession


def _aggressor(value: Any) -> AggressorSide:
    if value in (1, "1", "BUY", "BUYER", "bid"):
        return AggressorSide.BUYER
    if value in (2, "2", "SELL", "SELLER", "ask"):
        return AggressorSide.SELLER
    return AggressorSide.NO_AGGRESSOR


def _price(value: float) -> Price:
    text = f"{float(value):.8f}".rstrip("0").rstrip(".")
    if "." not in text:
        text = f"{text}.0"
    return Price.from_str(text)


def fields_to_trade_tick(fields: dict[str, Any], ts_init: int) -> TradeTick:
    size = max(int(fields["size"]), 1)
    return TradeTick(
        InstrumentId.from_str(fields["instrument_id"]),
        _price(fields["price"]),
        Quantity.from_int(size),
        _aggressor(fields.get("aggressor")),
        TradeId(str(fields.get("ts_event", ts_init))),
        int(fields["ts_event"]),
        ts_init,
    )


def fields_to_quote_tick(fields: dict[str, Any], ts_init: int) -> QuoteTick:
    return QuoteTick(
        InstrumentId.from_str(fields["instrument_id"]),
        _price(fields["bid_price"]),
        _price(fields["ask_price"]),
        Quantity.from_int(max(int(fields["bid_size"]), 1)),
        Quantity.from_int(max(int(fields["ask_size"]), 1)),
        int(fields["ts_event"]),
        ts_init,
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
            exchange = str(info.get("rithmic_exchange", "CME"))
            symbol = str(info.get("rithmic_symbol", instrument.raw_symbol.value))
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
        except ConvertError as exc:
            self._log.debug(f"skip event {etype}: {exc}")

    def _route(self, instrument_id: InstrumentId) -> tuple[str, str]:
        key = str(instrument_id)
        if key in self._instrument_routes:
            return self._instrument_routes[key]
        return instrument_id.symbol.value, "CME"

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
        start = int(request.start.timestamp()) if request.start else 0
        end = int(request.end.timestamp()) if request.end else start
        ticks = await asyncio.to_thread(self._session.load_ticks, symbol, exchange, start, end)
        ts_init = self._clock.timestamp_ns()
        for raw in ticks:
            payload = dict(raw)
            if payload.get("type") == "history_tick" and "trade_price" not in payload:
                payload["trade_price"] = payload.get("close_price") or payload.get("open_price")
                payload["trade_size"] = payload.get("trade_size") or payload.get("num_trades") or 1
                payload.setdefault("symbol", symbol)
                payload.setdefault("exchange", exchange)
            try:
                fields = last_trade_to_fields(payload)
                self._handle_data(fields_to_trade_tick(fields, ts_init))
            except ConvertError:
                continue
