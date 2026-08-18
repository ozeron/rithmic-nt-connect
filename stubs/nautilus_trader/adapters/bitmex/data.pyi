import asyncio
from nautilus_trader.adapters.bitmex.config import BitmexDataClientConfig as BitmexDataClientConfig
from nautilus_trader.adapters.bitmex.constants import BITMEX_VENUE as BITMEX_VENUE
from nautilus_trader.adapters.bitmex.providers import BitmexInstrumentProvider as BitmexInstrumentProvider
from nautilus_trader.adapters.bitmex.types import BITMEX_INSTRUMENT_TYPES as BITMEX_INSTRUMENT_TYPES, BitmexInstrument as BitmexInstrument
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.cache.transformers import transform_instrument_from_pyo3 as transform_instrument_from_pyo3
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.datetime import ensure_pydatetime_utc as ensure_pydatetime_utc
from nautilus_trader.core.nautilus_pyo3 import BitmexEnvironment as BitmexEnvironment
from nautilus_trader.data.messages import RequestBars as RequestBars, RequestFundingRates as RequestFundingRates, RequestInstrument as RequestInstrument, RequestInstruments as RequestInstruments, RequestOrderBookSnapshot as RequestOrderBookSnapshot, RequestTradeTicks as RequestTradeTicks, SubscribeBars as SubscribeBars, SubscribeInstrument as SubscribeInstrument, SubscribeInstrumentStatus as SubscribeInstrumentStatus, SubscribeInstruments as SubscribeInstruments, SubscribeOrderBook as SubscribeOrderBook, SubscribeQuoteTicks as SubscribeQuoteTicks, SubscribeTradeTicks as SubscribeTradeTicks, UnsubscribeBars as UnsubscribeBars, UnsubscribeInstrument as UnsubscribeInstrument, UnsubscribeInstrumentStatus as UnsubscribeInstrumentStatus, UnsubscribeInstruments as UnsubscribeInstruments, UnsubscribeOrderBook as UnsubscribeOrderBook, UnsubscribeQuoteTicks as UnsubscribeQuoteTicks, UnsubscribeTradeTicks as UnsubscribeTradeTicks
from nautilus_trader.live.cancellation import DEFAULT_FUTURE_CANCELLATION_TIMEOUT as DEFAULT_FUTURE_CANCELLATION_TIMEOUT, cancel_tasks_with_timeout as cancel_tasks_with_timeout
from nautilus_trader.live.data_client import LiveMarketDataClient as LiveMarketDataClient
from nautilus_trader.model.data import Bar as Bar, BookOrder as BookOrder, DataType as DataType, FundingRateUpdate as FundingRateUpdate, InstrumentStatus as InstrumentStatus, OrderBookDelta as OrderBookDelta, OrderBookDeltas as OrderBookDeltas, TradeTick as TradeTick, capsule_to_data as capsule_to_data
from nautilus_trader.model.enums import AggregationSource as AggregationSource, BarAggregation as BarAggregation, BookAction as BookAction, BookType as BookType, OrderSide as OrderSide, PriceType as PriceType, RecordFlag as RecordFlag, book_type_to_str as book_type_to_str
from nautilus_trader.model.identifiers import ClientId as ClientId

class BitmexDataClient(LiveMarketDataClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: nautilus_pyo3.BitmexHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: BitmexInstrumentProvider, config: BitmexDataClientConfig, name: str | None) -> None: ...
    @property
    def instrument_provider(self) -> BitmexInstrumentProvider: ...
