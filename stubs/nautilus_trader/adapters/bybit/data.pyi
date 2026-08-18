import asyncio
from nautilus_trader.adapters.bybit.config import BybitDataClientConfig as BybitDataClientConfig
from nautilus_trader.adapters.bybit.constants import BYBIT_VENUE as BYBIT_VENUE
from nautilus_trader.adapters.bybit.providers import BybitInstrumentProvider as BybitInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.datetime import ensure_pydatetime_utc as ensure_pydatetime_utc
from nautilus_trader.data.messages import RequestBars as RequestBars, RequestForwardPrices as RequestForwardPrices, RequestFundingRates as RequestFundingRates, RequestOrderBookSnapshot as RequestOrderBookSnapshot, RequestQuoteTicks as RequestQuoteTicks, RequestTradeTicks as RequestTradeTicks, SubscribeBars as SubscribeBars, SubscribeFundingRates as SubscribeFundingRates, SubscribeIndexPrices as SubscribeIndexPrices, SubscribeInstrumentStatus as SubscribeInstrumentStatus, SubscribeMarkPrices as SubscribeMarkPrices, SubscribeOptionGreeks as SubscribeOptionGreeks, SubscribeOrderBook as SubscribeOrderBook, SubscribeQuoteTicks as SubscribeQuoteTicks, SubscribeTradeTicks as SubscribeTradeTicks, UnsubscribeBars as UnsubscribeBars, UnsubscribeFundingRates as UnsubscribeFundingRates, UnsubscribeIndexPrices as UnsubscribeIndexPrices, UnsubscribeInstrumentStatus as UnsubscribeInstrumentStatus, UnsubscribeMarkPrices as UnsubscribeMarkPrices, UnsubscribeOptionGreeks as UnsubscribeOptionGreeks, UnsubscribeOrderBook as UnsubscribeOrderBook, UnsubscribeQuoteTicks as UnsubscribeQuoteTicks, UnsubscribeTradeTicks as UnsubscribeTradeTicks
from nautilus_trader.live.cancellation import DEFAULT_FUTURE_CANCELLATION_TIMEOUT as DEFAULT_FUTURE_CANCELLATION_TIMEOUT, cancel_tasks_with_timeout as cancel_tasks_with_timeout
from nautilus_trader.live.data_client import LiveMarketDataClient as LiveMarketDataClient
from nautilus_trader.model.data import Bar as Bar, DataType as DataType, FundingRateUpdate as FundingRateUpdate, IndexPriceUpdate as IndexPriceUpdate, InstrumentStatus as InstrumentStatus, MarkPriceUpdate as MarkPriceUpdate, OptionGreeks as OptionGreeks, OrderBookDeltas as OrderBookDeltas, TradeTick as TradeTick, capsule_to_data as capsule_to_data
from nautilus_trader.model.enums import BookType as BookType, MarketStatusAction as MarketStatusAction, PriceType as PriceType, book_type_to_str as book_type_to_str
from nautilus_trader.model.identifiers import ClientId as ClientId, InstrumentId as InstrumentId

class BybitDataClient(LiveMarketDataClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: nautilus_pyo3.BybitHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: BybitInstrumentProvider, config: BybitDataClientConfig, name: str | None) -> None: ...
    @property
    def instrument_provider(self) -> BybitInstrumentProvider: ...
