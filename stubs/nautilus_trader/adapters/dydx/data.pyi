import asyncio
from nautilus_trader.adapters.dydx.config import DydxDataClientConfig as DydxDataClientConfig
from nautilus_trader.adapters.dydx.constants import DYDX_VENUE as DYDX_VENUE
from nautilus_trader.adapters.dydx.providers import DydxInstrumentProvider as DydxInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.datetime import ensure_pydatetime_utc as ensure_pydatetime_utc
from nautilus_trader.core.nautilus_pyo3 import DydxNetwork as DydxNetwork
from nautilus_trader.data.messages import RequestBars as RequestBars, RequestFundingRates as RequestFundingRates, RequestInstrument as RequestInstrument, RequestInstruments as RequestInstruments, RequestOrderBookSnapshot as RequestOrderBookSnapshot, RequestQuoteTicks as RequestQuoteTicks, RequestTradeTicks as RequestTradeTicks, SubscribeBars as SubscribeBars, SubscribeFundingRates as SubscribeFundingRates, SubscribeIndexPrices as SubscribeIndexPrices, SubscribeInstrument as SubscribeInstrument, SubscribeInstrumentClose as SubscribeInstrumentClose, SubscribeInstrumentStatus as SubscribeInstrumentStatus, SubscribeInstruments as SubscribeInstruments, SubscribeMarkPrices as SubscribeMarkPrices, SubscribeOrderBook as SubscribeOrderBook, SubscribeQuoteTicks as SubscribeQuoteTicks, SubscribeTradeTicks as SubscribeTradeTicks, UnsubscribeBars as UnsubscribeBars, UnsubscribeFundingRates as UnsubscribeFundingRates, UnsubscribeIndexPrices as UnsubscribeIndexPrices, UnsubscribeInstrument as UnsubscribeInstrument, UnsubscribeInstrumentClose as UnsubscribeInstrumentClose, UnsubscribeInstrumentStatus as UnsubscribeInstrumentStatus, UnsubscribeInstruments as UnsubscribeInstruments, UnsubscribeMarkPrices as UnsubscribeMarkPrices, UnsubscribeOrderBook as UnsubscribeOrderBook, UnsubscribeQuoteTicks as UnsubscribeQuoteTicks, UnsubscribeTradeTicks as UnsubscribeTradeTicks
from nautilus_trader.live.cancellation import DEFAULT_FUTURE_CANCELLATION_TIMEOUT as DEFAULT_FUTURE_CANCELLATION_TIMEOUT, cancel_tasks_with_timeout as cancel_tasks_with_timeout
from nautilus_trader.live.data_client import LiveMarketDataClient as LiveMarketDataClient
from nautilus_trader.model.book import OrderBook as OrderBook
from nautilus_trader.model.data import Bar as Bar, FundingRateUpdate as FundingRateUpdate, IndexPriceUpdate as IndexPriceUpdate, InstrumentStatus as InstrumentStatus, MarkPriceUpdate as MarkPriceUpdate, OrderBookDeltas as OrderBookDeltas, QuoteTick as QuoteTick, TradeTick as TradeTick, capsule_to_data as capsule_to_data
from nautilus_trader.model.enums import BookType as BookType, book_type_to_str as book_type_to_str
from nautilus_trader.model.identifiers import ClientId as ClientId, InstrumentId as InstrumentId

class DydxDataClient(LiveMarketDataClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: nautilus_pyo3.DydxHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: DydxInstrumentProvider, config: DydxDataClientConfig, name: str | None) -> None: ...
    @property
    def instrument_provider(self) -> DydxInstrumentProvider: ...
