import asyncio
from nautilus_trader.adapters.kraken.config import KrakenDataClientConfig as KrakenDataClientConfig
from nautilus_trader.adapters.kraken.constants import KRAKEN_VENUE as KRAKEN_VENUE
from nautilus_trader.adapters.kraken.providers import KrakenInstrumentProvider as KrakenInstrumentProvider
from nautilus_trader.adapters.kraken.types import KRAKEN_INSTRUMENT_TYPES as KRAKEN_INSTRUMENT_TYPES, KrakenInstrument as KrakenInstrument
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.cache.transformers import transform_instrument_from_pyo3 as transform_instrument_from_pyo3
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.datetime import ensure_pydatetime_utc as ensure_pydatetime_utc
from nautilus_trader.core.nautilus_pyo3 import KrakenEnvironment as KrakenEnvironment, KrakenProductType as KrakenProductType
from nautilus_trader.data.messages import RequestBars as RequestBars, RequestInstrument as RequestInstrument, RequestInstruments as RequestInstruments, RequestOrderBookSnapshot as RequestOrderBookSnapshot, RequestTradeTicks as RequestTradeTicks, SubscribeBars as SubscribeBars, SubscribeFundingRates as SubscribeFundingRates, SubscribeIndexPrices as SubscribeIndexPrices, SubscribeInstrument as SubscribeInstrument, SubscribeInstrumentStatus as SubscribeInstrumentStatus, SubscribeInstruments as SubscribeInstruments, SubscribeMarkPrices as SubscribeMarkPrices, SubscribeOrderBook as SubscribeOrderBook, SubscribeQuoteTicks as SubscribeQuoteTicks, SubscribeTradeTicks as SubscribeTradeTicks, UnsubscribeBars as UnsubscribeBars, UnsubscribeFundingRates as UnsubscribeFundingRates, UnsubscribeIndexPrices as UnsubscribeIndexPrices, UnsubscribeInstrument as UnsubscribeInstrument, UnsubscribeInstrumentStatus as UnsubscribeInstrumentStatus, UnsubscribeInstruments as UnsubscribeInstruments, UnsubscribeMarkPrices as UnsubscribeMarkPrices, UnsubscribeOrderBook as UnsubscribeOrderBook, UnsubscribeQuoteTicks as UnsubscribeQuoteTicks, UnsubscribeTradeTicks as UnsubscribeTradeTicks
from nautilus_trader.live.cancellation import DEFAULT_FUTURE_CANCELLATION_TIMEOUT as DEFAULT_FUTURE_CANCELLATION_TIMEOUT, cancel_tasks_with_timeout as cancel_tasks_with_timeout
from nautilus_trader.live.data_client import LiveMarketDataClient as LiveMarketDataClient
from nautilus_trader.model.data import Bar as Bar, BookOrder as BookOrder, DataType as DataType, FundingRateUpdate as FundingRateUpdate, InstrumentStatus as InstrumentStatus, OrderBookDelta as OrderBookDelta, OrderBookDeltas as OrderBookDeltas, TradeTick as TradeTick, capsule_to_data as capsule_to_data
from nautilus_trader.model.enums import BookAction as BookAction, BookType as BookType, MarketStatusAction as MarketStatusAction, OrderSide as OrderSide, RecordFlag as RecordFlag, book_type_to_str as book_type_to_str
from nautilus_trader.model.identifiers import ClientId as ClientId, InstrumentId as InstrumentId

class KrakenDataClient(LiveMarketDataClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, http_client_spot: nautilus_pyo3.KrakenSpotHttpClient | None, http_client_futures: nautilus_pyo3.KrakenFuturesHttpClient | None, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: KrakenInstrumentProvider, config: KrakenDataClientConfig, name: str | None) -> None: ...
    @property
    def instrument_provider(self) -> KrakenInstrumentProvider: ...
