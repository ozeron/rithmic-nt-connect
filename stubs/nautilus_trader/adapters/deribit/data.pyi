import asyncio
from _typeshed import Incomplete
from nautilus_trader.adapters.deribit.config import DeribitDataClientConfig as DeribitDataClientConfig
from nautilus_trader.adapters.deribit.constants import DERIBIT_DATA_SESSION_NAME as DERIBIT_DATA_SESSION_NAME, DERIBIT_VENUE as DERIBIT_VENUE, DERIBIT_WS_HEARTBEAT_SECS as DERIBIT_WS_HEARTBEAT_SECS
from nautilus_trader.adapters.deribit.providers import DeribitInstrumentProvider as DeribitInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.cache.transformers import transform_instrument_from_pyo3 as transform_instrument_from_pyo3
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor
from nautilus_trader.common.secure import mask_api_key as mask_api_key
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.data import Data as Data
from nautilus_trader.core.datetime import ensure_pydatetime_utc as ensure_pydatetime_utc
from nautilus_trader.core.nautilus_pyo3 import DeribitCurrency as DeribitCurrency, DeribitEnvironment as DeribitEnvironment, DeribitUpdateInterval as DeribitUpdateInterval
from nautilus_trader.data.messages import RequestBars as RequestBars, RequestForwardPrices as RequestForwardPrices, RequestInstrument as RequestInstrument, RequestInstruments as RequestInstruments, RequestOrderBookSnapshot as RequestOrderBookSnapshot, RequestTradeTicks as RequestTradeTicks, SubscribeBars as SubscribeBars, SubscribeData as SubscribeData, SubscribeFundingRates as SubscribeFundingRates, SubscribeIndexPrices as SubscribeIndexPrices, SubscribeInstrument as SubscribeInstrument, SubscribeInstrumentStatus as SubscribeInstrumentStatus, SubscribeInstruments as SubscribeInstruments, SubscribeMarkPrices as SubscribeMarkPrices, SubscribeOptionGreeks as SubscribeOptionGreeks, SubscribeOrderBook as SubscribeOrderBook, SubscribeQuoteTicks as SubscribeQuoteTicks, SubscribeTradeTicks as SubscribeTradeTicks, UnsubscribeBars as UnsubscribeBars, UnsubscribeData as UnsubscribeData, UnsubscribeFundingRates as UnsubscribeFundingRates, UnsubscribeIndexPrices as UnsubscribeIndexPrices, UnsubscribeInstrument as UnsubscribeInstrument, UnsubscribeInstrumentStatus as UnsubscribeInstrumentStatus, UnsubscribeInstruments as UnsubscribeInstruments, UnsubscribeMarkPrices as UnsubscribeMarkPrices, UnsubscribeOptionGreeks as UnsubscribeOptionGreeks, UnsubscribeOrderBook as UnsubscribeOrderBook, UnsubscribeQuoteTicks as UnsubscribeQuoteTicks, UnsubscribeTradeTicks as UnsubscribeTradeTicks
from nautilus_trader.live.cancellation import DEFAULT_FUTURE_CANCELLATION_TIMEOUT as DEFAULT_FUTURE_CANCELLATION_TIMEOUT, cancel_tasks_with_timeout as cancel_tasks_with_timeout
from nautilus_trader.live.data_client import LiveMarketDataClient as LiveMarketDataClient
from nautilus_trader.model.data import Bar as Bar, BookOrder as BookOrder, CustomData as CustomData, DataType as DataType, FundingRateUpdate as FundingRateUpdate, InstrumentStatus as InstrumentStatus, OptionGreeks as OptionGreeks, OrderBookDelta as OrderBookDelta, OrderBookDeltas as OrderBookDeltas, TradeTick as TradeTick, capsule_to_data as capsule_to_data
from nautilus_trader.model.enums import BookAction as BookAction, BookType as BookType, OrderSide as OrderSide, RecordFlag as RecordFlag, book_type_to_str as book_type_to_str
from nautilus_trader.model.identifiers import ClientId as ClientId, InstrumentId as InstrumentId
from nautilus_trader.model.instruments import Instrument as Instrument
from typing import Any

class DeribitVolatilityIndex(Data):
    index_name: Incomplete
    volatility: Incomplete
    def __init__(self, index_name: str, volatility: float, ts_event: int, ts_init: int) -> None: ...
    @property
    def ts_event(self) -> int: ...
    @property
    def ts_init(self) -> int: ...
    @staticmethod
    def from_pyo3(pyo3_dvol: Any) -> DeribitVolatilityIndex: ...

class DeribitDataClient(LiveMarketDataClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: nautilus_pyo3.DeribitHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: DeribitInstrumentProvider, config: DeribitDataClientConfig, name: str | None) -> None: ...
    @property
    def instrument_provider(self) -> DeribitInstrumentProvider: ...
