import asyncio
from nautilus_trader.adapters.okx.config import OKXDataClientConfig as OKXDataClientConfig
from nautilus_trader.adapters.okx.constants import OKX_VENUE as OKX_VENUE
from nautilus_trader.adapters.okx.providers import OKXInstrumentProvider as OKXInstrumentProvider
from nautilus_trader.adapters.okx.types import GREEKS_CONVENTION_TO_TYPE as GREEKS_CONVENTION_TO_TYPE, OKX_INSTRUMENT_TYPES as OKX_INSTRUMENT_TYPES, OkxInstrument as OkxInstrument
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.cache.transformers import transform_instrument_from_pyo3 as transform_instrument_from_pyo3
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor
from nautilus_trader.common.secure import mask_api_key as mask_api_key
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.core.datetime import ensure_pydatetime_utc as ensure_pydatetime_utc
from nautilus_trader.core.nautilus_pyo3 import GreeksConvention as GreeksConvention, OKXEnvironment as OKXEnvironment, OKXGreeksType as OKXGreeksType, OKXRegion as OKXRegion
from nautilus_trader.data.messages import RequestBars as RequestBars, RequestForwardPrices as RequestForwardPrices, RequestFundingRates as RequestFundingRates, RequestInstrument as RequestInstrument, RequestInstruments as RequestInstruments, RequestOrderBookSnapshot as RequestOrderBookSnapshot, RequestQuoteTicks as RequestQuoteTicks, RequestTradeTicks as RequestTradeTicks, SubscribeBars as SubscribeBars, SubscribeFundingRates as SubscribeFundingRates, SubscribeIndexPrices as SubscribeIndexPrices, SubscribeInstrument as SubscribeInstrument, SubscribeInstrumentStatus as SubscribeInstrumentStatus, SubscribeInstruments as SubscribeInstruments, SubscribeMarkPrices as SubscribeMarkPrices, SubscribeOptionGreeks as SubscribeOptionGreeks, SubscribeOrderBook as SubscribeOrderBook, SubscribeQuoteTicks as SubscribeQuoteTicks, SubscribeTradeTicks as SubscribeTradeTicks, UnsubscribeBars as UnsubscribeBars, UnsubscribeFundingRates as UnsubscribeFundingRates, UnsubscribeIndexPrices as UnsubscribeIndexPrices, UnsubscribeInstrument as UnsubscribeInstrument, UnsubscribeInstruments as UnsubscribeInstruments, UnsubscribeMarkPrices as UnsubscribeMarkPrices, UnsubscribeOptionGreeks as UnsubscribeOptionGreeks, UnsubscribeOrderBook as UnsubscribeOrderBook, UnsubscribeQuoteTicks as UnsubscribeQuoteTicks, UnsubscribeTradeTicks as UnsubscribeTradeTicks
from nautilus_trader.live.cancellation import DEFAULT_FUTURE_CANCELLATION_TIMEOUT as DEFAULT_FUTURE_CANCELLATION_TIMEOUT, cancel_tasks_with_timeout as cancel_tasks_with_timeout
from nautilus_trader.live.data_client import LiveMarketDataClient as LiveMarketDataClient
from nautilus_trader.model.data import Bar as Bar, DataType as DataType, FundingRateUpdate as FundingRateUpdate, InstrumentStatus as InstrumentStatus, OptionGreeks as OptionGreeks, OrderBookDeltas as OrderBookDeltas, TradeTick as TradeTick, capsule_to_data as capsule_to_data
from nautilus_trader.model.enums import BookType as BookType, book_type_to_str as book_type_to_str
from nautilus_trader.model.identifiers import ClientId as ClientId, InstrumentId as InstrumentId
from nautilus_trader.model.instruments import CryptoFuture as CryptoFuture, CryptoPerpetual as CryptoPerpetual, Instrument as Instrument

class OKXDataClient(LiveMarketDataClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: nautilus_pyo3.OKXHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: OKXInstrumentProvider, config: OKXDataClientConfig, name: str | None) -> None: ...
    @property
    def instrument_provider(self) -> OKXInstrumentProvider: ...
    def unsubscribe_option_greeks(self, command: UnsubscribeOptionGreeks) -> None: ...
