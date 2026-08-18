import asyncio
from nautilus_trader.adapters.architect_ax.config import AxDataClientConfig as AxDataClientConfig
from nautilus_trader.adapters.architect_ax.constants import AX_AUTH_TOKEN_REFRESH_INTERVAL_SECS as AX_AUTH_TOKEN_REFRESH_INTERVAL_SECS, AX_AUTH_TOKEN_REFRESH_RETRY_SECS as AX_AUTH_TOKEN_REFRESH_RETRY_SECS, AX_AUTH_TOKEN_REQUEST_TIMEOUT_SECS as AX_AUTH_TOKEN_REQUEST_TIMEOUT_SECS, AX_AUTH_TOKEN_TTL_SECS as AX_AUTH_TOKEN_TTL_SECS, AX_VENUE as AX_VENUE
from nautilus_trader.adapters.architect_ax.providers import AxInstrumentProvider as AxInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.datetime import ensure_pydatetime_utc as ensure_pydatetime_utc
from nautilus_trader.data.messages import RequestBars as RequestBars, RequestFundingRates as RequestFundingRates, RequestInstruments as RequestInstruments, RequestOrderBookSnapshot as RequestOrderBookSnapshot, RequestQuoteTicks as RequestQuoteTicks, RequestTradeTicks as RequestTradeTicks, SubscribeBars as SubscribeBars, SubscribeFundingRates as SubscribeFundingRates, SubscribeInstrumentStatus as SubscribeInstrumentStatus, SubscribeMarkPrices as SubscribeMarkPrices, SubscribeOrderBook as SubscribeOrderBook, SubscribeQuoteTicks as SubscribeQuoteTicks, SubscribeTradeTicks as SubscribeTradeTicks, UnsubscribeBars as UnsubscribeBars, UnsubscribeFundingRates as UnsubscribeFundingRates, UnsubscribeInstrumentStatus as UnsubscribeInstrumentStatus, UnsubscribeMarkPrices as UnsubscribeMarkPrices, UnsubscribeOrderBook as UnsubscribeOrderBook, UnsubscribeQuoteTicks as UnsubscribeQuoteTicks, UnsubscribeTradeTicks as UnsubscribeTradeTicks
from nautilus_trader.live.data_client import LiveMarketDataClient as LiveMarketDataClient
from nautilus_trader.model.data import Bar as Bar, BookOrder as BookOrder, DataType as DataType, FundingRateUpdate as FundingRateUpdate, InstrumentStatus as InstrumentStatus, OrderBookDelta as OrderBookDelta, OrderBookDeltas as OrderBookDeltas, TradeTick as TradeTick, capsule_to_data as capsule_to_data
from nautilus_trader.model.enums import BookAction as BookAction, BookType as BookType, OrderSide as OrderSide, RecordFlag as RecordFlag, book_type_to_str as book_type_to_str
from nautilus_trader.model.identifiers import ClientId as ClientId, InstrumentId as InstrumentId

class AxDataClient(LiveMarketDataClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: nautilus_pyo3.AxHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: AxInstrumentProvider, config: AxDataClientConfig, name: str | None) -> None: ...
    @property
    def instrument_provider(self) -> AxInstrumentProvider: ...
