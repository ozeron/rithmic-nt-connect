import asyncio
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType as BinanceAccountType
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig as BinanceDataClientConfig
from nautilus_trader.adapters.binance.data import BinanceCommonDataClient as BinanceCommonDataClient
from nautilus_trader.adapters.binance.futures.enums import BinanceFuturesEnumParser as BinanceFuturesEnumParser
from nautilus_trader.adapters.binance.futures.http.market import BinanceFuturesMarketHttpAPI as BinanceFuturesMarketHttpAPI
from nautilus_trader.adapters.binance.futures.schemas.market import BinanceFuturesMarkPriceAllMsg as BinanceFuturesMarkPriceAllMsg, BinanceFuturesMarkPriceData as BinanceFuturesMarkPriceData, BinanceFuturesMarkPriceMsg as BinanceFuturesMarkPriceMsg
from nautilus_trader.adapters.binance.futures.types import BinanceFuturesMarkPriceUpdate as BinanceFuturesMarkPriceUpdate
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient as BinanceHttpClient
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.providers import InstrumentProvider as InstrumentProvider
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.model.data import CustomData as CustomData, DataType as DataType, MarkPriceUpdate as MarkPriceUpdate, OrderBookDelta as OrderBookDelta, OrderBookDeltas as OrderBookDeltas
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId

class BinanceFuturesDataClient(BinanceCommonDataClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: BinanceHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: InstrumentProvider, base_url_ws: str, config: BinanceDataClientConfig, account_type: BinanceAccountType = ..., name: str | None = None, base_url_ws_public: str | None = None) -> None: ...
