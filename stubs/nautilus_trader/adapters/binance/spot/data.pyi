import asyncio
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType as BinanceAccountType
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig as BinanceDataClientConfig
from nautilus_trader.adapters.binance.data import BinanceCommonDataClient as BinanceCommonDataClient
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient as BinanceHttpClient
from nautilus_trader.adapters.binance.spot.enums import BinanceSpotEnumParser as BinanceSpotEnumParser
from nautilus_trader.adapters.binance.spot.http.market import BinanceSpotMarketHttpAPI as BinanceSpotMarketHttpAPI
from nautilus_trader.adapters.binance.spot.schemas.market import BinanceSpotOrderBookPartialDepthMsg as BinanceSpotOrderBookPartialDepthMsg, BinanceSpotTradeMsg as BinanceSpotTradeMsg
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.providers import InstrumentProvider as InstrumentProvider
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.model.data import OrderBookDelta as OrderBookDelta, OrderBookDeltas as OrderBookDeltas, TradeTick as TradeTick
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId

class BinanceSpotDataClient(BinanceCommonDataClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: BinanceHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: InstrumentProvider, base_url_ws: str, config: BinanceDataClientConfig, account_type: BinanceAccountType = ..., name: str | None = None) -> None: ...
