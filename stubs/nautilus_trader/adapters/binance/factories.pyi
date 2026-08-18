import asyncio
from nautilus_trader.adapters.binance.common.constants import BINANCE_FUTURES_ORDER_COUNT_10S_KEY as BINANCE_FUTURES_ORDER_COUNT_10S_KEY, BINANCE_FUTURES_ORDER_COUNT_1M_KEY as BINANCE_FUTURES_ORDER_COUNT_1M_KEY
from nautilus_trader.adapters.binance.common.credentials import get_api_key as get_api_key, get_api_secret as get_api_secret, is_ed25519_private_key as is_ed25519_private_key
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType as BinanceAccountType, BinanceEnvironment as BinanceEnvironment, BinanceKeyType as BinanceKeyType
from nautilus_trader.adapters.binance.common.urls import get_http_base_url as get_http_base_url, get_usdm_ws_route_base_url as get_usdm_ws_route_base_url, get_ws_base_url as get_ws_base_url, get_ws_public_base_url as get_ws_public_base_url
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig as BinanceDataClientConfig, BinanceExecClientConfig as BinanceExecClientConfig, BinanceInstrumentProviderConfig as BinanceInstrumentProviderConfig
from nautilus_trader.adapters.binance.futures.data import BinanceFuturesDataClient as BinanceFuturesDataClient
from nautilus_trader.adapters.binance.futures.execution import BinanceFuturesExecutionClient as BinanceFuturesExecutionClient
from nautilus_trader.adapters.binance.futures.providers import BinanceFuturesInstrumentProvider as BinanceFuturesInstrumentProvider
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient as BinanceHttpClient
from nautilus_trader.adapters.binance.spot.data import BinanceSpotDataClient as BinanceSpotDataClient
from nautilus_trader.adapters.binance.spot.execution import BinanceSpotExecutionClient as BinanceSpotExecutionClient
from nautilus_trader.adapters.binance.spot.providers import BinanceSpotInstrumentProvider as BinanceSpotInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core.nautilus_pyo3 import Quota as Quota
from nautilus_trader.live.factories import LiveDataClientFactory as LiveDataClientFactory, LiveExecClientFactory as LiveExecClientFactory
from nautilus_trader.model.identifiers import Venue as Venue

def get_cached_binance_http_client(clock: LiveClock, account_type: BinanceAccountType, api_key: str | None = None, api_secret: str | None = None, key_type: BinanceKeyType = ..., base_url: str | None = None, environment: BinanceEnvironment = ..., is_us: bool = False, proxy_url: str | None = None) -> BinanceHttpClient: ...
def get_cached_binance_spot_instrument_provider(client: BinanceHttpClient, clock: LiveClock, account_type: BinanceAccountType, environment: BinanceEnvironment, config: InstrumentProviderConfig, venue: Venue) -> BinanceSpotInstrumentProvider: ...
def get_cached_binance_futures_instrument_provider(client: BinanceHttpClient, clock: LiveClock, account_type: BinanceAccountType, config: InstrumentProviderConfig | BinanceInstrumentProviderConfig, venue: Venue) -> BinanceFuturesInstrumentProvider: ...

class BinanceLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: BinanceDataClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> BinanceSpotDataClient | BinanceFuturesDataClient: ...

class BinanceLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: BinanceExecClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> BinanceSpotExecutionClient | BinanceFuturesExecutionClient: ...
