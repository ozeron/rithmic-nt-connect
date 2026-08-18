import asyncio
from nautilus_trader.adapters.kraken.config import KrakenDataClientConfig as KrakenDataClientConfig, KrakenExecClientConfig as KrakenExecClientConfig
from nautilus_trader.adapters.kraken.data import KrakenDataClient as KrakenDataClient
from nautilus_trader.adapters.kraken.execution import KrakenExecutionClient as KrakenExecutionClient
from nautilus_trader.adapters.kraken.providers import KrakenInstrumentProvider as KrakenInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import KrakenEnvironment as KrakenEnvironment, KrakenProductType as KrakenProductType
from nautilus_trader.live.factories import LiveDataClientFactory as LiveDataClientFactory, LiveExecClientFactory as LiveExecClientFactory

def get_cached_kraken_spot_http_client(api_key: str | None = None, api_secret: str | None = None, base_url: str | None = None, timeout_secs: int | None = None, max_retries: int | None = None, retry_delay_ms: int | None = None, retry_delay_max_ms: int | None = None, proxy_url: str | None = None, max_requests_per_second: int | None = None) -> nautilus_pyo3.KrakenSpotHttpClient: ...
def get_cached_kraken_futures_http_client(api_key: str | None = None, api_secret: str | None = None, base_url: str | None = None, demo: bool = False, timeout_secs: int | None = None, max_retries: int | None = None, retry_delay_ms: int | None = None, retry_delay_max_ms: int | None = None, proxy_url: str | None = None, max_requests_per_second: int | None = None) -> nautilus_pyo3.KrakenFuturesHttpClient: ...
def get_cached_kraken_instrument_provider(http_client_spot: nautilus_pyo3.KrakenSpotHttpClient | None, http_client_futures: nautilus_pyo3.KrakenFuturesHttpClient | None, product_types: tuple[KrakenProductType, ...], config: InstrumentProviderConfig) -> KrakenInstrumentProvider: ...

class KrakenLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str | None, config: KrakenDataClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> KrakenDataClient: ...

class KrakenLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str | None, config: KrakenExecClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> KrakenExecutionClient: ...
