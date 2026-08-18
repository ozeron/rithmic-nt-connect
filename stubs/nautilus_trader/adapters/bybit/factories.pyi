import asyncio
from nautilus_trader.adapters.bybit.config import BybitDataClientConfig as BybitDataClientConfig, BybitExecClientConfig as BybitExecClientConfig
from nautilus_trader.adapters.bybit.constants import BYBIT_ALL_PRODUCTS as BYBIT_ALL_PRODUCTS
from nautilus_trader.adapters.bybit.data import BybitDataClient as BybitDataClient
from nautilus_trader.adapters.bybit.execution import BybitExecutionClient as BybitExecutionClient
from nautilus_trader.adapters.bybit.providers import BybitInstrumentProvider as BybitInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import BybitEnvironment as BybitEnvironment, BybitProductType as BybitProductType
from nautilus_trader.live.factories import LiveDataClientFactory as LiveDataClientFactory, LiveExecClientFactory as LiveExecClientFactory

def get_cached_bybit_http_client(environment: BybitEnvironment = ..., api_key: str | None = None, api_secret: str | None = None, base_url: str | None = None, timeout_secs: int | None = None, max_retries: int | None = None, retry_delay_ms: int | None = None, retry_delay_max_ms: int | None = None, recv_window_ms: int | None = None, proxy_url: str | None = None) -> nautilus_pyo3.BybitHttpClient: ...
def get_cached_bybit_instrument_provider(client: nautilus_pyo3.BybitHttpClient, product_types: tuple[BybitProductType, ...], config: InstrumentProviderConfig | None = None) -> BybitInstrumentProvider: ...

class BybitLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: BybitDataClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> BybitDataClient: ...

class BybitLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: BybitExecClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> BybitExecutionClient: ...
