import asyncio
from nautilus_trader.adapters.deribit.config import DeribitDataClientConfig as DeribitDataClientConfig, DeribitExecClientConfig as DeribitExecClientConfig
from nautilus_trader.adapters.deribit.data import DeribitDataClient as DeribitDataClient
from nautilus_trader.adapters.deribit.execution import DeribitExecutionClient as DeribitExecutionClient
from nautilus_trader.adapters.deribit.providers import DeribitInstrumentProvider as DeribitInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import DeribitEnvironment as DeribitEnvironment, DeribitProductType as DeribitProductType
from nautilus_trader.live.factories import LiveDataClientFactory as LiveDataClientFactory, LiveExecClientFactory as LiveExecClientFactory

def get_cached_deribit_http_client(api_key: str | None = None, api_secret: str | None = None, base_url: str | None = None, environment: DeribitEnvironment = ..., timeout_secs: int | None = None, max_retries: int | None = None, retry_delay_ms: int | None = None, retry_delay_max_ms: int | None = None, proxy_url: str | None = None) -> nautilus_pyo3.DeribitHttpClient: ...
def get_cached_deribit_instrument_provider(client: nautilus_pyo3.DeribitHttpClient, product_types: tuple[DeribitProductType, ...] | None = None, config: InstrumentProviderConfig | None = None) -> DeribitInstrumentProvider: ...

class DeribitLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: DeribitDataClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> DeribitDataClient: ...

class DeribitLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: DeribitExecClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> DeribitExecutionClient: ...
