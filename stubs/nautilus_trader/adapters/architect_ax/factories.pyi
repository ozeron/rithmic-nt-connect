import asyncio
from nautilus_trader.adapters.architect_ax.config import AxDataClientConfig as AxDataClientConfig, AxExecClientConfig as AxExecClientConfig
from nautilus_trader.adapters.architect_ax.data import AxDataClient as AxDataClient
from nautilus_trader.adapters.architect_ax.execution import AxExecutionClient as AxExecutionClient
from nautilus_trader.adapters.architect_ax.providers import AxInstrumentProvider as AxInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import AxEnvironment as AxEnvironment
from nautilus_trader.live.factories import LiveDataClientFactory as LiveDataClientFactory, LiveExecClientFactory as LiveExecClientFactory

def get_cached_ax_http_client(api_key: str | None = None, api_secret: str | None = None, base_url: str | None = None, orders_base_url: str | None = None, environment: AxEnvironment = ..., timeout_secs: int | None = None, max_retries: int | None = None, retry_delay_ms: int | None = None, retry_delay_max_ms: int | None = None, proxy_url: str | None = None) -> nautilus_pyo3.AxHttpClient: ...
def get_cached_ax_instrument_provider(client: nautilus_pyo3.AxHttpClient, config: InstrumentProviderConfig | None = None) -> AxInstrumentProvider: ...

class AxLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: AxDataClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> AxDataClient: ...

class AxLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: AxExecClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> AxExecutionClient: ...
