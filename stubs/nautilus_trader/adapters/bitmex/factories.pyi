import asyncio
from nautilus_trader.adapters.bitmex.config import BitmexDataClientConfig as BitmexDataClientConfig
from nautilus_trader.adapters.bitmex.data import BitmexDataClient as BitmexDataClient
from nautilus_trader.adapters.bitmex.execution import BitmexExecClientConfig as BitmexExecClientConfig, BitmexExecutionClient as BitmexExecutionClient
from nautilus_trader.adapters.bitmex.providers import BitmexInstrumentProvider as BitmexInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import BitmexEnvironment as BitmexEnvironment
from nautilus_trader.live.factories import LiveDataClientFactory as LiveDataClientFactory, LiveExecClientFactory as LiveExecClientFactory

def get_bitmex_http_client(api_key: str | None = None, api_secret: str | None = None, base_url: str | None = None, environment: BitmexEnvironment = ..., timeout_secs: int | None = None, max_retries: int | None = None, retry_delay_ms: int | None = None, retry_delay_max_ms: int | None = None, recv_window_ms: int | None = None, max_requests_per_second: int | None = None, max_requests_per_minute: int | None = None, proxy_url: str | None = None) -> nautilus_pyo3.BitmexHttpClient: ...
def get_bitmex_instrument_provider(client: nautilus_pyo3.BitmexHttpClient, active_only: bool, config: InstrumentProviderConfig) -> BitmexInstrumentProvider: ...

class BitmexLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str | None, config: BitmexDataClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> BitmexDataClient: ...

class BitmexLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str | None, config: BitmexExecClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> BitmexExecutionClient: ...
