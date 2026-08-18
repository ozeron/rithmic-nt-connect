import asyncio
from nautilus_trader.adapters.dydx.config import DydxDataClientConfig as DydxDataClientConfig, DydxExecClientConfig as DydxExecClientConfig
from nautilus_trader.adapters.dydx.data import DydxDataClient as DydxDataClient
from nautilus_trader.adapters.dydx.execution import DydxExecutionClient as DydxExecutionClient
from nautilus_trader.adapters.dydx.providers import DydxInstrumentProvider as DydxInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import DydxNetwork as DydxNetwork
from nautilus_trader.live.factories import LiveDataClientFactory as LiveDataClientFactory, LiveExecClientFactory as LiveExecClientFactory

def get_cached_dydx_http_client(base_url: str | None = None, network: DydxNetwork = ..., proxy_url: str | None = None) -> nautilus_pyo3.DydxHttpClient: ...
def get_cached_dydx_instrument_provider(client: nautilus_pyo3.DydxHttpClient, config: InstrumentProviderConfig | None = None) -> DydxInstrumentProvider: ...

class DydxLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: DydxDataClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> DydxDataClient: ...

class DydxLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: DydxExecClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> DydxExecutionClient: ...
