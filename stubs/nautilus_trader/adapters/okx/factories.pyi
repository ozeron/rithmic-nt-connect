import asyncio
from nautilus_trader.adapters.okx.config import OKXDataClientConfig as OKXDataClientConfig, OKXExecClientConfig as OKXExecClientConfig
from nautilus_trader.adapters.okx.data import OKXDataClient as OKXDataClient
from nautilus_trader.adapters.okx.execution import OKXExecutionClient as OKXExecutionClient
from nautilus_trader.adapters.okx.providers import OKXInstrumentProvider as OKXInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import OKXContractType as OKXContractType, OKXEnvironment as OKXEnvironment, OKXInstrumentType as OKXInstrumentType, OKXRegion as OKXRegion
from nautilus_trader.live.factories import LiveDataClientFactory as LiveDataClientFactory, LiveExecClientFactory as LiveExecClientFactory

def get_cached_okx_http_client(api_key: str | None = None, api_secret: str | None = None, api_passphrase: str | None = None, base_url: str | None = None, timeout_secs: int | None = None, max_retries: int | None = None, retry_delay_ms: int | None = None, retry_delay_max_ms: int | None = None, environment: OKXEnvironment = ..., proxy_url: str | None = None) -> nautilus_pyo3.OKXHttpClient: ...
def get_cached_okx_instrument_provider(client: nautilus_pyo3.OKXHttpClient, instrument_types: tuple[OKXInstrumentType, ...], contract_types: tuple[OKXContractType, ...] | None = None, instrument_families: tuple[str, ...] | None = None, load_spreads: bool = False, config: InstrumentProviderConfig | None = None) -> OKXInstrumentProvider: ...

class OKXLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: OKXDataClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> OKXDataClient: ...

class OKXLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: OKXExecClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> OKXExecutionClient: ...
