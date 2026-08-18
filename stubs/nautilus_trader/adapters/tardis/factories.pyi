import asyncio
from nautilus_trader.adapters.tardis.config import TardisDataClientConfig as TardisDataClientConfig
from nautilus_trader.adapters.tardis.data import TardisDataClient as TardisDataClient
from nautilus_trader.adapters.tardis.providers import TardisInstrumentProvider as TardisInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core.nautilus_pyo3 import TardisHttpClient as TardisHttpClient
from nautilus_trader.live.factories import LiveDataClientFactory as LiveDataClientFactory

def get_tardis_http_client(api_key: str | None = None, base_url: str | None = None, timeout_secs: int = 60, proxy_url: str | None = None) -> TardisHttpClient: ...
def get_tardis_instrument_provider(client: TardisHttpClient, config: InstrumentProviderConfig) -> TardisInstrumentProvider: ...

class TardisLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: TardisDataClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> TardisDataClient: ...
