import asyncio
from nautilus_trader.adapters.databento.config import DatabentoDataClientConfig as DatabentoDataClientConfig
from nautilus_trader.adapters.databento.constants import PUBLISHERS_FILEPATH as PUBLISHERS_FILEPATH
from nautilus_trader.adapters.databento.data import DatabentoDataClient as DatabentoDataClient
from nautilus_trader.adapters.databento.loaders import DatabentoDataLoader as DatabentoDataLoader
from nautilus_trader.adapters.databento.providers import DatabentoInstrumentProvider as DatabentoInstrumentProvider
from nautilus_trader.adapters.env import get_env_key as get_env_key
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.live.factories import LiveDataClientFactory as LiveDataClientFactory

def get_cached_databento_http_client(key: str | None = None, gateway: str | None = None, use_exchange_as_venue: bool = True) -> nautilus_pyo3.DatabentoHistoricalClient: ...
def get_cached_databento_instrument_provider(http_client: nautilus_pyo3.DatabentoHistoricalClient, clock: LiveClock, live_api_key: str | None = None, live_gateway: str | None = None, loader: DatabentoDataLoader | None = None, config: InstrumentProviderConfig | None = None, use_exchange_as_venue: bool = True) -> DatabentoInstrumentProvider: ...

class DatabentoLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: DatabentoDataClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> DatabentoDataClient: ...
