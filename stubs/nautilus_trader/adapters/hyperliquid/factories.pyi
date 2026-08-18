import asyncio
from nautilus_trader.adapters.hyperliquid.config import HyperliquidDataClientConfig as HyperliquidDataClientConfig, HyperliquidExecClientConfig as HyperliquidExecClientConfig
from nautilus_trader.adapters.hyperliquid.data import HyperliquidDataClient as HyperliquidDataClient
from nautilus_trader.adapters.hyperliquid.enums import HyperliquidProductType as HyperliquidProductType
from nautilus_trader.adapters.hyperliquid.execution import HyperliquidExecutionClient as HyperliquidExecutionClient
from nautilus_trader.adapters.hyperliquid.providers import HyperliquidInstrumentProvider as HyperliquidInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import HyperliquidEnvironment as HyperliquidEnvironment
from nautilus_trader.live.factories import LiveDataClientFactory as LiveDataClientFactory, LiveExecClientFactory as LiveExecClientFactory

def get_cached_hyperliquid_http_client(private_key: str | None = None, vault_address: str | None = None, account_address: str | None = None, timeout_secs: int | None = None, environment: HyperliquidEnvironment = ..., proxy_url: str | None = None, normalize_prices: bool = True, include_builder_attribution: bool = True) -> nautilus_pyo3.HyperliquidHttpClient: ...
def get_cached_hyperliquid_instrument_provider(client: nautilus_pyo3.HyperliquidHttpClient, config: InstrumentProviderConfig | None = None, product_types: tuple[HyperliquidProductType, ...] | None = None) -> HyperliquidInstrumentProvider: ...

class HyperliquidLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: HyperliquidDataClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> HyperliquidDataClient: ...

class HyperliquidLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: HyperliquidExecClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> HyperliquidExecutionClient: ...
