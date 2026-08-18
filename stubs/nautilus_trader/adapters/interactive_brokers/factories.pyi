import asyncio
from nautilus_trader.adapters.interactive_brokers.client import InteractiveBrokersClient as InteractiveBrokersClient
from nautilus_trader.adapters.interactive_brokers.common import IB_VENUE as IB_VENUE
from nautilus_trader.adapters.interactive_brokers.config import DockerizedIBGatewayConfig as DockerizedIBGatewayConfig, InteractiveBrokersDataClientConfig as InteractiveBrokersDataClientConfig, InteractiveBrokersExecClientConfig as InteractiveBrokersExecClientConfig, InteractiveBrokersInstrumentProviderConfig as InteractiveBrokersInstrumentProviderConfig
from nautilus_trader.adapters.interactive_brokers.data import InteractiveBrokersDataClient as InteractiveBrokersDataClient
from nautilus_trader.adapters.interactive_brokers.execution import InteractiveBrokersExecutionClient as InteractiveBrokersExecutionClient
from nautilus_trader.adapters.interactive_brokers.gateway import DockerizedIBGateway as DockerizedIBGateway
from nautilus_trader.adapters.interactive_brokers.providers import InteractiveBrokersInstrumentProvider as InteractiveBrokersInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.live.factories import LiveDataClientFactory as LiveDataClientFactory, LiveExecClientFactory as LiveExecClientFactory
from nautilus_trader.model.identifiers import AccountId as AccountId

GATEWAYS: dict[tuple, DockerizedIBGateway]
IB_CLIENTS: dict[tuple, InteractiveBrokersClient]
IB_INSTRUMENT_PROVIDERS: dict[tuple, InteractiveBrokersInstrumentProvider]

def get_cached_ib_client(loop: asyncio.AbstractEventLoop, msgbus: MessageBus, cache: Cache, clock: LiveClock, host: str = '127.0.0.1', port: int | None = None, client_id: int = 1, dockerized_gateway: DockerizedIBGatewayConfig | None = None, fetch_all_open_orders: bool = False, request_timeout_secs: int = 60) -> InteractiveBrokersClient: ...
def get_cached_interactive_brokers_instrument_provider(client: InteractiveBrokersClient, clock: LiveClock, config: InteractiveBrokersInstrumentProviderConfig) -> InteractiveBrokersInstrumentProvider: ...

class InteractiveBrokersLiveDataClientFactory(LiveDataClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: InteractiveBrokersDataClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> InteractiveBrokersDataClient: ...

class InteractiveBrokersLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: InteractiveBrokersExecClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> InteractiveBrokersExecutionClient: ...
