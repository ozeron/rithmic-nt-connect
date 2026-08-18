import asyncio
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.config import LiveDataClientConfig as LiveDataClientConfig, LiveExecClientConfig as LiveExecClientConfig
from nautilus_trader.live.data_client import LiveDataClient as LiveDataClient
from nautilus_trader.live.execution_client import LiveExecutionClient as LiveExecutionClient

class LiveDataClientFactory:
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: LiveDataClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> LiveDataClient: ...

class LiveExecClientFactory:
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: LiveExecClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> LiveExecutionClient: ...
