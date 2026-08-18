import asyncio
from nautilus_trader.adapters.sandbox.config import SandboxExecutionClientConfig as SandboxExecutionClientConfig
from nautilus_trader.adapters.sandbox.execution import SandboxExecutionClient as SandboxExecutionClient
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.live.factories import LiveExecClientFactory as LiveExecClientFactory
from nautilus_trader.portfolio import PortfolioFacade as PortfolioFacade

class SandboxLiveExecClientFactory(LiveExecClientFactory):
    @staticmethod
    def create(loop: asyncio.AbstractEventLoop, name: str, config: SandboxExecutionClientConfig, portfolio: PortfolioFacade, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> SandboxExecutionClient: ...
