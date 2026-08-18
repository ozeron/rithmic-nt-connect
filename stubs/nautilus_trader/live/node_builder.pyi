import asyncio
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, Logger as Logger, MessageBus as MessageBus
from nautilus_trader.config import ImportableConfig as ImportableConfig, LiveDataClientConfig as LiveDataClientConfig, LiveExecClientConfig as LiveExecClientConfig
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.live.data_engine import LiveDataEngine as LiveDataEngine
from nautilus_trader.live.execution_engine import LiveExecutionEngine as LiveExecutionEngine
from nautilus_trader.live.factories import LiveDataClientFactory as LiveDataClientFactory, LiveExecClientFactory as LiveExecClientFactory
from nautilus_trader.model.identifiers import Venue as Venue
from nautilus_trader.portfolio.portfolio import Portfolio as Portfolio

class TradingNodeBuilder:
    def __init__(self, loop: asyncio.AbstractEventLoop, data_engine: LiveDataEngine, exec_engine: LiveExecutionEngine, portfolio: Portfolio, msgbus: MessageBus, cache: Cache, clock: LiveClock, logger: Logger) -> None: ...
    def add_data_client_factory(self, name: str, factory: type[LiveDataClientFactory]) -> None: ...
    def add_exec_client_factory(self, name: str, factory: type[LiveExecClientFactory]) -> None: ...
    def build_data_clients(self, config: dict[str, LiveDataClientConfig]) -> None: ...
    def build_exec_clients(self, config: dict[str, LiveExecClientConfig]) -> None: ...
