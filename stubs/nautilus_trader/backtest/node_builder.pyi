from nautilus_trader.backtest.engine import BacktestEngine as BacktestEngine
from nautilus_trader.common.component import Logger as Logger
from nautilus_trader.config import ImportableConfig as ImportableConfig
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.live.config import LiveDataClientConfig as LiveDataClientConfig
from nautilus_trader.live.factories import LiveDataClientFactory as LiveDataClientFactory
from nautilus_trader.model.identifiers import Venue as Venue

class BacktestNodeBuilder:
    def __init__(self, engine: BacktestEngine, logger: Logger) -> None: ...
    def add_data_client_factory(self, name: str, factory: type[LiveDataClientFactory]) -> None: ...
    def build_data_clients(self, config: dict[str, type[LiveDataClientConfig]]) -> None: ...
