from nautilus_trader.config import ActorConfig as ActorConfig
from nautilus_trader.examples.strategies.signal_strategy import SignalStrategy as SignalStrategy, SignalStrategyConfig as SignalStrategyConfig
from nautilus_trader.trading.config import ImportableStrategyConfig as ImportableStrategyConfig
from nautilus_trader.trading.controller import Controller as Controller

class ControllerConfig(ActorConfig, frozen=True): ...

class MyController(Controller):
    def start(self) -> None: ...
