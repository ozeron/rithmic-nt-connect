from nautilus_trader.config import StrategyConfig as StrategyConfig
from nautilus_trader.model.data import QuoteTick as QuoteTick, TradeTick as TradeTick
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId
from nautilus_trader.model.instruments import Instrument as Instrument
from nautilus_trader.trading.strategy import Strategy as Strategy

class SignalStrategyConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId

class SignalStrategy(Strategy):
    instrument: Instrument | None
    counter: int
    def __init__(self, config: SignalStrategyConfig) -> None: ...
    def on_start(self) -> None: ...
    def on_quote_tick(self, tick: QuoteTick) -> None: ...
    def on_trade_tick(self, tick: TradeTick) -> None: ...
