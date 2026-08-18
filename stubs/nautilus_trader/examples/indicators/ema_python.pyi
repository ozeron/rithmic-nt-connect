from _typeshed import Incomplete
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.indicators import Indicator as Indicator
from nautilus_trader.model.data import Bar as Bar, QuoteTick as QuoteTick, TradeTick as TradeTick
from nautilus_trader.model.enums import PriceType as PriceType

class PyExponentialMovingAverage(Indicator):
    period: Incomplete
    price_type: Incomplete
    alpha: Incomplete
    value: float
    count: int
    def __init__(self, period: int, price_type: PriceType = ...) -> None: ...
    def handle_quote_tick(self, tick: QuoteTick): ...
    def handle_trade_tick(self, tick: TradeTick): ...
    def handle_bar(self, bar: Bar): ...
    def update_raw(self, value: float): ...
