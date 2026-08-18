from _typeshed import Incomplete
from decimal import Decimal
from nautilus_trader.core.message import Event as Event
from nautilus_trader.model.book import OrderBook as OrderBook
from nautilus_trader.model.data import OrderBookDeltas as OrderBookDeltas
from nautilus_trader.model.enums import BookType as BookType, OrderSide as OrderSide, PositionSide as PositionSide
from nautilus_trader.model.events import PositionChanged as PositionChanged, PositionClosed as PositionClosed, PositionOpened as PositionOpened
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId
from nautilus_trader.model.instruments import Instrument as Instrument
from nautilus_trader.model.objects import Price as Price
from nautilus_trader.trading.strategy import Strategy as Strategy

class MarketMaker(Strategy):
    instrument_id: Incomplete
    trade_size: Incomplete
    max_size: Incomplete
    instrument: Instrument | None
    def __init__(self, instrument_id: InstrumentId, trade_size: Decimal, max_size: Decimal) -> None: ...
    def on_start(self) -> None: ...
    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None: ...
    def on_event(self, event: Event) -> None: ...
    def buy(self, price: Decimal) -> None: ...
    def sell(self, price: Decimal) -> None: ...
    def on_stop(self) -> None: ...
