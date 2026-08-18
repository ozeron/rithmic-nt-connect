import msgspec
from nautilus_trader.adapters.binance.common.enums import BinanceOrderType as BinanceOrderType
from nautilus_trader.adapters.binance.common.schemas.market import BinanceExchangeFilter as BinanceExchangeFilter, BinanceOrderBookDelta as BinanceOrderBookDelta, BinanceRateLimit as BinanceRateLimit, BinanceSymbolFilter as BinanceSymbolFilter
from nautilus_trader.core.datetime import millis_to_nanos as millis_to_nanos
from nautilus_trader.model.data import OrderBookDelta as OrderBookDelta, OrderBookDeltas as OrderBookDeltas, TradeTick as TradeTick
from nautilus_trader.model.enums import AggressorSide as AggressorSide, CurrencyType as CurrencyType, OrderSide as OrderSide, RecordFlag as RecordFlag
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId, TradeId as TradeId
from nautilus_trader.model.objects import Currency as Currency, Price as Price, Quantity as Quantity

class BinanceSpotSymbolInfo(msgspec.Struct, frozen=True):
    symbol: str
    status: str
    baseAsset: str
    baseAssetPrecision: int
    quoteAsset: str
    quotePrecision: int
    quoteAssetPrecision: int
    orderTypes: list[BinanceOrderType]
    icebergAllowed: bool
    ocoAllowed: bool
    quoteOrderQtyMarketAllowed: bool
    allowTrailingStop: bool
    isSpotTradingAllowed: bool
    isMarginTradingAllowed: bool
    filters: list[BinanceSymbolFilter]
    permissions: list[str]
    def parse_to_base_asset(self): ...
    def parse_to_quote_asset(self): ...

class BinanceSpotExchangeInfo(msgspec.Struct, frozen=True):
    timezone: str
    serverTime: int
    rateLimits: list[BinanceRateLimit]
    exchangeFilters: list[BinanceExchangeFilter]
    symbols: list[BinanceSpotSymbolInfo]

class BinanceSpotAvgPrice(msgspec.Struct, frozen=True):
    mins: int
    price: str

class BinanceSpotOrderBookPartialDepthData(msgspec.Struct):
    lastUpdateId: int
    bids: list[BinanceOrderBookDelta]
    asks: list[BinanceOrderBookDelta]
    def parse_to_order_book_snapshot(self, instrument_id: InstrumentId, ts_init: int) -> OrderBookDeltas: ...

class BinanceSpotOrderBookPartialDepthMsg(msgspec.Struct):
    stream: str
    data: BinanceSpotOrderBookPartialDepthData

class BinanceSpotTradeData(msgspec.Struct):
    e: str
    E: int
    s: str
    t: int
    p: str
    q: str
    T: int
    m: bool
    def parse_to_trade_tick(self, instrument_id: InstrumentId, ts_init: int | None = None) -> TradeTick: ...

class BinanceSpotTradeMsg(msgspec.Struct):
    stream: str
    data: BinanceSpotTradeData
