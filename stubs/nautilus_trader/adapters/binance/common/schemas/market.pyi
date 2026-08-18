import msgspec
from nautilus_trader.adapters.binance.common.enums import BinanceEnumParser as BinanceEnumParser, BinanceExchangeFilterType as BinanceExchangeFilterType, BinanceKlineInterval as BinanceKlineInterval, BinanceRateLimitInterval as BinanceRateLimitInterval, BinanceRateLimitType as BinanceRateLimitType, BinanceSymbolFilterType as BinanceSymbolFilterType
from nautilus_trader.adapters.binance.common.types import BinanceBar as BinanceBar, BinanceTicker as BinanceTicker
from nautilus_trader.core.datetime import millis_to_nanos as millis_to_nanos
from nautilus_trader.model.data import BarType as BarType, BookOrder as BookOrder, OrderBookDelta as OrderBookDelta, OrderBookDeltas as OrderBookDeltas, QuoteTick as QuoteTick, TradeTick as TradeTick
from nautilus_trader.model.enums import AggregationSource as AggregationSource, AggressorSide as AggressorSide, BookAction as BookAction, OrderSide as OrderSide, RecordFlag as RecordFlag
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId, TradeId as TradeId
from nautilus_trader.model.objects import Price as Price, Quantity as Quantity

class BinanceTime(msgspec.Struct, frozen=True):
    serverTime: int

class BinanceExchangeFilter(msgspec.Struct):
    filterType: BinanceExchangeFilterType
    maxNumOrders: int | None = ...
    maxNumAlgoOrders: int | None = ...

class BinanceRateLimit(msgspec.Struct):
    rateLimitType: BinanceRateLimitType
    interval: BinanceRateLimitInterval
    intervalNum: int
    limit: int
    count: int | None = ...

class BinanceSymbolFilter(msgspec.Struct):
    filterType: BinanceSymbolFilterType
    minPrice: str | None = ...
    maxPrice: str | None = ...
    tickSize: str | None = ...
    multiplierUp: str | None = ...
    multiplierDown: str | None = ...
    multiplierDecimal: str | None = ...
    avgPriceMins: int | None = ...
    minQty: str | None = ...
    maxQty: str | None = ...
    stepSize: str | None = ...
    limit: int | None = ...
    maxNumOrders: int | None = ...
    notional: str | None = ...
    minNotional: str | None = ...
    maxNumAlgoOrders: int | None = ...
    bidMultiplierUp: str | None = ...
    bidMultiplierDown: str | None = ...
    askMultiplierUp: str | None = ...
    askMultiplierDown: str | None = ...
    applyMinToMarket: bool | None = ...
    maxNotional: str | None = ...
    applyMaxToMarket: bool | None = ...
    maxNumIcebergOrders: int | None = ...
    maxPosition: str | None = ...
    minTrailingAboveDelta: int | None = ...
    maxTrailingAboveDelta: int | None = ...
    minTrailingBelowDelta: int | None = ...
    maxTrailingBelowDelta: int | None = ...

class BinanceDepth(msgspec.Struct, frozen=True):
    lastUpdateId: int
    bids: list[tuple[str, str]]
    asks: list[tuple[str, str]]
    symbol: str | None = ...
    pair: str | None = ...
    E: int | None = ...
    T: int | None = ...
    def parse_to_order_book_snapshot(self, instrument_id: InstrumentId, ts_init: int) -> OrderBookDeltas: ...

class BinanceTrade(msgspec.Struct, frozen=True):
    id: int
    price: str
    qty: str
    quoteQty: str
    time: int
    isBuyerMaker: bool
    isBestMatch: bool | None = ...
    def parse_to_trade_tick(self, instrument_id: InstrumentId, ts_init: int | None = None) -> TradeTick: ...

class BinanceAggTrade(msgspec.Struct, frozen=True):
    a: int
    p: str
    q: str
    f: int
    l: int
    T: int
    m: bool
    M: bool | None = ...
    def parse_to_trade_tick(self, instrument_id: InstrumentId, ts_init: int | None = None) -> TradeTick: ...

class BinanceKline(msgspec.Struct, array_like=True):
    open_time: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    close_time: int
    asset_volume: str
    trades_count: int
    taker_base_volume: str
    taker_quote_volume: str
    ignore: str
    def parse_to_binance_bar(self, bar_type: BarType, ts_init: int | None = None) -> BinanceBar: ...

class BinanceTicker24hr(msgspec.Struct, frozen=True):
    symbol: str | None
    lastPrice: str | None
    openPrice: str | None
    highPrice: str | None
    lowPrice: str | None
    volume: str | None
    openTime: int | None
    closeTime: int | None
    firstId: int | None
    lastId: int | None
    count: int | None
    priceChange: str | None = ...
    priceChangePercent: str | None = ...
    weightedAvgPrice: str | None = ...
    lastQty: str | None = ...
    prevClosePrice: str | None = ...
    bidPrice: str | None = ...
    bidQty: str | None = ...
    askPrice: str | None = ...
    askQty: str | None = ...
    pair: str | None = ...
    baseVolume: str | None = ...
    quoteVolume: str | None = ...

class BinanceTickerPrice(msgspec.Struct, frozen=True):
    symbol: str | None
    price: str | None
    time: int | None = ...
    pair: str | None = ...
    ps: str | None = ...

class BinanceTickerBook(msgspec.Struct, frozen=True):
    symbol: str | None
    bidPrice: str | None
    bidQty: str | None
    askPrice: str | None
    askQty: str | None
    pair: str | None = ...
    time: int | None = ...

class BinanceDataMsgWrapper(msgspec.Struct):
    stream: str | None = ...
    id: int | None = ...

class BinanceOrderBookDelta(msgspec.Struct, array_like=True):
    price: str
    size: str
    def parse_to_order_book_delta(self, instrument_id: InstrumentId, side: OrderSide, flags: int, sequence: int, ts_event: int, ts_init: int) -> OrderBookDelta: ...

class BinanceOrderBookData(msgspec.Struct, frozen=True):
    e: str
    E: int
    s: str
    U: int
    u: int
    b: list[BinanceOrderBookDelta]
    a: list[BinanceOrderBookDelta]
    T: int | None = ...
    pu: int | None = ...
    ps: str | None = ...
    def parse_to_order_book_deltas(self, instrument_id: InstrumentId, ts_init: int, snapshot: bool = False) -> OrderBookDeltas: ...

class BinanceOrderBookMsg(msgspec.Struct, frozen=True):
    stream: str
    data: BinanceOrderBookData

class BinanceQuoteData(msgspec.Struct, frozen=True):
    s: str
    u: int
    b: str
    B: str
    a: str
    A: str
    T: int | None = ...
    def parse_to_quote_tick(self, instrument_id: InstrumentId, ts_init: int | None = None) -> QuoteTick: ...

class BinanceQuoteMsg(msgspec.Struct, frozen=True):
    stream: str
    data: BinanceQuoteData

class BinanceAggregatedTradeData(msgspec.Struct, frozen=True):
    e: str
    E: int
    s: str
    a: int
    p: str
    q: str
    f: int
    l: int
    T: int
    m: bool
    def parse_to_trade_tick(self, instrument_id: InstrumentId, ts_init: int | None = None) -> TradeTick: ...

class BinanceAggregatedTradeMsg(msgspec.Struct, frozen=True):
    stream: str
    data: BinanceAggregatedTradeData

class BinanceTickerData(msgspec.Struct, kw_only=True, frozen=True):
    e: str
    E: int
    s: str
    p: str
    P: str
    w: str
    x: str | None = ...
    c: str
    Q: str
    b: str | None = ...
    B: str | None = ...
    a: str | None = ...
    A: str | None = ...
    o: str
    h: str
    l: str
    v: str
    q: str
    O: int
    C: int
    F: int
    L: int
    n: int
    def parse_to_binance_ticker(self, instrument_id: InstrumentId, ts_init: int) -> BinanceTicker: ...

class BinanceTickerMsg(msgspec.Struct, frozen=True):
    stream: str
    data: BinanceTickerData

class BinanceCandlestick(msgspec.Struct, frozen=True):
    t: int
    T: int
    s: str
    i: BinanceKlineInterval
    f: int
    L: int
    o: str
    c: str
    h: str
    l: str
    v: str
    n: int
    x: bool
    q: str
    V: str
    Q: str
    B: str
    def parse_to_binance_bar(self, instrument_id: InstrumentId, enum_parser: BinanceEnumParser, ts_init: int | None = None) -> BinanceBar: ...

class BinanceCandlestickData(msgspec.Struct, frozen=True):
    e: str
    E: int
    s: str
    k: BinanceCandlestick

class BinanceCandlestickMsg(msgspec.Struct, frozen=True):
    stream: str
    data: BinanceCandlestickData
