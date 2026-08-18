import msgspec
from nautilus_trader.adapters.binance.common.enums import BinanceOrderType as BinanceOrderType, BinanceTimeInForce as BinanceTimeInForce
from nautilus_trader.adapters.binance.common.schemas.market import BinanceExchangeFilter as BinanceExchangeFilter, BinanceRateLimit as BinanceRateLimit, BinanceSymbolFilter as BinanceSymbolFilter
from nautilus_trader.adapters.binance.futures.enums import BinanceFuturesContractStatus as BinanceFuturesContractStatus
from nautilus_trader.adapters.binance.futures.types import BinanceFuturesMarkPriceUpdate as BinanceFuturesMarkPriceUpdate
from nautilus_trader.core.datetime import millis_to_nanos as millis_to_nanos
from nautilus_trader.model.data import TradeTick as TradeTick
from nautilus_trader.model.enums import AggressorSide as AggressorSide, CurrencyType as CurrencyType
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId, TradeId as TradeId
from nautilus_trader.model.objects import Currency as Currency, Price as Price, Quantity as Quantity

class BinanceFuturesAsset(msgspec.Struct, frozen=True):
    asset: str
    marginAvailable: bool
    autoAssetExchange: str

class BinanceFuturesSymbolInfo(msgspec.Struct, kw_only=True, frozen=True):
    symbol: str
    pair: str
    contractType: str
    deliveryDate: int
    onboardDate: int
    status: BinanceFuturesContractStatus | None = ...
    maintMarginPercent: str
    requiredMarginPercent: str
    baseAsset: str
    quoteAsset: str
    marginAsset: str
    pricePrecision: int
    quantityPrecision: int
    baseAssetPrecision: int
    quotePrecision: int
    underlyingType: str
    underlyingSubType: list[str]
    settlePlan: int | None = ...
    triggerProtect: str
    liquidationFee: str
    marketTakeBound: str
    filters: list[BinanceSymbolFilter]
    orderTypes: list[BinanceOrderType]
    timeInForce: list[BinanceTimeInForce]
    def parse_to_base_currency(self): ...
    def parse_to_quote_currency(self): ...

class BinanceFuturesExchangeInfo(msgspec.Struct, kw_only=True, frozen=True):
    timezone: str
    serverTime: int
    rateLimits: list[BinanceRateLimit]
    exchangeFilters: list[BinanceExchangeFilter]
    assets: list[BinanceFuturesAsset] | None = ...
    symbols: list[BinanceFuturesSymbolInfo]

class BinanceFuturesMarkFunding(msgspec.Struct, frozen=True):
    symbol: str
    markPrice: str
    indexPrice: str
    estimatedSettlePrice: str
    lastFundingRate: str
    nextFundingTime: int
    interestRate: str
    time: int

class BinanceFuturesFundRate(msgspec.Struct, frozen=True):
    symbol: str
    fundingRate: str
    fundingTime: str

class BinanceFuturesTradeData(msgspec.Struct, frozen=True):
    e: str
    E: int
    s: str
    t: int
    p: str
    q: str
    T: int
    m: bool
    def parse_to_trade_tick(self, instrument_id: InstrumentId, ts_init: int) -> TradeTick: ...

class BinanceFuturesTradeMsg(msgspec.Struct, frozen=True):
    stream: str
    data: BinanceFuturesTradeData

class BinanceFuturesMarkPriceData(msgspec.Struct, frozen=True):
    e: str
    E: int
    s: str
    p: str
    i: str
    P: str
    r: str
    T: int
    def parse_to_binance_futures_mark_price_update(self, instrument_id: InstrumentId, ts_init: int) -> BinanceFuturesMarkPriceUpdate: ...

class BinanceFuturesMarkPriceMsg(msgspec.Struct, frozen=True):
    stream: str
    data: BinanceFuturesMarkPriceData

class BinanceFuturesMarkPriceAllMsg(msgspec.Struct, frozen=True):
    stream: str
    data: list[BinanceFuturesMarkPriceData]
