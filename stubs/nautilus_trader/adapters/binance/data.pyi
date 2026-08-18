import asyncio
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType as BinanceAccountType, BinanceEnumParser as BinanceEnumParser, BinanceErrorCode as BinanceErrorCode, BinanceKlineInterval as BinanceKlineInterval
from nautilus_trader.adapters.binance.common.schemas.market import BinanceAggregatedTradeMsg as BinanceAggregatedTradeMsg, BinanceCandlestickMsg as BinanceCandlestickMsg, BinanceDataMsgWrapper as BinanceDataMsgWrapper, BinanceOrderBookMsg as BinanceOrderBookMsg, BinanceQuoteMsg as BinanceQuoteMsg, BinanceTickerMsg as BinanceTickerMsg
from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol as BinanceSymbol
from nautilus_trader.adapters.binance.common.types import BinanceBar as BinanceBar, BinanceTicker as BinanceTicker
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig as BinanceDataClientConfig
from nautilus_trader.adapters.binance.futures.types import BinanceFuturesMarkPriceUpdate as BinanceFuturesMarkPriceUpdate
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient as BinanceHttpClient
from nautilus_trader.adapters.binance.http.error import BinanceError as BinanceError
from nautilus_trader.adapters.binance.http.market import BinanceMarketHttpAPI as BinanceMarketHttpAPI
from nautilus_trader.adapters.binance.websocket.client import BinanceWebSocketClient as BinanceWebSocketClient
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor
from nautilus_trader.common.providers import InstrumentProvider as InstrumentProvider
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.core.datetime import secs_to_millis as secs_to_millis
from nautilus_trader.data.aggregation import BarAggregator as BarAggregator, TickBarAggregator as TickBarAggregator, ValueBarAggregator as ValueBarAggregator, VolumeBarAggregator as VolumeBarAggregator
from nautilus_trader.data.messages import RequestBars as RequestBars, RequestInstrument as RequestInstrument, RequestOrderBookSnapshot as RequestOrderBookSnapshot, RequestQuoteTicks as RequestQuoteTicks, RequestTradeTicks as RequestTradeTicks, SubscribeBars as SubscribeBars, SubscribeData as SubscribeData, SubscribeInstrument as SubscribeInstrument, SubscribeInstruments as SubscribeInstruments, SubscribeMarkPrices as SubscribeMarkPrices, SubscribeOrderBook as SubscribeOrderBook, SubscribeQuoteTicks as SubscribeQuoteTicks, SubscribeTradeTicks as SubscribeTradeTicks, UnsubscribeBars as UnsubscribeBars, UnsubscribeData as UnsubscribeData, UnsubscribeInstrument as UnsubscribeInstrument, UnsubscribeInstruments as UnsubscribeInstruments, UnsubscribeOrderBook as UnsubscribeOrderBook, UnsubscribeQuoteTicks as UnsubscribeQuoteTicks, UnsubscribeTradeTicks as UnsubscribeTradeTicks
from nautilus_trader.live.data_client import LiveMarketDataClient as LiveMarketDataClient
from nautilus_trader.model.data import Bar as Bar, BarSpecification as BarSpecification, BarType as BarType, CustomData as CustomData, DataType as DataType, OrderBookDelta as OrderBookDelta, OrderBookDeltas as OrderBookDeltas, QuoteTick as QuoteTick, TradeTick as TradeTick, bar_aggregation_not_implemented_message as bar_aggregation_not_implemented_message
from nautilus_trader.model.enums import AggregationSource as AggregationSource, AggressorSide as AggressorSide, BarAggregation as BarAggregation, BookType as BookType, PriceType as PriceType
from nautilus_trader.model.identifiers import ClientId as ClientId, InstrumentId as InstrumentId, Symbol as Symbol, TradeId as TradeId
from nautilus_trader.model.instruments import Instrument as Instrument
from nautilus_trader.model.objects import Quantity as Quantity

class BinanceCommonDataClient(LiveMarketDataClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: BinanceHttpClient, market: BinanceMarketHttpAPI, enum_parser: BinanceEnumParser, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: InstrumentProvider, account_type: BinanceAccountType, base_url_ws: str, name: str | None, config: BinanceDataClientConfig, base_url_ws_public: str | None = None) -> None: ...
