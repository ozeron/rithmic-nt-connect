import asyncio
from _typeshed import Incomplete
from decimal import Decimal
from nautilus_trader.adapters.hyperliquid.config import HyperliquidDataClientConfig as HyperliquidDataClientConfig
from nautilus_trader.adapters.hyperliquid.constants import HYPERLIQUID_VENUE as HYPERLIQUID_VENUE
from nautilus_trader.adapters.hyperliquid.providers import HyperliquidInstrumentProvider as HyperliquidInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.data import Data as Data
from nautilus_trader.core.datetime import ensure_pydatetime_utc as ensure_pydatetime_utc
from nautilus_trader.data.messages import RequestBars as RequestBars, RequestData as RequestData, RequestInstrument as RequestInstrument, RequestInstruments as RequestInstruments, RequestQuoteTicks as RequestQuoteTicks, RequestTradeTicks as RequestTradeTicks, SubscribeBars as SubscribeBars, SubscribeData as SubscribeData, SubscribeFundingRates as SubscribeFundingRates, SubscribeIndexPrices as SubscribeIndexPrices, SubscribeInstrument as SubscribeInstrument, SubscribeInstruments as SubscribeInstruments, SubscribeMarkPrices as SubscribeMarkPrices, SubscribeOrderBook as SubscribeOrderBook, SubscribeQuoteTicks as SubscribeQuoteTicks, SubscribeTradeTicks as SubscribeTradeTicks, UnsubscribeBars as UnsubscribeBars, UnsubscribeData as UnsubscribeData, UnsubscribeFundingRates as UnsubscribeFundingRates, UnsubscribeIndexPrices as UnsubscribeIndexPrices, UnsubscribeInstrument as UnsubscribeInstrument, UnsubscribeInstruments as UnsubscribeInstruments, UnsubscribeMarkPrices as UnsubscribeMarkPrices, UnsubscribeOrderBook as UnsubscribeOrderBook, UnsubscribeQuoteTicks as UnsubscribeQuoteTicks, UnsubscribeTradeTicks as UnsubscribeTradeTicks
from nautilus_trader.live.data_client import LiveMarketDataClient as LiveMarketDataClient
from nautilus_trader.model.data import Bar as Bar, CustomData as CustomData, DataType as DataType, FundingRateUpdate as FundingRateUpdate, capsule_to_data as capsule_to_data
from nautilus_trader.model.enums import AggressorSide as AggressorSide, BookType as BookType, book_type_to_str as book_type_to_str
from nautilus_trader.model.identifiers import ClientId as ClientId, InstrumentId as InstrumentId
from nautilus_trader.model.instruments import instruments_from_pyo3 as instruments_from_pyo3
from nautilus_trader.model.objects import Price as Price, Quantity as Quantity
from typing import Any

class HyperliquidAllMids(Data):
    mids: Incomplete
    def __init__(self, mids: dict[str, str], ts_event: int, ts_init: int) -> None: ...
    @property
    def ts_event(self) -> int: ...
    @property
    def ts_init(self) -> int: ...
    @staticmethod
    def from_pyo3(pyo3_all_mids: Any) -> HyperliquidAllMids: ...

class HyperliquidOpenInterest(Data):
    instrument_id: Incomplete
    open_interest: Incomplete
    def __init__(self, instrument_id: InstrumentId, open_interest: Decimal, ts_event: int, ts_init: int) -> None: ...
    @property
    def ts_event(self) -> int: ...
    @property
    def ts_init(self) -> int: ...
    @staticmethod
    def from_pyo3(pyo3_open_interest: Any) -> HyperliquidOpenInterest: ...

class HyperliquidPublicTrade(Data):
    instrument_id: Incomplete
    price: Incomplete
    size: Incomplete
    aggressor_side: Incomplete
    trade_id: Incomplete
    buyer: Incomplete
    seller: Incomplete
    hash: Incomplete
    def __init__(self, instrument_id: InstrumentId, price: Price, size: Quantity, aggressor_side: AggressorSide, trade_id: str, buyer: str, seller: str, hash: str, ts_event: int, ts_init: int) -> None: ...
    @property
    def ts_event(self) -> int: ...
    @property
    def ts_init(self) -> int: ...
    @staticmethod
    def from_pyo3(pyo3_public_trade: Any) -> HyperliquidPublicTrade: ...
    def to_pyo3(self) -> Any: ...

class HyperliquidImpactPrices:
    bid: Incomplete
    ask: Incomplete
    def __init__(self, bid: Price, ask: Price) -> None: ...

class HyperliquidDexAssetCtx:
    dex: Incomplete
    instrument_id: Incomplete
    mark_price: Incomplete
    oracle_price: Incomplete
    prev_day_price: Incomplete
    mid_price: Incomplete
    impact_prices: Incomplete
    funding_rate: Incomplete
    open_interest: Incomplete
    premium: Incomplete
    day_ntl_volume: Incomplete
    day_base_volume: Incomplete
    def __init__(self, dex: str, instrument_id: InstrumentId, mark_price: Price, oracle_price: Price, prev_day_price: Price, mid_price: Price | None, impact_prices: HyperliquidImpactPrices | None, funding_rate: Decimal, open_interest: Decimal, premium: Decimal | None, day_ntl_volume: Decimal, day_base_volume: Decimal) -> None: ...
    @staticmethod
    def from_pyo3(pyo3_entry: Any) -> HyperliquidDexAssetCtx: ...

class HyperliquidAllDexsAssetCtxs(Data):
    entries: Incomplete
    def __init__(self, entries: list[HyperliquidDexAssetCtx], ts_event: int, ts_init: int) -> None: ...
    @property
    def ts_event(self) -> int: ...
    @property
    def ts_init(self) -> int: ...
    @staticmethod
    def from_pyo3(pyo3_all_ctxs: Any) -> HyperliquidAllDexsAssetCtxs: ...

class HyperliquidDataClient(LiveMarketDataClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: nautilus_pyo3.HyperliquidHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: HyperliquidInstrumentProvider, config: HyperliquidDataClientConfig, name: str | None = None) -> None: ...
    @property
    def instrument_provider(self) -> HyperliquidInstrumentProvider: ...
