from nautilus_trader.adapters.binance.common.constants import BINANCE_VENUE as BINANCE_VENUE
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType as BinanceAccountType, BinanceEnvironment as BinanceEnvironment, BinanceSymbolFilterType as BinanceSymbolFilterType
from nautilus_trader.adapters.binance.common.schemas.market import BinanceSymbolFilter as BinanceSymbolFilter
from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol as BinanceSymbol
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient as BinanceHttpClient
from nautilus_trader.adapters.binance.http.error import BinanceClientError as BinanceClientError
from nautilus_trader.adapters.binance.spot.http.market import BinanceSpotMarketHttpAPI as BinanceSpotMarketHttpAPI
from nautilus_trader.adapters.binance.spot.http.wallet import BinanceSpotWalletHttpAPI as BinanceSpotWalletHttpAPI
from nautilus_trader.adapters.binance.spot.schemas.market import BinanceSpotSymbolInfo as BinanceSpotSymbolInfo
from nautilus_trader.adapters.binance.spot.schemas.wallet import BinanceSpotTradeFee as BinanceSpotTradeFee
from nautilus_trader.common.component import LiveClock as LiveClock
from nautilus_trader.common.providers import InstrumentProvider as InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.core.datetime import millis_to_nanos as millis_to_nanos
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId, Symbol as Symbol, Venue as Venue
from nautilus_trader.model.instruments.currency_pair import CurrencyPair as CurrencyPair
from nautilus_trader.model.objects import Money as Money, PRICE_MAX as PRICE_MAX, PRICE_MIN as PRICE_MIN, Price as Price, QUANTITY_MAX as QUANTITY_MAX, QUANTITY_MIN as QUANTITY_MIN, Quantity as Quantity

class BinanceSpotInstrumentProvider(InstrumentProvider):
    def __init__(self, client: BinanceHttpClient, clock: LiveClock, account_type: BinanceAccountType = ..., environment: BinanceEnvironment = ..., config: InstrumentProviderConfig | None = None, venue: Venue = ...) -> None: ...
    async def load_all_async(self, filters: dict | None = None) -> None: ...
    async def load_ids_async(self, instrument_ids: list[InstrumentId], filters: dict | None = None) -> None: ...
    async def load_async(self, instrument_id: InstrumentId, filters: dict | None = None) -> None: ...
