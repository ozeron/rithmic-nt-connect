from nautilus_trader.adapters.binance.common.constants import BINANCE_VENUE as BINANCE_VENUE
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType as BinanceAccountType, BinanceSymbolFilterType as BinanceSymbolFilterType
from nautilus_trader.adapters.binance.common.schemas.market import BinanceSymbolFilter as BinanceSymbolFilter
from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol as BinanceSymbol
from nautilus_trader.adapters.binance.config import BinanceInstrumentProviderConfig as BinanceInstrumentProviderConfig
from nautilus_trader.adapters.binance.futures.enums import BinanceFuturesContractStatus as BinanceFuturesContractStatus, BinanceFuturesContractType as BinanceFuturesContractType
from nautilus_trader.adapters.binance.futures.http.account import BinanceFuturesAccountHttpAPI as BinanceFuturesAccountHttpAPI
from nautilus_trader.adapters.binance.futures.http.market import BinanceFuturesMarketHttpAPI as BinanceFuturesMarketHttpAPI
from nautilus_trader.adapters.binance.futures.http.wallet import BinanceFuturesWalletHttpAPI as BinanceFuturesWalletHttpAPI
from nautilus_trader.adapters.binance.futures.schemas.account import BinanceFuturesFeeRates as BinanceFuturesFeeRates, BinanceFuturesPositionRisk as BinanceFuturesPositionRisk
from nautilus_trader.adapters.binance.futures.schemas.market import BinanceFuturesSymbolInfo as BinanceFuturesSymbolInfo
from nautilus_trader.adapters.binance.futures.schemas.wallet import BinanceFuturesCommissionRate as BinanceFuturesCommissionRate
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient as BinanceHttpClient
from nautilus_trader.common.component import LiveClock as LiveClock
from nautilus_trader.common.providers import InstrumentProvider as InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.core.datetime import millis_to_nanos as millis_to_nanos
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId, Symbol as Symbol, Venue as Venue
from nautilus_trader.model.instruments.crypto_future import CryptoFuture as CryptoFuture
from nautilus_trader.model.instruments.crypto_perpetual import CryptoPerpetual as CryptoPerpetual
from nautilus_trader.model.objects import Money as Money, PRICE_MAX as PRICE_MAX, PRICE_MIN as PRICE_MIN, Price as Price, QUANTITY_MAX as QUANTITY_MAX, QUANTITY_MIN as QUANTITY_MIN, Quantity as Quantity

class BinanceFuturesInstrumentProvider(InstrumentProvider):
    def __init__(self, client: BinanceHttpClient, clock: LiveClock, account_type: BinanceAccountType = ..., config: InstrumentProviderConfig | BinanceInstrumentProviderConfig | None = None, venue: Venue = ...) -> None: ...
    async def load_all_async(self, filters: dict | None = None) -> None: ...
    async def load_ids_async(self, instrument_ids: list[InstrumentId], filters: dict | None = None) -> None: ...
    async def load_async(self, instrument_id: InstrumentId, filters: dict | None = None) -> None: ...
