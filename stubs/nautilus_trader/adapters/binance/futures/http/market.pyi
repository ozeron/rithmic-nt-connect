from nautilus_trader.adapters.binance.common.enums import BinanceAccountType as BinanceAccountType, BinanceSecurityType as BinanceSecurityType
from nautilus_trader.adapters.binance.futures.schemas.market import BinanceFuturesExchangeInfo as BinanceFuturesExchangeInfo
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient as BinanceHttpClient
from nautilus_trader.adapters.binance.http.endpoint import BinanceHttpEndpoint as BinanceHttpEndpoint
from nautilus_trader.adapters.binance.http.market import BinanceMarketHttpAPI as BinanceMarketHttpAPI
from nautilus_trader.core.nautilus_pyo3 import HttpMethod as HttpMethod

class BinanceFuturesExchangeInfoHttp(BinanceHttpEndpoint):
    def __init__(self, client: BinanceHttpClient, base_endpoint: str) -> None: ...
    async def get(self) -> BinanceFuturesExchangeInfo: ...

class BinanceFuturesMarketHttpAPI(BinanceMarketHttpAPI):
    def __init__(self, client: BinanceHttpClient, account_type: BinanceAccountType = ...) -> None: ...
    async def query_futures_exchange_info(self) -> BinanceFuturesExchangeInfo: ...
