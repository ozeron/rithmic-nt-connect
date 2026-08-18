import msgspec
from _typeshed import Incomplete
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType as BinanceAccountType, BinanceSecurityType as BinanceSecurityType
from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol as BinanceSymbol
from nautilus_trader.adapters.binance.futures.schemas.wallet import BinanceFuturesCommissionRate as BinanceFuturesCommissionRate
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient as BinanceHttpClient
from nautilus_trader.adapters.binance.http.endpoint import BinanceHttpEndpoint as BinanceHttpEndpoint
from nautilus_trader.common.component import LiveClock as LiveClock
from nautilus_trader.core.nautilus_pyo3 import HttpMethod as HttpMethod

class BinanceFuturesCommissionRateHttp(BinanceHttpEndpoint):
    def __init__(self, client: BinanceHttpClient, base_endpoint: str) -> None: ...
    class GetParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        timestamp: str
        symbol: BinanceSymbol
        recvWindow: str | None = ...
    async def get(self, params: GetParameters) -> BinanceFuturesCommissionRate: ...

class BinanceFuturesWalletHttpAPI:
    client: Incomplete
    base_endpoint: str
    def __init__(self, client: BinanceHttpClient, clock: LiveClock, account_type: BinanceAccountType = ...) -> None: ...
    async def query_futures_commission_rate(self, symbol: str, recv_window: str | None = None) -> BinanceFuturesCommissionRate: ...
