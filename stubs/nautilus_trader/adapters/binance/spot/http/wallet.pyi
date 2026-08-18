import msgspec
from _typeshed import Incomplete
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType as BinanceAccountType, BinanceSecurityType as BinanceSecurityType
from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol as BinanceSymbol
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient as BinanceHttpClient
from nautilus_trader.adapters.binance.http.endpoint import BinanceHttpEndpoint as BinanceHttpEndpoint
from nautilus_trader.adapters.binance.spot.schemas.wallet import BinanceSpotTradeFee as BinanceSpotTradeFee
from nautilus_trader.common.component import LiveClock as LiveClock
from nautilus_trader.core.nautilus_pyo3 import HttpMethod as HttpMethod

class BinanceSpotTradeFeeHttp(BinanceHttpEndpoint):
    def __init__(self, client: BinanceHttpClient, base_endpoint: str) -> None: ...
    class GetParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        timestamp: str
        symbol: BinanceSymbol | None = ...
        recvWindow: str | None = ...
    async def get(self, params: GetParameters) -> list[BinanceSpotTradeFee]: ...

class BinanceSpotWalletHttpAPI:
    client: Incomplete
    base_endpoint: str
    def __init__(self, client: BinanceHttpClient, clock: LiveClock, account_type: BinanceAccountType = ...) -> None: ...
    async def query_spot_trade_fees(self, symbol: str | None = None, recv_window: str | None = None) -> list[BinanceSpotTradeFee]: ...
