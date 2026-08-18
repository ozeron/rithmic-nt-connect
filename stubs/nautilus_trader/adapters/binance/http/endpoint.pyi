from _typeshed import Incomplete
from nautilus_trader.adapters.binance.common.enums import BinanceSecurityType as BinanceSecurityType
from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol as BinanceSymbol, BinanceSymbols as BinanceSymbols
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient as BinanceHttpClient
from nautilus_trader.core.nautilus_pyo3 import HttpMethod as HttpMethod
from typing import Any

def enc_hook(obj: Any) -> Any: ...

class BinanceHttpEndpoint:
    client: Incomplete
    methods_desc: Incomplete
    url_path: Incomplete
    decoder: Incomplete
    encoder: Incomplete
    def __init__(self, client: BinanceHttpClient, methods_desc: dict[HttpMethod, BinanceSecurityType], url_path: str) -> None: ...
