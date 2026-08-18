from _typeshed import Incomplete
from nautilus_trader.adapters.binance.common.constants import BINANCE_RETRY_ERRORS as BINANCE_RETRY_ERRORS
from nautilus_trader.adapters.binance.common.enums import BinanceErrorCode as BinanceErrorCode

class BinanceError(Exception):
    status: Incomplete
    message: Incomplete
    headers: Incomplete
    def __init__(self, status, message, headers) -> None: ...

class BinanceServerError(BinanceError):
    def __init__(self, status, message, headers) -> None: ...

class BinanceClientError(BinanceError):
    def __init__(self, status, message, headers) -> None: ...

def get_binance_error_code(error: BaseException) -> BinanceErrorCode | None: ...
def should_retry(error: BaseException) -> bool: ...
