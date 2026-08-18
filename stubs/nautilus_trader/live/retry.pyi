from _typeshed import Incomplete
from collections.abc import Awaitable, Callable as Callable
from nautilus_trader.common.component import Logger as Logger

def get_exponential_backoff(num_attempts: int, delay_initial_ms: int = 500, delay_max_ms: int = 2000, backoff_factor: int = 2, jitter: bool = True) -> int: ...

class RetryManager[T]:
    max_retries: Incomplete
    delay_initial_ms: Incomplete
    delay_max_ms: Incomplete
    backoff_factor: Incomplete
    retries: int
    exc_types: Incomplete
    retry_check: Incomplete
    error_logger: Incomplete
    cancel_event: Incomplete
    log: Incomplete
    name: str | None
    details: list[object] | None
    details_str: str | None
    result: bool
    message: str | None
    last_exception: BaseException | None
    def __init__(self, max_retries: int, delay_initial_ms: int, delay_max_ms: int, backoff_factor: int, logger: Logger, exc_types: tuple[type[BaseException], ...], retry_check: Callable[[BaseException], bool] | None = None, error_logger: Callable[[str, BaseException | None], None] | None = None) -> None: ...
    async def run(self, name: str, details: list[object] | None, func: Callable[..., Awaitable[T]], *args, **kwargs) -> T | None: ...
    def cancel(self) -> None: ...
    def clear(self) -> None: ...

class RetryManagerPool[T]:
    max_retries: Incomplete
    delay_initial_ms: Incomplete
    delay_max_ms: Incomplete
    backoff_factor: Incomplete
    logger: Incomplete
    exc_types: Incomplete
    retry_check: Incomplete
    error_logger: Incomplete
    pool_size: Incomplete
    def __init__(self, pool_size: int, max_retries: int, delay_initial_ms: int, delay_max_ms: int, backoff_factor: int, logger: Logger, exc_types: tuple[type[BaseException], ...], retry_check: Callable[[BaseException], bool] | None = None, error_logger: Callable[[str, BaseException | None], None] | None = None) -> None: ...
    def shutdown(self) -> None: ...
    async def acquire(self) -> RetryManager: ...
    async def release(self, retry_manager: RetryManager) -> None: ...
