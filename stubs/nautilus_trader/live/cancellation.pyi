import asyncio
from nautilus_trader.common.component import Logger as Logger
from weakref import WeakSet

DEFAULT_FUTURE_CANCELLATION_TIMEOUT: float
DEFAULT_TASK_CANCELLATION_TIMEOUT: float

async def cancel_tasks_with_timeout(tasks: WeakSet[asyncio.Task] | set[asyncio.Task | asyncio.Future], logger: Logger | None = None, timeout_secs: float = ...) -> None: ...
