import asyncio
from _typeshed import Incomplete
from nautilus_trader.accounting.factory import AccountFactory as AccountFactory
from nautilus_trader.adapters.bybit.config import BybitExecClientConfig as BybitExecClientConfig
from nautilus_trader.adapters.bybit.constants import BYBIT_VENUE as BYBIT_VENUE
from nautilus_trader.adapters.bybit.providers import BybitInstrumentProvider as BybitInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor, LogLevel as LogLevel
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.datetime import ensure_pydatetime_utc as ensure_pydatetime_utc
from nautilus_trader.core.nautilus_pyo3 import BybitAccountType as BybitAccountType, BybitMarginAction as BybitMarginAction, BybitPositionIdx as BybitPositionIdx, BybitPositionMode as BybitPositionMode, BybitProductType as BybitProductType
from nautilus_trader.execution.messages import BatchCancelOrders as BatchCancelOrders, CancelAllOrders as CancelAllOrders, CancelOrder as CancelOrder, GenerateFillReports as GenerateFillReports, GenerateOrderStatusReport as GenerateOrderStatusReport, GenerateOrderStatusReports as GenerateOrderStatusReports, GeneratePositionStatusReports as GeneratePositionStatusReports, ModifyOrder as ModifyOrder, QueryAccount as QueryAccount, SubmitOrder as SubmitOrder, SubmitOrderList as SubmitOrderList
from nautilus_trader.execution.reports import FillReport as FillReport, OrderStatusReport as OrderStatusReport, PositionStatusReport as PositionStatusReport
from nautilus_trader.live.enqueue import ThrottledEnqueuer as ThrottledEnqueuer
from nautilus_trader.live.execution_client import LiveExecutionClient as LiveExecutionClient
from nautilus_trader.model.data import DataType as DataType
from nautilus_trader.model.enums import AccountType as AccountType, OmsType as OmsType, OrderSide as OrderSide, OrderStatus as OrderStatus, OrderType as OrderType, order_side_to_str as order_side_to_str
from nautilus_trader.model.events import AccountState as AccountState, OrderCancelRejected as OrderCancelRejected, OrderModifyRejected as OrderModifyRejected, OrderRejected as OrderRejected
from nautilus_trader.model.functions import order_side_to_pyo3 as order_side_to_pyo3, order_type_to_pyo3 as order_type_to_pyo3, time_in_force_to_pyo3 as time_in_force_to_pyo3
from nautilus_trader.model.identifiers import AccountId as AccountId, ClientId as ClientId, ClientOrderId as ClientOrderId, InstrumentId as InstrumentId, PositionId as PositionId
from nautilus_trader.model.objects import Quantity as Quantity
from nautilus_trader.model.orders import Order as Order

class BybitExecutionClient(LiveExecutionClient):
    pyo3_account_id: Incomplete
    def __init__(self, loop: asyncio.AbstractEventLoop, client: nautilus_pyo3.BybitHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: BybitInstrumentProvider, config: BybitExecClientConfig, name: str | None) -> None: ...
    @property
    def bybit_instrument_provider(self) -> BybitInstrumentProvider: ...
    async def generate_order_status_reports(self, command: GenerateOrderStatusReports) -> list[OrderStatusReport]: ...
    async def generate_order_status_report(self, command: GenerateOrderStatusReport) -> OrderStatusReport | None: ...
    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]: ...
    async def generate_position_status_reports(self, command: GeneratePositionStatusReports) -> list[PositionStatusReport]: ...
    async def set_leverage(self, symbol: str, leverage: int) -> None: ...
    async def set_position_mode(self, symbol: str, mode: BybitPositionMode) -> None: ...
