import asyncio
from _typeshed import Incomplete
from nautilus_trader.adapters.bitmex.config import BitmexExecClientConfig as BitmexExecClientConfig
from nautilus_trader.adapters.bitmex.constants import BITMEX_VENUE as BITMEX_VENUE
from nautilus_trader.adapters.bitmex.providers import BitmexInstrumentProvider as BitmexInstrumentProvider
from nautilus_trader.adapters.bitmex.types import BITMEX_INSTRUMENT_TYPES as BITMEX_INSTRUMENT_TYPES, BitmexInstrument as BitmexInstrument
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor, LogLevel as LogLevel
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import BitmexEnvironment as BitmexEnvironment
from nautilus_trader.core.uuid import UUID4 as UUID4
from nautilus_trader.execution.messages import BatchCancelOrders as BatchCancelOrders, CancelAllOrders as CancelAllOrders, CancelOrder as CancelOrder, GenerateFillReports as GenerateFillReports, GenerateOrderStatusReport as GenerateOrderStatusReport, GenerateOrderStatusReports as GenerateOrderStatusReports, GeneratePositionStatusReports as GeneratePositionStatusReports, ModifyOrder as ModifyOrder, QueryOrder as QueryOrder, SubmitOrder as SubmitOrder, SubmitOrderList as SubmitOrderList
from nautilus_trader.execution.reports import FillReport as FillReport, OrderStatusReport as OrderStatusReport, PositionStatusReport as PositionStatusReport
from nautilus_trader.live.cancellation import DEFAULT_FUTURE_CANCELLATION_TIMEOUT as DEFAULT_FUTURE_CANCELLATION_TIMEOUT, cancel_tasks_with_timeout as cancel_tasks_with_timeout
from nautilus_trader.live.execution_client import LiveExecutionClient as LiveExecutionClient
from nautilus_trader.model.enums import AccountType as AccountType, ContingencyType as ContingencyType, OmsType as OmsType, OrderStatus as OrderStatus, OrderType as OrderType
from nautilus_trader.model.events import AccountState as AccountState, OrderAccepted as OrderAccepted, OrderCancelRejected as OrderCancelRejected, OrderCanceled as OrderCanceled, OrderExpired as OrderExpired, OrderFilled as OrderFilled, OrderModifyRejected as OrderModifyRejected, OrderRejected as OrderRejected, OrderTriggered as OrderTriggered, OrderUpdated as OrderUpdated
from nautilus_trader.model.functions import contingency_type_to_pyo3 as contingency_type_to_pyo3, order_side_to_pyo3 as order_side_to_pyo3, order_type_to_pyo3 as order_type_to_pyo3, time_in_force_to_pyo3 as time_in_force_to_pyo3, trailing_offset_type_to_pyo3 as trailing_offset_type_to_pyo3, trigger_type_to_pyo3 as trigger_type_to_pyo3
from nautilus_trader.model.identifiers import AccountId as AccountId, ClientId as ClientId, ClientOrderId as ClientOrderId
from nautilus_trader.model.objects import Quantity as Quantity
from nautilus_trader.model.orders import Order as Order

class BitmexExecutionClient(LiveExecutionClient):
    pyo3_account_id: Incomplete
    def __init__(self, loop: asyncio.AbstractEventLoop, client: nautilus_pyo3.BitmexHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: BitmexInstrumentProvider, config: BitmexExecClientConfig, name: str | None) -> None: ...
    @property
    def instrument_provider(self) -> BitmexInstrumentProvider: ...
    async def generate_order_status_reports(self, command: GenerateOrderStatusReports) -> list[OrderStatusReport]: ...
    async def generate_order_status_report(self, command: GenerateOrderStatusReport) -> OrderStatusReport | None: ...
    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]: ...
    async def generate_position_status_reports(self, command: GeneratePositionStatusReports) -> list[PositionStatusReport]: ...
    BITMEX_PEG_PRICE_TYPES: Incomplete
