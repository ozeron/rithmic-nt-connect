import asyncio
from _typeshed import Incomplete
from nautilus_trader.adapters.deribit.config import DeribitExecClientConfig as DeribitExecClientConfig
from nautilus_trader.adapters.deribit.constants import DERIBIT_EXECUTION_SESSION_NAME as DERIBIT_EXECUTION_SESSION_NAME, DERIBIT_VENUE as DERIBIT_VENUE
from nautilus_trader.adapters.deribit.providers import DeribitInstrumentProvider as DeribitInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor, LogLevel as LogLevel
from nautilus_trader.common.secure import mask_api_key as mask_api_key
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import DeribitEnvironment as DeribitEnvironment
from nautilus_trader.execution.messages import BatchCancelOrders as BatchCancelOrders, CancelAllOrders as CancelAllOrders, CancelOrder as CancelOrder, GenerateFillReports as GenerateFillReports, GenerateOrderStatusReport as GenerateOrderStatusReport, GenerateOrderStatusReports as GenerateOrderStatusReports, GeneratePositionStatusReports as GeneratePositionStatusReports, ModifyOrder as ModifyOrder, QueryAccount as QueryAccount, QueryOrder as QueryOrder, SubmitOrder as SubmitOrder, SubmitOrderList as SubmitOrderList
from nautilus_trader.execution.reports import FillReport as FillReport, OrderStatusReport as OrderStatusReport, PositionStatusReport as PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient as LiveExecutionClient
from nautilus_trader.model.enums import AccountType as AccountType, OmsType as OmsType, OrderSide as OrderSide
from nautilus_trader.model.events import AccountState as AccountState, OrderAccepted as OrderAccepted, OrderCancelRejected as OrderCancelRejected, OrderCanceled as OrderCanceled, OrderExpired as OrderExpired, OrderModifyRejected as OrderModifyRejected, OrderRejected as OrderRejected, OrderUpdated as OrderUpdated
from nautilus_trader.model.functions import order_side_to_pyo3 as order_side_to_pyo3, order_type_to_pyo3 as order_type_to_pyo3, time_in_force_to_pyo3 as time_in_force_to_pyo3, trigger_type_to_pyo3 as trigger_type_to_pyo3
from nautilus_trader.model.identifiers import AccountId as AccountId, ClientId as ClientId, ClientOrderId as ClientOrderId

class DeribitExecutionClient(LiveExecutionClient):
    pyo3_account_id: Incomplete
    def __init__(self, loop: asyncio.AbstractEventLoop, http_client: nautilus_pyo3.DeribitHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: DeribitInstrumentProvider, config: DeribitExecClientConfig, name: str | None) -> None: ...
    async def generate_order_status_report(self, command: GenerateOrderStatusReport) -> OrderStatusReport | None: ...
    async def generate_order_status_reports(self, command: GenerateOrderStatusReports) -> list[OrderStatusReport]: ...
    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]: ...
    async def generate_position_status_reports(self, command: GeneratePositionStatusReports) -> list[PositionStatusReport]: ...
