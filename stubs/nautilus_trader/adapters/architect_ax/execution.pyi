import asyncio
from _typeshed import Incomplete
from nautilus_trader.adapters.architect_ax.config import AxExecClientConfig as AxExecClientConfig
from nautilus_trader.adapters.architect_ax.constants import AX_ADAPTER_SUPPORTED_ORDER_TYPES as AX_ADAPTER_SUPPORTED_ORDER_TYPES, AX_AUTH_TOKEN_REFRESH_INTERVAL_SECS as AX_AUTH_TOKEN_REFRESH_INTERVAL_SECS, AX_AUTH_TOKEN_REFRESH_RETRY_SECS as AX_AUTH_TOKEN_REFRESH_RETRY_SECS, AX_AUTH_TOKEN_REQUEST_TIMEOUT_SECS as AX_AUTH_TOKEN_REQUEST_TIMEOUT_SECS, AX_AUTH_TOKEN_TTL_SECS as AX_AUTH_TOKEN_TTL_SECS, AX_VENUE as AX_VENUE, AX_WS_ORDERS_PRODUCTION_URL as AX_WS_ORDERS_PRODUCTION_URL, AX_WS_ORDERS_SANDBOX_URL as AX_WS_ORDERS_SANDBOX_URL
from nautilus_trader.adapters.architect_ax.providers import AxInstrumentProvider as AxInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor, LogLevel as LogLevel
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import AxEnvironment as AxEnvironment
from nautilus_trader.execution.messages import CancelAllOrders as CancelAllOrders, CancelOrder as CancelOrder, GenerateFillReports as GenerateFillReports, GenerateOrderStatusReport as GenerateOrderStatusReport, GenerateOrderStatusReports as GenerateOrderStatusReports, GeneratePositionStatusReports as GeneratePositionStatusReports, ModifyOrder as ModifyOrder, QueryAccount as QueryAccount, SubmitOrder as SubmitOrder, SubmitOrderList as SubmitOrderList
from nautilus_trader.execution.reports import FillReport as FillReport, OrderStatusReport as OrderStatusReport, PositionStatusReport as PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient as LiveExecutionClient
from nautilus_trader.model.enums import AccountType as AccountType, OmsType as OmsType, OrderStatus as OrderStatus, OrderType as OrderType, TimeInForce as TimeInForce, position_side_to_str as position_side_to_str
from nautilus_trader.model.events import AccountState as AccountState, OrderAccepted as OrderAccepted, OrderCancelRejected as OrderCancelRejected, OrderCanceled as OrderCanceled, OrderExpired as OrderExpired, OrderFilled as OrderFilled, OrderRejected as OrderRejected, OrderUpdated as OrderUpdated
from nautilus_trader.model.functions import order_side_to_pyo3 as order_side_to_pyo3, order_type_to_pyo3 as order_type_to_pyo3, time_in_force_to_pyo3 as time_in_force_to_pyo3
from nautilus_trader.model.identifiers import AccountId as AccountId, ClientId as ClientId, ClientOrderId as ClientOrderId, InstrumentId as InstrumentId
from nautilus_trader.model.orders import LimitOrder as LimitOrder, Order as Order

class AxExecutionClient(LiveExecutionClient):
    pyo3_account_id: Incomplete
    pyo3_trader_id: Incomplete
    def __init__(self, loop: asyncio.AbstractEventLoop, client: nautilus_pyo3.AxHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: AxInstrumentProvider, config: AxExecClientConfig, name: str | None) -> None: ...
    async def generate_order_status_report(self, command: GenerateOrderStatusReport) -> OrderStatusReport | None: ...
    async def generate_order_status_reports(self, command: GenerateOrderStatusReports) -> list[OrderStatusReport]: ...
    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]: ...
    async def generate_position_status_reports(self, command: GeneratePositionStatusReports) -> list[PositionStatusReport]: ...
