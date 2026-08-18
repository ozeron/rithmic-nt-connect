import asyncio
from nautilus_trader.adapters.hyperliquid.config import HyperliquidExecClientConfig as HyperliquidExecClientConfig
from nautilus_trader.adapters.hyperliquid.constants import HYPERLIQUID_POST_ONLY_WOULD_MATCH as HYPERLIQUID_POST_ONLY_WOULD_MATCH, HYPERLIQUID_VENUE as HYPERLIQUID_VENUE
from nautilus_trader.adapters.hyperliquid.providers import HyperliquidInstrumentProvider as HyperliquidInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.cache.transformers import transform_order_to_pyo3 as transform_order_to_pyo3
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor, LogLevel as LogLevel
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.execution.messages import BatchCancelOrders as BatchCancelOrders, CancelAllOrders as CancelAllOrders, CancelOrder as CancelOrder, GenerateFillReports as GenerateFillReports, GenerateOrderStatusReport as GenerateOrderStatusReport, GenerateOrderStatusReports as GenerateOrderStatusReports, GeneratePositionStatusReports as GeneratePositionStatusReports, ModifyOrder as ModifyOrder, QueryAccount as QueryAccount, SubmitOrder as SubmitOrder, SubmitOrderList as SubmitOrderList
from nautilus_trader.execution.reports import FillReport as FillReport, OrderStatusReport as OrderStatusReport, PositionStatusReport as PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient as LiveExecutionClient
from nautilus_trader.model.enums import AccountType as AccountType, OmsType as OmsType, OrderSide as OrderSide, OrderStatus as OrderStatus, OrderType as OrderType, TimeInForce as TimeInForce, order_side_to_str as order_side_to_str
from nautilus_trader.model.events import AccountState as AccountState, OrderAccepted as OrderAccepted, OrderCancelRejected as OrderCancelRejected, OrderCanceled as OrderCanceled, OrderExpired as OrderExpired, OrderModifyRejected as OrderModifyRejected, OrderRejected as OrderRejected, OrderUpdated as OrderUpdated
from nautilus_trader.model.functions import order_side_to_pyo3 as order_side_to_pyo3, order_type_to_pyo3 as order_type_to_pyo3, time_in_force_to_pyo3 as time_in_force_to_pyo3
from nautilus_trader.model.identifiers import AccountId as AccountId, ClientId as ClientId, ClientOrderId as ClientOrderId, VenueOrderId as VenueOrderId
from nautilus_trader.model.objects import Price as Price, Quantity as Quantity
from nautilus_trader.model.orders import Order as Order

class HyperliquidExecutionClient(LiveExecutionClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: nautilus_pyo3.HyperliquidHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: HyperliquidInstrumentProvider, config: HyperliquidExecClientConfig, name: str | None = None, account_address: str | None = None) -> None: ...
    @property
    def hyperliquid_instrument_provider(self) -> HyperliquidInstrumentProvider: ...
    async def generate_order_status_report(self, command: GenerateOrderStatusReport) -> OrderStatusReport | None: ...
    async def generate_order_status_reports(self, command: GenerateOrderStatusReports) -> list[OrderStatusReport]: ...
    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]: ...
    async def generate_position_status_reports(self, command: GeneratePositionStatusReports) -> list[PositionStatusReport]: ...
