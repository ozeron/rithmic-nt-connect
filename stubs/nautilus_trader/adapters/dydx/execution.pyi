import asyncio
from nautilus_trader.adapters.dydx.config import DydxExecClientConfig as DydxExecClientConfig
from nautilus_trader.adapters.dydx.constants import DYDX_VENUE as DYDX_VENUE
from nautilus_trader.adapters.dydx.providers import DydxInstrumentProvider as DydxInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor, LogLevel as LogLevel
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.datetime import nanos_to_secs as nanos_to_secs
from nautilus_trader.core.nautilus_pyo3 import DydxNetwork as DydxNetwork
from nautilus_trader.core.uuid import UUID4 as UUID4
from nautilus_trader.execution.messages import BatchCancelOrders as BatchCancelOrders, CancelAllOrders as CancelAllOrders, CancelOrder as CancelOrder, GenerateFillReports as GenerateFillReports, GenerateOrderStatusReport as GenerateOrderStatusReport, GenerateOrderStatusReports as GenerateOrderStatusReports, GeneratePositionStatusReports as GeneratePositionStatusReports, ModifyOrder as ModifyOrder, QueryAccount as QueryAccount, SubmitOrder as SubmitOrder, SubmitOrderList as SubmitOrderList
from nautilus_trader.execution.reports import FillReport as FillReport, OrderStatusReport as OrderStatusReport, PositionStatusReport as PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient as LiveExecutionClient
from nautilus_trader.model.enums import AccountType as AccountType, OmsType as OmsType, OrderStatus as OrderStatus, OrderType as OrderType
from nautilus_trader.model.events import AccountState as AccountState, OrderAccepted as OrderAccepted, OrderCanceled as OrderCanceled, OrderFilled as OrderFilled
from nautilus_trader.model.functions import order_side_to_pyo3 as order_side_to_pyo3, order_type_to_pyo3 as order_type_to_pyo3
from nautilus_trader.model.identifiers import AccountId as AccountId, ClientId as ClientId
from nautilus_trader.model.orders import LimitIfTouchedOrder as LimitIfTouchedOrder, LimitOrder as LimitOrder, MarketIfTouchedOrder as MarketIfTouchedOrder, MarketOrder as MarketOrder, Order as Order, StopLimitOrder as StopLimitOrder, StopMarketOrder as StopMarketOrder

class DydxExecutionClient(LiveExecutionClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: nautilus_pyo3.DydxHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: DydxInstrumentProvider, config: DydxExecClientConfig, name: str | None) -> None: ...
    @property
    def pyo3_account_id(self) -> nautilus_pyo3.AccountId: ...
    async def generate_order_status_report(self, command: GenerateOrderStatusReport) -> OrderStatusReport | None: ...
    async def generate_order_status_reports(self, command: GenerateOrderStatusReports) -> list[OrderStatusReport]: ...
    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]: ...
    async def generate_position_status_reports(self, command: GeneratePositionStatusReports) -> list[PositionStatusReport]: ...
