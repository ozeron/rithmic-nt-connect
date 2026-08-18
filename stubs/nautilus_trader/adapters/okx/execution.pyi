import asyncio
from _typeshed import Incomplete
from enum import Enum
from nautilus_trader.adapters.okx.config import OKXExecClientConfig as OKXExecClientConfig
from nautilus_trader.adapters.okx.constants import OKX_VENUE as OKX_VENUE
from nautilus_trader.adapters.okx.providers import OKXInstrumentProvider as OKXInstrumentProvider
from nautilus_trader.adapters.okx.types import OKXAttachedOcoBinding as OKXAttachedOcoBinding, OkxInstrument as OkxInstrument
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor, LogLevel as LogLevel
from nautilus_trader.common.secure import mask_api_key as mask_api_key
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.core.datetime import ensure_pydatetime_utc as ensure_pydatetime_utc
from nautilus_trader.core.nautilus_pyo3 import OKXEnvironment as OKXEnvironment, OKXInstrumentType as OKXInstrumentType, OKXMarginMode as OKXMarginMode, OKXRegion as OKXRegion, OKXTradeMode as OKXTradeMode
from nautilus_trader.core.uuid import UUID4 as UUID4
from nautilus_trader.execution.messages import BatchCancelOrders as BatchCancelOrders, CancelAllOrders as CancelAllOrders, CancelOrder as CancelOrder, GenerateFillReports as GenerateFillReports, GenerateOrderStatusReport as GenerateOrderStatusReport, GenerateOrderStatusReports as GenerateOrderStatusReports, GeneratePositionStatusReports as GeneratePositionStatusReports, ModifyOrder as ModifyOrder, QueryAccount as QueryAccount, SubmitOrder as SubmitOrder, SubmitOrderList as SubmitOrderList
from nautilus_trader.execution.reports import FillReport as FillReport, OrderStatusReport as OrderStatusReport, PositionStatusReport as PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient as LiveExecutionClient
from nautilus_trader.model.enums import AccountType as AccountType, OmsType as OmsType, OrderSide as OrderSide, OrderStatus as OrderStatus, OrderType as OrderType, PositionSide as PositionSide, TrailingOffsetType as TrailingOffsetType, TriggerType as TriggerType, order_side_to_str as order_side_to_str
from nautilus_trader.model.events import AccountState as AccountState, OrderAccepted as OrderAccepted, OrderCancelRejected as OrderCancelRejected, OrderCanceled as OrderCanceled, OrderExpired as OrderExpired, OrderModifyRejected as OrderModifyRejected, OrderRejected as OrderRejected, OrderTriggered as OrderTriggered, OrderUpdated as OrderUpdated
from nautilus_trader.model.functions import order_side_to_pyo3 as order_side_to_pyo3, order_type_to_pyo3 as order_type_to_pyo3, time_in_force_to_pyo3 as time_in_force_to_pyo3, trigger_type_to_pyo3 as trigger_type_to_pyo3
from nautilus_trader.model.identifiers import AccountId as AccountId, ClientId as ClientId, ClientOrderId as ClientOrderId, InstrumentId as InstrumentId
from nautilus_trader.model.instruments import CryptoFuturesSpread as CryptoFuturesSpread, CryptoOption as CryptoOption, CryptoOptionSpread as CryptoOptionSpread, CurrencyPair as CurrencyPair
from nautilus_trader.model.objects import Quantity as Quantity
from nautilus_trader.model.orders import Order as Order

class _OKXOrderCommandRoute(Enum):
    REGULAR_WS = 'regular_ws'
    ALGO_HTTP = 'algo_http'
    SPREAD_HTTP = 'spread_http'

class _OKXCancelAllOrdersRoute(Enum):
    BATCH_WS = 'batch_ws'
    MASS_CANCEL_HTTP = 'mass_cancel_http'
    SPREAD_HTTP = 'spread_http'

class OKXExecutionClient(LiveExecutionClient):
    pyo3_account_id: Incomplete
    def __init__(self, loop: asyncio.AbstractEventLoop, client: nautilus_pyo3.OKXHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: OKXInstrumentProvider, config: OKXExecClientConfig, name: str | None) -> None: ...
    @property
    def okx_instrument_provider(self) -> OKXInstrumentProvider: ...
    async def generate_order_status_reports(self, command: GenerateOrderStatusReports) -> list[OrderStatusReport]: ...
    async def generate_order_status_report(self, command: GenerateOrderStatusReport) -> OrderStatusReport | None: ...
    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]: ...
    async def generate_position_status_reports(self, command: GeneratePositionStatusReports) -> list[PositionStatusReport]: ...
