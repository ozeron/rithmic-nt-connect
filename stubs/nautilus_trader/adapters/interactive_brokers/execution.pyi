import asyncio
from _typeshed import Incomplete
from ibapi.commission_and_fees_report import CommissionAndFeesReport as CommissionAndFeesReport
from ibapi.execution import Execution as Execution
from ibapi.order import Order as IBOrder
from ibapi.order_condition import OrderCondition as OrderCondition
from nautilus_trader.adapters.interactive_brokers.client import InteractiveBrokersClient as InteractiveBrokersClient
from nautilus_trader.adapters.interactive_brokers.client.common import IBPosition as IBPosition, get_venue_order_id as get_venue_order_id
from nautilus_trader.adapters.interactive_brokers.common import IBContract as IBContract, IBOrderTags as IBOrderTags
from nautilus_trader.adapters.interactive_brokers.config import InteractiveBrokersExecClientConfig as InteractiveBrokersExecClientConfig
from nautilus_trader.adapters.interactive_brokers.parsing.execution import MAP_ORDER_ACTION as MAP_ORDER_ACTION, MAP_ORDER_FIELDS as MAP_ORDER_FIELDS, MAP_ORDER_STATUS as MAP_ORDER_STATUS, MAP_ORDER_TYPE as MAP_ORDER_TYPE, MAP_TIME_IN_FORCE as MAP_TIME_IN_FORCE, MAP_TRIGGER_METHOD as MAP_TRIGGER_METHOD, ORDER_SIDE_TO_ORDER_ACTION as ORDER_SIDE_TO_ORDER_ACTION, timestring_to_timestamp as timestring_to_timestamp
from nautilus_trader.adapters.interactive_brokers.parsing.price_conversion import ib_price_to_nautilus_price as ib_price_to_nautilus_price, nautilus_price_to_ib_price as nautilus_price_to_ib_price
from nautilus_trader.adapters.interactive_brokers.providers import InteractiveBrokersInstrumentProvider as InteractiveBrokersInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogLevel as LogLevel
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.core.rust.common import LogColor as LogColor
from nautilus_trader.core.uuid import UUID4 as UUID4
from nautilus_trader.execution.messages import BatchCancelOrders as BatchCancelOrders, CancelAllOrders as CancelAllOrders, CancelOrder as CancelOrder, GenerateFillReports as GenerateFillReports, GenerateOrderStatusReport as GenerateOrderStatusReport, GenerateOrderStatusReports as GenerateOrderStatusReports, GeneratePositionStatusReports as GeneratePositionStatusReports, ModifyOrder as ModifyOrder, QueryAccount as QueryAccount, SubmitOrder as SubmitOrder, SubmitOrderList as SubmitOrderList
from nautilus_trader.execution.reports import ExecutionMassStatus as ExecutionMassStatus, FillReport as FillReport, OrderStatusReport as OrderStatusReport, PositionStatusReport as PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient as LiveExecutionClient
from nautilus_trader.model.enums import AccountType as AccountType, LiquiditySide as LiquiditySide, OmsType as OmsType, OrderSide as OrderSide, OrderStatus as OrderStatus, OrderType as OrderType, PositionSide as PositionSide, TimeInForce as TimeInForce, TrailingOffsetType as TrailingOffsetType, TriggerType as TriggerType, order_side_to_str as order_side_to_str, trailing_offset_type_to_str as trailing_offset_type_to_str
from nautilus_trader.model.identifiers import AccountId as AccountId, ClientId as ClientId, ClientOrderId as ClientOrderId, InstrumentId as InstrumentId, TradeId as TradeId, VenueOrderId as VenueOrderId, generic_spread_id_n_legs as generic_spread_id_n_legs, generic_spread_id_to_list as generic_spread_id_to_list, is_generic_spread_id as is_generic_spread_id
from nautilus_trader.model.instruments import Instrument as Instrument
from nautilus_trader.model.objects import AccountBalance as AccountBalance, Currency as Currency, MarginBalance as MarginBalance, Money as Money, Price as Price, Quantity as Quantity
from nautilus_trader.model.orders.base import Order as Order
from nautilus_trader.model.orders.limit_if_touched import LimitIfTouchedOrder as LimitIfTouchedOrder
from nautilus_trader.model.orders.market_if_touched import MarketIfTouchedOrder as MarketIfTouchedOrder
from nautilus_trader.model.orders.stop_limit import StopLimitOrder as StopLimitOrder
from nautilus_trader.model.orders.stop_market import StopMarketOrder as StopMarketOrder
from nautilus_trader.model.orders.trailing_stop_limit import TrailingStopLimitOrder as TrailingStopLimitOrder
from nautilus_trader.model.orders.trailing_stop_market import TrailingStopMarketOrder as TrailingStopMarketOrder

ib_to_nautilus_trigger_method: Incomplete
ib_to_nautilus_time_in_force: Incomplete
ib_to_nautilus_order_side: Incomplete
ib_to_nautilus_order_type: Incomplete

class InteractiveBrokersExecutionClient(LiveExecutionClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: InteractiveBrokersClient, account_id: AccountId, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: InteractiveBrokersInstrumentProvider, config: InteractiveBrokersExecClientConfig, name: str | None = None, connection_timeout: int = 300, track_option_exercise_from_position_update: bool = False) -> None: ...
    @property
    def instrument_provider(self) -> InteractiveBrokersInstrumentProvider: ...
    async def generate_order_status_report(self, command: GenerateOrderStatusReport) -> OrderStatusReport | None: ...
    async def generate_order_status_reports(self, command: GenerateOrderStatusReports) -> list[OrderStatusReport]: ...
    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]: ...
    async def generate_position_status_reports(self, command: GeneratePositionStatusReports) -> list[PositionStatusReport]: ...
    reconciliation_active: bool
    async def generate_mass_status(self, lookback_mins: int | None = None) -> ExecutionMassStatus | None: ...
    async def handle_order_status_report(self, ib_order: IBOrder) -> None: ...
