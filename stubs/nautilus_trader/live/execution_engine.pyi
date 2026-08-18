import asyncio
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor, LogLevel as LogLevel
from nautilus_trader.config import LiveExecEngineConfig as LiveExecEngineConfig
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.core.datetime import dt_to_unix_nanos as dt_to_unix_nanos, millis_to_nanos as millis_to_nanos, secs_to_nanos as secs_to_nanos
from nautilus_trader.core.fsm import InvalidStateTrigger as InvalidStateTrigger
from nautilus_trader.core.message import Command as Command
from nautilus_trader.core.uuid import UUID4 as UUID4
from nautilus_trader.execution.client import ExecutionClient as ExecutionClient
from nautilus_trader.execution.engine import ExecutionEngine as ExecutionEngine
from nautilus_trader.execution.messages import GenerateExecutionMassStatus as GenerateExecutionMassStatus, GenerateFillReports as GenerateFillReports, GenerateOrderStatusReport as GenerateOrderStatusReport, GenerateOrderStatusReports as GenerateOrderStatusReports, GeneratePositionStatusReports as GeneratePositionStatusReports, QueryOrder as QueryOrder
from nautilus_trader.execution.reports import ExecutionMassStatus as ExecutionMassStatus, ExecutionReport as ExecutionReport, FillReport as FillReport, OrderStatusReport as OrderStatusReport, PositionStatusReport as PositionStatusReport
from nautilus_trader.live.enqueue import ThrottledEnqueuer as ThrottledEnqueuer
from nautilus_trader.live.reconciliation import adjust_fills_for_partial_window as adjust_fills_for_partial_window, calculate_reconciliation_price as calculate_reconciliation_price, create_inferred_order_filled_event as create_inferred_order_filled_event, create_order_accepted_event as create_order_accepted_event, create_order_canceled_event as create_order_canceled_event, create_order_expired_event as create_order_expired_event, create_order_filled_event as create_order_filled_event, create_order_rejected_event as create_order_rejected_event, create_order_triggered_event as create_order_triggered_event, create_order_updated_event as create_order_updated_event, get_existing_fill_for_trade_id as get_existing_fill_for_trade_id, is_within_single_unit_tolerance as is_within_single_unit_tolerance
from nautilus_trader.model.book import py_should_handle_own_book_order as py_should_handle_own_book_order
from nautilus_trader.model.enums import OrderSide as OrderSide, OrderStatus as OrderStatus, OrderType as OrderType, PositionSide as PositionSide, TimeInForce as TimeInForce, TriggerType as TriggerType, trailing_offset_type_to_str as trailing_offset_type_to_str, trigger_type_to_str as trigger_type_to_str
from nautilus_trader.model.events import OrderEvent as OrderEvent, OrderFilled as OrderFilled, OrderInitialized as OrderInitialized
from nautilus_trader.model.identifiers import AccountId as AccountId, ClientId as ClientId, ClientOrderId as ClientOrderId, InstrumentId as InstrumentId, PositionId as PositionId, StrategyId as StrategyId, TradeId as TradeId, Venue as Venue, VenueOrderId as VenueOrderId
from nautilus_trader.model.instruments import CurrencyPair as CurrencyPair, Instrument as Instrument
from nautilus_trader.model.objects import Price as Price, Quantity as Quantity
from nautilus_trader.model.orders import Order as Order, OrderUnpacker as OrderUnpacker
from nautilus_trader.model.position import Position as Position

InstrumentAccountKey = tuple[InstrumentId, AccountId]

class LiveExecutionEngine(ExecutionEngine):
    reconciliation_lookback_mins: int
    reconciliation_instrument_ids: list[InstrumentId]
    filter_unclaimed_external_orders: bool
    filter_position_reports: bool
    filtered_client_order_ids: list[ClientOrderId]
    generate_missing_orders: bool
    inflight_check_interval_ms: int
    inflight_check_threshold_ms: int
    inflight_check_max_retries: int
    own_books_audit_interval_secs: float | None
    open_check_interval_secs: float | None
    open_check_open_only: bool
    open_check_lookback_mins: int
    open_check_threshold_ms: int
    open_check_missing_retries: int
    max_single_order_queries_per_cycle: int
    single_order_query_delay_ms: int
    position_check_interval_secs: float | None
    position_check_lookback_mins: int
    position_check_threshold_ms: int
    position_check_retries: int
    reconciliation_startup_delay_secs: float
    graceful_shutdown_on_exception: bool
    def __init__(self, loop: asyncio.AbstractEventLoop, msgbus: MessageBus, cache: Cache, clock: LiveClock, config: LiveExecEngineConfig | None = None) -> None: ...
    @property
    def reconciliation(self) -> bool: ...
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def get_cmd_queue_task(self) -> asyncio.Task | None: ...
    def get_evt_queue_task(self) -> asyncio.Task | None: ...
    def get_own_books_audit_task(self) -> asyncio.Task | None: ...
    def get_reconciliation_task(self) -> asyncio.Task | None: ...
    def cmd_qsize(self) -> int: ...
    def evt_qsize(self) -> int: ...
    def kill(self) -> None: ...
    def execute(self, command: Command) -> None: ...
    def process(self, event: OrderEvent) -> None: ...
    def generate_execution_mass_status(self, command: GenerateExecutionMassStatus) -> None: ...
    async def reconcile_execution_state(self, timeout_secs: float = 10.0) -> bool: ...
    def reconcile_execution_report(self, report: ExecutionReport) -> bool: ...
    def reconcile_execution_mass_status(self, report: ExecutionMassStatus) -> None: ...
