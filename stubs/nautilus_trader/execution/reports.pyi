from _typeshed import Incomplete
from datetime import datetime
from decimal import Decimal
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.core.message import Document as Document
from nautilus_trader.core.uuid import UUID4 as UUID4
from nautilus_trader.model.enums import ContingencyType as ContingencyType, LiquiditySide as LiquiditySide, OrderSide as OrderSide, OrderStatus as OrderStatus, OrderType as OrderType, PositionSide as PositionSide, TimeInForce as TimeInForce, TrailingOffsetType as TrailingOffsetType, TriggerType as TriggerType, contingency_type_to_str as contingency_type_to_str, liquidity_side_to_str as liquidity_side_to_str, order_side_to_str as order_side_to_str, order_status_to_str as order_status_to_str, order_type_to_str as order_type_to_str, position_side_to_str as position_side_to_str, time_in_force_to_str as time_in_force_to_str, trailing_offset_type_to_str as trailing_offset_type_to_str, trigger_type_to_str as trigger_type_to_str
from nautilus_trader.model.functions import contingency_type_from_pyo3 as contingency_type_from_pyo3, contingency_type_to_pyo3 as contingency_type_to_pyo3, liquidity_side_from_pyo3 as liquidity_side_from_pyo3, order_side_from_pyo3 as order_side_from_pyo3, order_side_to_pyo3 as order_side_to_pyo3, order_status_from_pyo3 as order_status_from_pyo3, order_status_to_pyo3 as order_status_to_pyo3, order_type_from_pyo3 as order_type_from_pyo3, order_type_to_pyo3 as order_type_to_pyo3, position_side_from_pyo3 as position_side_from_pyo3, time_in_force_from_pyo3 as time_in_force_from_pyo3, time_in_force_to_pyo3 as time_in_force_to_pyo3, trailing_offset_type_to_pyo3 as trailing_offset_type_to_pyo3, trigger_type_to_pyo3 as trigger_type_to_pyo3
from nautilus_trader.model.identifiers import AccountId as AccountId, ClientId as ClientId, ClientOrderId as ClientOrderId, InstrumentId as InstrumentId, OrderListId as OrderListId, PositionId as PositionId, TradeId as TradeId, Venue as Venue, VenueOrderId as VenueOrderId
from nautilus_trader.model.objects import Money as Money, Price as Price, Quantity as Quantity
from nautilus_trader.model.orders import Order as Order
from typing import Any

class ExecutionReport(Document):
    account_id: Incomplete
    instrument_id: Incomplete
    def __init__(self, account_id: AccountId, instrument_id: InstrumentId, report_id: UUID4, ts_init: int) -> None: ...

class OrderStatusReport(ExecutionReport):
    client_order_id: Incomplete
    order_list_id: Incomplete
    venue_order_id: Incomplete
    venue_position_id: Incomplete
    linked_order_ids: Incomplete
    parent_order_id: Incomplete
    order_side: Incomplete
    order_type: Incomplete
    contingency_type: Incomplete
    time_in_force: Incomplete
    expire_time: Incomplete
    order_status: Incomplete
    price: Incomplete
    trigger_price: Incomplete
    trigger_type: Incomplete
    limit_offset: Incomplete
    trailing_offset: Incomplete
    trailing_offset_type: Incomplete
    quantity: Incomplete
    filled_qty: Incomplete
    leaves_qty: Incomplete
    display_qty: Incomplete
    avg_px: Incomplete
    post_only: Incomplete
    reduce_only: Incomplete
    cancel_reason: Incomplete
    ts_accepted: Incomplete
    ts_triggered: Incomplete
    ts_last: Incomplete
    def __init__(self, account_id: AccountId, instrument_id: InstrumentId, venue_order_id: VenueOrderId, order_side: OrderSide, order_type: OrderType, time_in_force: TimeInForce, order_status: OrderStatus, quantity: Quantity, filled_qty: Quantity, report_id: UUID4, ts_accepted: int, ts_last: int, ts_init: int, client_order_id: ClientOrderId | None = None, order_list_id: OrderListId | None = None, venue_position_id: PositionId | None = None, linked_order_ids: list[ClientOrderId] | None = None, parent_order_id: ClientOrderId | None = None, contingency_type: ContingencyType = ..., expire_time: datetime | None = None, price: Price | None = None, trigger_price: Price | None = None, trigger_type: TriggerType = ..., limit_offset: Decimal | None = None, trailing_offset: Decimal | None = None, trailing_offset_type: TrailingOffsetType | None = None, avg_px: Decimal | None = None, display_qty: Quantity | None = None, post_only: bool = False, reduce_only: bool = False, cancel_reason: str | None = None, ts_triggered: int | None = None) -> None: ...
    @property
    def is_open(self) -> bool: ...
    def is_order_updated(self, order: Order) -> bool: ...
    def __eq__(self, other: object) -> bool: ...
    __hash__: None
    def to_dict(self) -> dict[str, Any]: ...
    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> OrderStatusReport: ...
    def to_pyo3(self) -> nautilus_pyo3.OrderStatusReport: ...
    @staticmethod
    def from_pyo3(pyo3_report: nautilus_pyo3.OrderStatusReport) -> OrderStatusReport: ...

class FillReport(ExecutionReport):
    client_order_id: Incomplete
    venue_order_id: Incomplete
    venue_position_id: Incomplete
    trade_id: Incomplete
    order_side: Incomplete
    last_qty: Incomplete
    last_px: Incomplete
    commission: Incomplete
    liquidity_side: Incomplete
    avg_px: Incomplete
    ts_event: Incomplete
    def __init__(self, account_id: AccountId, instrument_id: InstrumentId, venue_order_id: VenueOrderId, trade_id: TradeId, order_side: OrderSide, last_qty: Quantity, last_px: Price, commission: Money, liquidity_side: LiquiditySide, report_id: UUID4, ts_event: int, ts_init: int, avg_px: Decimal | None = None, client_order_id: ClientOrderId | None = None, venue_position_id: PositionId | None = None) -> None: ...
    def __eq__(self, other: object) -> bool: ...
    __hash__: None
    def to_dict(self) -> dict[str, Any]: ...
    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> FillReport: ...
    def to_pyo3(self) -> nautilus_pyo3.FillReport: ...
    @staticmethod
    def from_pyo3(pyo3_report: nautilus_pyo3.FillReport) -> FillReport: ...

class PositionStatusReport(ExecutionReport):
    venue_position_id: Incomplete
    position_side: Incomplete
    quantity: Incomplete
    avg_px_open: Incomplete
    signed_decimal_qty: Incomplete
    ts_last: Incomplete
    def __init__(self, account_id: AccountId, instrument_id: InstrumentId, position_side: PositionSide, quantity: Quantity, report_id: UUID4, ts_last: int, ts_init: int, venue_position_id: PositionId | None = None, avg_px_open: Decimal | None = None) -> None: ...
    @staticmethod
    def create_flat(account_id: AccountId, instrument_id: InstrumentId, size_precision: int, ts_init: int, report_id: UUID4 | None = None) -> PositionStatusReport: ...
    def to_dict(self) -> dict[str, Any]: ...
    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> PositionStatusReport: ...
    def to_pyo3(self) -> nautilus_pyo3.PositionStatusReport: ...
    @staticmethod
    def from_pyo3(pyo3_report: nautilus_pyo3.PositionStatusReport) -> PositionStatusReport: ...

class ExecutionMassStatus(Document):
    client_id: Incomplete
    account_id: Incomplete
    venue: Incomplete
    def __init__(self, client_id: ClientId, account_id: AccountId, venue: Venue | None, report_id: UUID4, ts_init: int) -> None: ...
    @property
    def order_reports(self) -> dict[VenueOrderId, OrderStatusReport]: ...
    @property
    def fill_reports(self) -> dict[VenueOrderId, list[FillReport]]: ...
    @property
    def position_reports(self) -> dict[InstrumentId, list[PositionStatusReport]]: ...
    def add_order_reports(self, reports: list[OrderStatusReport]) -> None: ...
    def add_fill_reports(self, reports: list[FillReport]) -> None: ...
    def add_position_reports(self, reports: list[PositionStatusReport]) -> None: ...
    def to_dict(self) -> dict[str, Any]: ...
    def to_pyo3(self) -> nautilus_pyo3.ExecutionMassStatus: ...
    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> ExecutionMassStatus: ...
