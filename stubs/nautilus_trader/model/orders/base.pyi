import nautilus_pyo3
from _typeshed import Incomplete
from nautilus_trader.model.functions import order_status_to_str as order_status_to_str
from typing import Any, ClassVar

__pyx_capi__: dict
__test__: dict

class Order:
    __pyx_vtable__: ClassVar[PyCapsule] = ...
    account_id: Incomplete
    avg_px: Incomplete
    client_order_id: Incomplete
    contingency_type: Incomplete
    emulation_trigger: Incomplete
    event_count: Incomplete
    events: Incomplete
    exec_algorithm_id: Incomplete
    exec_algorithm_params: Incomplete
    exec_spawn_id: Incomplete
    filled_qty: Incomplete
    has_activation_price: Incomplete
    has_price: Incomplete
    has_trigger_price: Incomplete
    # ``price``/``trigger_price`` live on the concrete limit/stop subclasses,
    # not the base; declared here so adapters can read them guarded by
    # ``has_price``/``has_trigger_price``. ``trigger_type`` is deliberately
    # absent: only stop orders expose it, and unguarded access must stay an error.
    price: Incomplete
    trigger_price: Incomplete
    init_event: Incomplete
    init_id: Incomplete
    instrument_id: Incomplete
    is_active_local: Incomplete
    is_aggressive: Incomplete
    is_buy: Incomplete
    is_canceled: Incomplete
    is_child_order: Incomplete
    is_closed: Incomplete
    is_contingency: Incomplete
    is_emulated: Incomplete
    is_inflight: Incomplete
    is_open: Incomplete
    is_parent_order: Incomplete
    is_passive: Incomplete
    is_pending_cancel: Incomplete
    is_pending_update: Incomplete
    is_post_only: Incomplete
    is_primary: Incomplete
    is_quote_quantity: Incomplete
    is_reduce_only: Incomplete
    is_sell: Incomplete
    is_spawned: Incomplete
    last_event: Incomplete
    last_trade_id: Incomplete
    leaves_qty: Incomplete
    linked_order_ids: Incomplete
    liquidity_side: Incomplete
    order_list_id: Incomplete
    order_type: Incomplete
    overfill_qty: Incomplete
    parent_order_id: Incomplete
    position_id: Incomplete
    quantity: Incomplete
    side: Incomplete
    slippage: Incomplete
    status: Incomplete
    strategy_id: Incomplete
    symbol: Incomplete
    tags: Incomplete
    time_in_force: Incomplete
    trade_ids: Incomplete
    trader_id: Incomplete
    trigger_instrument_id: Incomplete
    ts_accepted: Incomplete
    ts_closed: Incomplete
    ts_init: Incomplete
    ts_last: Incomplete
    ts_submitted: Incomplete
    venue: Incomplete
    venue_order_id: Incomplete
    venue_order_ids: Incomplete
    def __init__(self, OrderInitializedinit) -> Any: ...
    def apply(self, OrderEventevent) -> void: ...
    @staticmethod
    def closing_side(PositionSideposition_side) -> OrderSide: ...
    def commissions(self) -> list: ...
    def info(self) -> str: ...
    def is_duplicate_fill(self, OrderFilledfill) -> bool: ...
    @staticmethod
    def opposite_side(OrderSideside) -> OrderSide: ...
    def set_quote_quantity(self, boolvalue) -> void: ...
    def side_string(self) -> str: ...
    def signed_decimal_qty(self) -> Any: ...
    def status_string(self) -> str: ...
    def tif_string(self) -> str: ...
    def to_dict(self) -> dict: ...
    def to_own_book_order(self) -> nautilus_pyo3.OwnBookOrder: ...
    def type_string(self) -> str: ...
    def would_reduce_only(self, PositionSideposition_side, Quantityposition_qty) -> bool: ...
    def __eq__(self, other: object) -> bool: ...
    def __ge__(self, other: object) -> bool: ...
    def __gt__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
    def __le__(self, other: object) -> bool: ...
    def __lt__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...
    def __reduce__(self): ...
    def __reduce_cython__(self) -> Any: ...
    def __setstate_cython__(self, __pyx_state) -> Any: ...
