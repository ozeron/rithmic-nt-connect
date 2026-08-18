import nautilus_trader.model.orders.base
from _typeshed import Incomplete
from decimal import Decimal
from typing import Any, ClassVar

__test__: dict

class TrailingStopLimitOrder(nautilus_trader.model.orders.base.Order):
    __pyx_vtable__: ClassVar[PyCapsule] = ...
    activation_price: Incomplete
    display_qty: Incomplete
    expire_time: Incomplete
    expire_time_ns: Incomplete
    is_activated: Incomplete
    is_triggered: Incomplete
    limit_offset: Incomplete
    price: Any
    trailing_offset: Incomplete
    trailing_offset_type: Incomplete
    trigger_price: Incomplete
    trigger_type: Incomplete
    ts_triggered: Incomplete
    def __init__(self, TraderIdtrader_id, StrategyIdstrategy_id, InstrumentIdinstrument_id, ClientOrderIdclient_order_id, OrderSideorder_side, Quantityquantity, Priceprice: Price | None, Pricetrigger_price: Price | None, TriggerTypetrigger_type, limit_offset: Decimal, trailing_offset: Decimal, TrailingOffsetTypetrailing_offset_type, UUID4init_id, uint64_tts_init, Priceactivation_price: Price | None = ..., TimeInForcetime_in_force=..., uint64_texpire_time_ns=..., boolpost_only=..., boolreduce_only=..., boolquote_quantity=..., Quantitydisplay_qty=..., TriggerTypeemulation_trigger=..., InstrumentIdtrigger_instrument_id=..., ContingencyTypecontingency_type=..., OrderListIdorder_list_id=..., listlinked_order_ids=..., ClientOrderIdparent_order_id=..., ExecAlgorithmIdexec_algorithm_id=..., dictexec_algorithm_params=..., ClientOrderIdexec_spawn_id=..., listtags=...) -> Any: ...
    @staticmethod
    def create(init) -> Any: ...
    def info(self) -> str: ...
    def to_dict(self) -> dict: ...
    def __reduce__(self): ...
    def __reduce_cython__(self) -> Any: ...
    def __setstate_cython__(self, __pyx_state) -> Any: ...
