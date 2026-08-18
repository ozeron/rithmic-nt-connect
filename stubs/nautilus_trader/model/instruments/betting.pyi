import _cython_3_2_9
import nautilus_trader.model.instruments.base
from _typeshed import Incomplete
from decimal import Decimal
from typing import Any, ClassVar

__pyx_capi__: dict
__test__: dict
make_symbol: _cython_3_2_9.cython_function_or_method
null_handicap: _cython_3_2_9.cython_function_or_method
order_side_to_bet_side: _cython_3_2_9.cython_function_or_method

class BettingInstrument(nautilus_trader.model.instruments.base.Instrument):
    __pyx_vtable__: ClassVar[PyCapsule] = ...
    betting_type: Incomplete
    competition_id: Incomplete
    competition_name: Incomplete
    event_country_code: Incomplete
    event_id: Incomplete
    event_name: Incomplete
    event_open_date: Incomplete
    event_type_id: Incomplete
    event_type_name: Incomplete
    market_id: Incomplete
    market_name: Incomplete
    market_start_time: Incomplete
    market_type: Incomplete
    selection_handicap: Incomplete
    selection_id: Incomplete
    selection_name: Incomplete
    def __init__(self, strvenue_name, intevent_type_id, strevent_type_name, intcompetition_id, strcompetition_name, intevent_id, strevent_name, strevent_country_code, datetimeevent_open_date, strbetting_type, strmarket_id, strmarket_name, datetimemarket_start_time, strmarket_type, intselection_id, strselection_name, strcurrency, floatselection_handicap, int8_tprice_precision, int8_tsize_precision, uint64_tts_event, uint64_tts_init, Quantitymax_quantity: Quantity | None = ..., Quantitymin_quantity: Quantity | None = ..., Moneymax_notional: Money | None = ..., Moneymin_notional: Money | None = ..., Pricemax_price: Price | None = ..., Pricemin_price: Price | None = ..., margin_init: Decimal | None = ..., margin_maint: Decimal | None = ..., maker_fee: Decimal | None = ..., taker_fee: Decimal | None = ..., strtick_scheme_name=..., dictinfo=...) -> None: ...
    @staticmethod
    def from_dict(dictvalues) -> BettingInstrument: ...
    def notional_value(self, Quantityquantity, Priceprice, booluse_quote_for_inverse=..., Currencytarget_currency=..., Priceconversion_price=...) -> Money: ...
    @staticmethod
    def to_dict(BettingInstrumentobj) -> dict[str, object]: ...
    def __reduce__(self): ...
    def __reduce_cython__(self) -> Any: ...
    def __setstate_cython__(self, __pyx_state) -> Any: ...
