import nautilus_trader.model.instruments.base
from _typeshed import Incomplete
from decimal import Decimal
from typing import Any, ClassVar

__test__: dict

class OptionContract(nautilus_trader.model.instruments.base.Instrument):
    __pyx_vtable__: ClassVar[PyCapsule] = ...
    activation_ns: Incomplete
    activation_utc: OptionContract.activation_utc
    exchange: Incomplete
    expiration_ns: Incomplete
    expiration_utc: OptionContract.expiration_utc
    option_kind: Incomplete
    strike_price: Incomplete
    underlying: Incomplete
    def __init__(self, InstrumentIdinstrument_id, Symbolraw_symbol, AssetClassasset_class, Currencycurrency, intprice_precision, Priceprice_increment, Quantitymultiplier, Quantitylot_size, strunderlying, OptionKindoption_kind, Pricestrike_price, uint64_tactivation_ns, uint64_texpiration_ns, uint64_tts_event, uint64_tts_init, margin_init: Decimal | None = ..., margin_maint: Decimal | None = ..., maker_fee: Decimal | None = ..., taker_fee: Decimal | None = ..., strexchange=..., strtick_scheme_name=..., dictinfo=...) -> None: ...
    @staticmethod
    def from_dict(dictvalues) -> OptionContract: ...
    @staticmethod
    def from_pyo3(pyo3_instrument) -> OptionContract: ...
    @staticmethod
    def to_dict(OptionContractobj) -> dict[str, object]: ...
    def __reduce__(self): ...
    def __reduce_cython__(self) -> Any: ...
    def __setstate_cython__(self, __pyx_state) -> Any: ...
