import nautilus_trader.model.instruments.base
from _typeshed import Incomplete
from decimal import Decimal
from typing import Any, ClassVar

from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

__test__: dict

class FuturesContract(nautilus_trader.model.instruments.base.Instrument):
    __pyx_vtable__: ClassVar[PyCapsule] = ...
    activation_ns: Incomplete
    activation_utc: FuturesContract.activation_utc
    exchange: Incomplete
    expiration_ns: Incomplete
    expiration_utc: FuturesContract.expiration_utc
    underlying: Incomplete
    # Constructor hand-ported from futures_contract.pyx (stubgen mangles cdef names).
    def __init__(
        self,
        instrument_id: InstrumentId,
        raw_symbol: Symbol,
        asset_class: AssetClass,
        currency: Currency,
        price_precision: int,
        price_increment: Price,
        multiplier: Quantity,
        lot_size: Quantity,
        underlying: str,
        activation_ns: int,
        expiration_ns: int,
        ts_event: int,
        ts_init: int,
        margin_init: Decimal | None = None,
        margin_maint: Decimal | None = None,
        maker_fee: Decimal | None = None,
        taker_fee: Decimal | None = None,
        exchange: str | None = None,
        tick_scheme_name: str | None = None,
        info: dict | None = None,
    ) -> None: ...
    @staticmethod
    def from_dict(dictvalues) -> FuturesContract: ...
    @staticmethod
    def from_pyo3(pyo3_instrument) -> FuturesContract: ...
    @staticmethod
    def to_dict(FuturesContractobj) -> dict[str, object]: ...
    def __reduce__(self): ...
    def __reduce_cython__(self) -> Any: ...
    def __setstate_cython__(self, __pyx_state) -> Any: ...
