from dataclasses import dataclass
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.nautilus_pyo3 import GreeksConvention as GreeksConvention, OKXGreeksType as OKXGreeksType
from nautilus_trader.model.identifiers import ClientOrderId as ClientOrderId, InstrumentId as InstrumentId
from typing import Final

OkxInstrument = nautilus_pyo3.CurrencyPair | nautilus_pyo3.CryptoPerpetual | nautilus_pyo3.CryptoFuture | nautilus_pyo3.CryptoFuturesSpread | nautilus_pyo3.CryptoOption | nautilus_pyo3.CryptoOptionSpread | nautilus_pyo3.BinaryOption
OKX_INSTRUMENT_TYPES: Final[tuple[type[nautilus_pyo3.CurrencyPair], type[nautilus_pyo3.CryptoPerpetual], type[nautilus_pyo3.CryptoFuture], type[nautilus_pyo3.CryptoFuturesSpread], type[nautilus_pyo3.CryptoOption], type[nautilus_pyo3.CryptoOptionSpread], type[nautilus_pyo3.BinaryOption]]]
GREEKS_CONVENTION_TO_TYPE: Final[dict[GreeksConvention, OKXGreeksType]]

@dataclass(frozen=True)
class OKXAttachedOcoBinding:
    parent_client_order_id: ClientOrderId
    attach_client_order_id: ClientOrderId
    instrument_id: InstrumentId
    sl_client_order_id: ClientOrderId | None
    tp_client_order_id: ClientOrderId | None
    def child_client_order_ids(self) -> list[ClientOrderId]: ...
    def all_client_order_ids(self) -> list[ClientOrderId]: ...
