from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from typing import Final

BybitInstrument = nautilus_pyo3.CurrencyPair | nautilus_pyo3.CryptoPerpetual | nautilus_pyo3.CryptoFuture | nautilus_pyo3.CryptoOption
BYBIT_INSTRUMENT_TYPES: Final[tuple[type[nautilus_pyo3.CurrencyPair], type[nautilus_pyo3.CryptoPerpetual], type[nautilus_pyo3.CryptoFuture], type[nautilus_pyo3.CryptoOption]]]
