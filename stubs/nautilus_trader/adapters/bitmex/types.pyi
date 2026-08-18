from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from typing import Final

BitmexInstrument = nautilus_pyo3.CurrencyPair | nautilus_pyo3.CryptoPerpetual | nautilus_pyo3.CryptoFuture
BITMEX_INSTRUMENT_TYPES: Final[tuple[type[nautilus_pyo3.CurrencyPair], type[nautilus_pyo3.CryptoPerpetual], type[nautilus_pyo3.CryptoFuture]]]
