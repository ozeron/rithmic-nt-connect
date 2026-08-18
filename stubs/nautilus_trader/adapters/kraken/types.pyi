from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from typing import Final

KrakenInstrument = nautilus_pyo3.CurrencyPair | nautilus_pyo3.CryptoPerpetual | nautilus_pyo3.TokenizedAsset
KRAKEN_INSTRUMENT_TYPES: Final[tuple[type[nautilus_pyo3.CurrencyPair], type[nautilus_pyo3.CryptoPerpetual], type[nautilus_pyo3.TokenizedAsset]]]
