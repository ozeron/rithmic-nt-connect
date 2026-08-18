from collections.abc import Iterable
from nautilus_trader.adapters.hyperliquid.enums import DEFAULT_PRODUCT_TYPES as DEFAULT_PRODUCT_TYPES, HyperliquidProductType as HyperliquidProductType
from nautilus_trader.common.providers import InstrumentProvider as InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId
from nautilus_trader.model.instruments import BinaryOption as BinaryOption, CryptoPerpetual as CryptoPerpetual, CurrencyPair as CurrencyPair, Instrument as Instrument, instruments_from_pyo3 as instruments_from_pyo3
from typing import Any

class HyperliquidInstrumentProvider(InstrumentProvider):
    def __init__(self, client: nautilus_pyo3.HyperliquidHttpClient, config: InstrumentProviderConfig | None = None, *, product_types: Iterable[HyperliquidProductType] | None = None) -> None: ...
    def instruments_pyo3(self) -> list[Any]: ...
    async def load_all_async(self, filters: dict | None = None) -> None: ...
