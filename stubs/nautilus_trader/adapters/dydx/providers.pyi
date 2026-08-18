from nautilus_trader.common.providers import InstrumentProvider as InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.model.instruments import instruments_from_pyo3 as instruments_from_pyo3
from typing import Any

class DydxInstrumentProvider(InstrumentProvider):
    def __init__(self, client: nautilus_pyo3.DydxHttpClient, config: InstrumentProviderConfig | None = None) -> None: ...
    def instruments_pyo3(self) -> list[Any]: ...
    async def load_all_async(self, filters: dict | None = None) -> None: ...
