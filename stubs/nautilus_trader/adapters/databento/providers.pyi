import datetime as dt
import pandas as pd
from nautilus_trader.adapters.databento.common import instrument_id_to_pyo3 as instrument_id_to_pyo3
from nautilus_trader.adapters.databento.constants import ALL_SYMBOLS as ALL_SYMBOLS, PUBLISHERS_FILEPATH as PUBLISHERS_FILEPATH
from nautilus_trader.adapters.databento.enums import DatabentoSchema as DatabentoSchema
from nautilus_trader.adapters.databento.loaders import DatabentoDataLoader as DatabentoDataLoader
from nautilus_trader.common.component import LiveClock as LiveClock
from nautilus_trader.common.enums import LogColor as LogColor
from nautilus_trader.common.providers import InstrumentProvider as InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig as InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId
from nautilus_trader.model.instruments import Instrument as Instrument, instruments_from_pyo3 as instruments_from_pyo3

class DatabentoInstrumentProvider(InstrumentProvider):
    def __init__(self, http_client: nautilus_pyo3.DatabentoHistoricalClient, clock: LiveClock, live_api_key: str | None = None, live_gateway: str | None = None, loader: DatabentoDataLoader | None = None, config: InstrumentProviderConfig | None = None, use_exchange_as_venue: bool = True) -> None: ...
    async def load_all_async(self, filters: dict | None = None) -> None: ...
    async def load_ids_async(self, instrument_ids: list[InstrumentId], filters: dict | None = None) -> None: ...
    async def load_async(self, instrument_id: InstrumentId, filters: dict | None = None) -> None: ...
    async def get_range(self, instrument_ids: list[InstrumentId], start: pd.Timestamp | dt.date | str | int, end: pd.Timestamp | dt.date | str | int | None = None, filters: dict | None = None) -> list[Instrument]: ...
