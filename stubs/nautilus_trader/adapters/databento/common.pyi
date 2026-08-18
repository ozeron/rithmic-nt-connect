from nautilus_trader.adapters.databento.enums import DatabentoSchema as DatabentoSchema
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.model.data import BarType as BarType
from nautilus_trader.model.enums import BarAggregation as BarAggregation, PriceType as PriceType
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId

def instrument_id_to_pyo3(instrument_id: InstrumentId | nautilus_pyo3.InstrumentId) -> nautilus_pyo3.InstrumentId: ...
def databento_schema_from_nautilus_bar_type(bar_type: BarType) -> DatabentoSchema: ...
