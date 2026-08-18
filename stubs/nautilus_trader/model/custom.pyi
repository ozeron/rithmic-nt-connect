from nautilus_trader.core.datetime import unix_nanos_to_iso8601 as unix_nanos_to_iso8601
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId
from nautilus_trader.serialization.arrow.serializer import register_arrow as register_arrow
from nautilus_trader.serialization.base import register_serializable_type as register_serializable_type

def customdataclass(*args, **kwargs): ...
def customdataclass_pyo3(*args, **kwargs): ...
