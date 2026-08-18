import pyarrow as pa
from nautilus_trader.common.config import msgspec_encoding_hook as msgspec_encoding_hook
from nautilus_trader.common.messages import ComponentStateChanged as ComponentStateChanged, TradingStateChanged as TradingStateChanged
from nautilus_trader.serialization.arrow.schema import NAUTILUS_ARROW_SCHEMA as NAUTILUS_ARROW_SCHEMA

def serialize(event: ComponentStateChanged | TradingStateChanged) -> pa.RecordBatch: ...
def deserialize(cls): ...
