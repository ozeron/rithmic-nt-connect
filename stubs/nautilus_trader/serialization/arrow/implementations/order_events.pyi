import pyarrow as pa
from nautilus_trader.common.config import msgspec_encoding_hook as msgspec_encoding_hook
from nautilus_trader.model.events import OrderFilled as OrderFilled, OrderInitialized as OrderInitialized
from nautilus_trader.serialization.arrow.schema import NAUTILUS_ARROW_SCHEMA as NAUTILUS_ARROW_SCHEMA

def serialize(event: OrderInitialized | OrderFilled) -> pa.RecordBatch: ...
def deserialize(cls): ...
