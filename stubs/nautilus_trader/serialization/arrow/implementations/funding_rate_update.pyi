import pyarrow as pa
from nautilus_trader.common.config import msgspec_encoding_hook as msgspec_encoding_hook
from nautilus_trader.model.data import FundingRateUpdate as FundingRateUpdate
from nautilus_trader.serialization.arrow.schema import NAUTILUS_ARROW_SCHEMA as NAUTILUS_ARROW_SCHEMA

def serialize(funding_rate: FundingRateUpdate) -> pa.RecordBatch: ...
def deserialize(batch: pa.RecordBatch) -> list[FundingRateUpdate]: ...
