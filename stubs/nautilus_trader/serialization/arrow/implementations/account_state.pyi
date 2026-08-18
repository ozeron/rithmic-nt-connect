import pyarrow as pa
from _typeshed import Incomplete
from nautilus_trader.common.config import msgspec_encoding_hook as msgspec_encoding_hook
from nautilus_trader.model.events import AccountState as AccountState
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId
from nautilus_trader.model.objects import Currency as Currency
from pyarrow import RecordBatch as RecordBatch

def serialize(state: AccountState) -> RecordBatch: ...
def deserialize(data: pa.RecordBatch) -> list[AccountState]: ...

SCHEMA: Incomplete
