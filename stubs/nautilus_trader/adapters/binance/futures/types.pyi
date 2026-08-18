from _typeshed import Incomplete
from decimal import Decimal
from nautilus_trader.core.data import Data as Data
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId
from nautilus_trader.model.objects import Price as Price
from typing import Any

class BinanceFuturesMarkPriceUpdate(Data):
    instrument_id: Incomplete
    mark: Incomplete
    index: Incomplete
    estimated_settle: Incomplete
    funding_rate: Incomplete
    next_funding_ns: Incomplete
    def __init__(self, instrument_id: InstrumentId, mark: Price, index: Price, estimated_settle: Price, funding_rate: Decimal, next_funding_ns: int, ts_event: int, ts_init: int) -> None: ...
    @property
    def ts_event(self) -> int: ...
    @property
    def ts_init(self) -> int: ...
    @staticmethod
    def from_dict(values: dict[str, Any]) -> BinanceFuturesMarkPriceUpdate: ...
    @staticmethod
    def to_dict(obj: BinanceFuturesMarkPriceUpdate) -> dict[str, Any]: ...
