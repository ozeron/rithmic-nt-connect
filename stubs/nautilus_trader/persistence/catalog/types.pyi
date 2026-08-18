from dataclasses import dataclass
from nautilus_trader.core.data import Data as Data
from nautilus_trader.model.identifiers import ClientId as ClientId
from nautilus_trader.model.instruments import Instrument as Instrument

@dataclass(frozen=True)
class CatalogDataResult:
    data_cls: type
    data: list[Data]
    instruments: list[Instrument] | None = ...
    client_id: ClientId | None = ...
