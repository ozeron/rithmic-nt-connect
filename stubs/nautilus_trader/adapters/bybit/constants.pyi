from nautilus_trader.core.nautilus_pyo3 import BybitProductType as BybitProductType
from nautilus_trader.model.identifiers import ClientId as ClientId, Venue as Venue
from typing import Final

BYBIT: Final[str]
BYBIT_VENUE: Final[Venue]
BYBIT_CLIENT_ID: Final[ClientId]
BYBIT_ALL_PRODUCTS: Final[tuple[BybitProductType, ...]]
BYBIT_MULTIPLIERS: Final[list[int]]
