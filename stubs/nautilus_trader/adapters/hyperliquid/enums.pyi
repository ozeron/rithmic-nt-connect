from _typeshed import Incomplete
from enum import Enum

class HyperliquidProductType(str, Enum):
    SPOT = 'spot'
    PERP = 'perp'
    PERP_HIP3 = 'perp_hip3'
    OUTCOME = 'outcome'
    @property
    def is_spot(self) -> bool: ...
    @property
    def is_perp(self) -> bool: ...
    @property
    def is_perp_hip3(self) -> bool: ...
    @property
    def is_outcome(self) -> bool: ...

DEFAULT_PRODUCT_TYPES: Incomplete
