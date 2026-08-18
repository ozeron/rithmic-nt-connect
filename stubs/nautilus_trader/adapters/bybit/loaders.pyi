import pandas as pd
from nautilus_trader.core.nautilus_pyo3 import BybitProductType as BybitProductType
from nautilus_trader.model.enums import RecordFlag as RecordFlag
from os import PathLike

class BybitOrderBookDeltaDataLoader:
    @classmethod
    def load(cls, file_path: PathLike[str] | str, nrows: int | None = None, product_type: BybitProductType = ...) -> pd.DataFrame: ...
    @classmethod
    def map_actions(cls, update_type: str, size: float) -> str: ...
    @classmethod
    def map_sides(cls, side: str) -> str: ...
    @classmethod
    def map_flags(cls, update_type: str) -> int: ...
