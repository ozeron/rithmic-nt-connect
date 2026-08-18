import pandas as pd
from _typeshed import Incomplete
from nautilus_trader.common.actor import Actor as Actor
from nautilus_trader.common.config import ActorConfig as ActorConfig
from nautilus_trader.core.datetime import unix_nanos_to_dt as unix_nanos_to_dt
from nautilus_trader.model.data import DataType as DataType
from nautilus_trader.model.greeks_data import YieldCurveData as YieldCurveData
from os import PathLike
from typing import Any

class CSVTickDataLoader:
    @staticmethod
    def load(file_path: PathLike[str] | str, index_col: str | int = 'timestamp', parse_dates: bool = True, datetime_format: str = 'mixed', **kwargs: Any) -> pd.DataFrame: ...

class CSVBarDataLoader:
    @staticmethod
    def load(file_path: PathLike[str] | str, index_col: str | int = 'timestamp', parse_dates: bool = True, **kwargs: Any) -> pd.DataFrame: ...

class ParquetTickDataLoader:
    @staticmethod
    def load(file_path: PathLike[str] | str, timestamp_column: str = 'timestamp') -> pd.DataFrame: ...

class ParquetBarDataLoader:
    @staticmethod
    def load(file_path: PathLike[str] | str) -> pd.DataFrame: ...

class InterestRateProviderConfig(ActorConfig, frozen=True):
    interest_rates_file: str
    curve_name: str = ...

class InterestRateProvider(Actor):
    interest_rates_df: Incomplete
    def __init__(self, config: InterestRateProviderConfig) -> None: ...
    def on_start(self) -> None: ...
    def update_interest_rate(self, alert=None) -> None: ...
    def on_stop(self) -> None: ...

def import_interest_rates(xml_interest_rate_file): ...
def next_month_start_from_timestamp(timestamp): ...
