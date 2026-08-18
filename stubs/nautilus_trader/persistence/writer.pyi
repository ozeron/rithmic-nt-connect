import datetime as dt
import fsspec
import pandas as pd
from _typeshed import Incomplete
from enum import Enum
from fsspec.compression import AbstractBufferedFile as AbstractBufferedFile
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import Clock as Clock, Logger as Logger
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.core.uuid import UUID4 as UUID4
from nautilus_trader.model.data import Bar as Bar, CustomData as CustomData, FundingRateUpdate as FundingRateUpdate, IndexPriceUpdate as IndexPriceUpdate, MarkPriceUpdate as MarkPriceUpdate, OrderBookDelta as OrderBookDelta, OrderBookDeltas as OrderBookDeltas, OrderBookDepth10 as OrderBookDepth10, QuoteTick as QuoteTick, TradeTick as TradeTick
from nautilus_trader.model.instruments import Instrument as Instrument
from nautilus_trader.persistence.funcs import class_to_filename as class_to_filename, urisafe_identifier as urisafe_identifier
from nautilus_trader.serialization.arrow.serializer import ArrowSerializer as ArrowSerializer, list_schemas as list_schemas
from pyarrow import RecordBatchStreamWriter as RecordBatchStreamWriter
from typing import Any

class RotationMode(Enum):
    SIZE = 0
    INTERVAL = 1
    SCHEDULED_DATES = 2
    NO_ROTATION = 3

class StreamingFeatherWriter:
    path: Incomplete
    cache: Incomplete
    clock: Incomplete
    fs: fsspec.AbstractFileSystem
    include_types: Incomplete
    log: Incomplete
    rotation_mode: Incomplete
    max_file_size: Incomplete
    rotation_interval: Incomplete
    rotation_time: Incomplete
    rotation_timezone: Incomplete
    flush_interval_ms: Incomplete
    missing_writers: set[type]
    def __init__(self, path: str, cache: Cache, clock: Clock, fs_protocol: str | None = 'file', flush_interval_ms: int | None = None, replace: bool = False, include_types: list[type] | None = None, rotation_mode: RotationMode = ..., max_file_size: int = ..., rotation_interval: pd.Timedelta | None = None, rotation_time: dt.time = ..., rotation_timezone: str = 'UTC') -> None: ...
    def write(self, obj: object) -> None: ...
    def check_flush(self) -> None: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...
    def get_current_file_info(self) -> dict[str | tuple[str, str], dict[str, Any]]: ...
    def get_next_rotation_time(self, table_name: str | tuple[str, str]) -> pd.Timestamp | None: ...
    @property
    def is_closed(self) -> bool: ...
