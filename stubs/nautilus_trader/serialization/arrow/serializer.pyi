import pyarrow as pa
from _typeshed import Incomplete
from collections.abc import Callable
from nautilus_trader.common.messages import ComponentStateChanged as ComponentStateChanged, ShutdownSystem as ShutdownSystem, TradingStateChanged as TradingStateChanged
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.core.data import Data as Data
from nautilus_trader.core.message import Event as Event
from nautilus_trader.model.data import Bar as Bar, CustomData as CustomData, FundingRateUpdate as FundingRateUpdate, IndexPriceUpdate as IndexPriceUpdate, InstrumentClose as InstrumentClose, MarkPriceUpdate as MarkPriceUpdate, OptionGreeks as OptionGreeks, OrderBookDelta as OrderBookDelta, OrderBookDeltas as OrderBookDeltas, OrderBookDepth10 as OrderBookDepth10, QuoteTick as QuoteTick, TradeTick as TradeTick
from nautilus_trader.model.events import AccountState as AccountState, OrderFilled as OrderFilled, OrderInitialized as OrderInitialized, PositionEvent as PositionEvent
from nautilus_trader.model.instruments import Instrument as Instrument
from nautilus_trader.persistence.wranglers_v2 import BarDataWranglerV2 as BarDataWranglerV2, OrderBookDeltaDataWranglerV2 as OrderBookDeltaDataWranglerV2, OrderBookDepth10DataWranglerV2 as OrderBookDepth10DataWranglerV2, QuoteTickDataWranglerV2 as QuoteTickDataWranglerV2, TradeTickDataWranglerV2 as TradeTickDataWranglerV2
from nautilus_trader.serialization.arrow.implementations import account_state as account_state, component_commands as component_commands, component_events as component_events, funding_rate_update as funding_rate_update, instruments as instruments, order_events as order_events, position_events as position_events
from nautilus_trader.serialization.arrow.schema import NAUTILUS_ARROW_SCHEMA as NAUTILUS_ARROW_SCHEMA

NautilusRustDataType = nautilus_pyo3.OrderBookDelta | nautilus_pyo3.OrderBookDepth10 | nautilus_pyo3.QuoteTick | nautilus_pyo3.TradeTick | nautilus_pyo3.Bar | nautilus_pyo3.MarkPriceUpdate | nautilus_pyo3.IndexPriceUpdate | nautilus_pyo3.OptionGreeks | nautilus_pyo3.InstrumentClose

def get_schema(data_cls: type) -> pa.Schema: ...
def list_schemas() -> dict[type, pa.Schema]: ...
def register_arrow(data_cls: type, schema: pa.Schema | None, encoder: Callable | None = None, decoder: Callable | None = None, batch_encoder: Callable | None = None) -> None: ...

class ArrowSerializer:
    @staticmethod
    def rust_defined_to_record_batch(data: list[Data], data_cls: type) -> pa.Table | pa.RecordBatch: ...
    @staticmethod
    def serialize(data: Data | Event, data_cls: type[Data | Event] | None = None) -> pa.RecordBatch: ...
    @staticmethod
    def serialize_batch(data: list[Data | Event] | list[NautilusRustDataType], data_cls: type[Data | Event | NautilusRustDataType]) -> pa.Table: ...
    @staticmethod
    def deserialize(data_cls: type, batch: pa.RecordBatch | pa.Table) -> list[Data | Event]: ...

def make_dict_serializer(schema: pa.Schema) -> Callable[[list[Data | Event]], pa.RecordBatch]: ...
def make_dict_deserializer(data_cls): ...
def dicts_to_record_batch(data: list[dict], schema: pa.Schema) -> pa.RecordBatch: ...

RUST_SERIALIZERS: Incomplete
RUST_STR_SERIALIZERS: Incomplete

def register_rust_custom_serializer(class_name: str, encoder_fn: Callable, converter_fn: Callable, data_cls: type | None = None) -> None: ...
