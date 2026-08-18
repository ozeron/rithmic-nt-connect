import pyarrow as pa
from _typeshed import Incomplete
from nautilus_trader.common.messages import ComponentStateChanged as ComponentStateChanged, ShutdownSystem as ShutdownSystem, TradingStateChanged as TradingStateChanged
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.model.data import Bar as Bar, FundingRateUpdate as FundingRateUpdate, IndexPriceUpdate as IndexPriceUpdate, InstrumentClose as InstrumentClose, InstrumentStatus as InstrumentStatus, MarkPriceUpdate as MarkPriceUpdate, OptionGreeks as OptionGreeks, OrderBookDelta as OrderBookDelta, OrderBookDepth10 as OrderBookDepth10, QuoteTick as QuoteTick, TradeTick as TradeTick
from nautilus_trader.model.events import OrderAccepted as OrderAccepted, OrderCancelRejected as OrderCancelRejected, OrderCanceled as OrderCanceled, OrderDenied as OrderDenied, OrderEmulated as OrderEmulated, OrderExpired as OrderExpired, OrderFilled as OrderFilled, OrderInitialized as OrderInitialized, OrderModifyRejected as OrderModifyRejected, OrderPendingCancel as OrderPendingCancel, OrderPendingUpdate as OrderPendingUpdate, OrderRejected as OrderRejected, OrderReleased as OrderReleased, OrderSubmitted as OrderSubmitted, OrderTriggered as OrderTriggered, OrderUpdated as OrderUpdated

def infer_dtype(dtype_str: str) -> pa.DataType: ...

NAUTILUS_ARROW_SCHEMA: Incomplete
