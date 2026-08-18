import datetime
from _typeshed import Incomplete
from decimal import Decimal
from nautilus_trader.core.datetime import nanos_to_secs as nanos_to_secs
from nautilus_trader.model.data import BarAggregation as BarAggregation, BarSpecification as BarSpecification, BarType as BarType
from nautilus_trader.model.enums import BookAction as BookAction, OrderSide as OrderSide, PriceType as PriceType
from nautilus_trader.model.identifiers import TradeId as TradeId

MKT_DEPTH_OPERATIONS: Incomplete
IB_SIDE: Incomplete
IB_TICK_TYPE: Incomplete

def what_to_show(bar_type: BarType) -> str: ...
def generate_trade_id(ts_event: int, price: float, size: Decimal) -> TradeId: ...
def bar_spec_to_bar_size(bar_spec: BarSpecification) -> str: ...
def timedelta_to_duration_str(duration: datetime.timedelta) -> str: ...
