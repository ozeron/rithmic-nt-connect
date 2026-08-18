import pandas as pd
from _typeshed import Incomplete
from collections.abc import Callable as Callable
from nautilus_trader.model.enums import OrderSide as OrderSide, OrderStatus as OrderStatus, OrderType as OrderType, TimeInForce as TimeInForce, TriggerType as TriggerType

MAP_TRIGGER_METHOD: dict[int, int]
MAP_TIME_IN_FORCE: dict[int, str]
MAP_ORDER_ACTION: dict[int, str]
ORDER_SIDE_TO_ORDER_ACTION: dict[str, str]
MAP_ORDER_TYPE: dict[int | tuple[int, int], str]
MAP_ORDER_FIELDS: set[tuple[str, str, Callable]]
MAP_ORDER_STATUS: Incomplete

def timestring_to_timestamp(timestring: str) -> pd.Timestamp: ...
