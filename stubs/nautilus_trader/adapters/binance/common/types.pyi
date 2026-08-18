from _typeshed import Incomplete
from decimal import Decimal
from nautilus_trader.core.data import Data as Data
from nautilus_trader.model.data import Bar as Bar, BarType as BarType
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId
from nautilus_trader.model.objects import Price as Price, Quantity as Quantity
from typing import Any

class BinanceBar(Bar):
    quote_volume: Incomplete
    count: Incomplete
    taker_buy_base_volume: Incomplete
    taker_buy_quote_volume: Incomplete
    taker_sell_base_volume: Incomplete
    taker_sell_quote_volume: Incomplete
    def __init__(self, bar_type: BarType, open: Price, high: Price, low: Price, close: Price, volume: Quantity, quote_volume: Decimal, count: int, taker_buy_base_volume: Decimal, taker_buy_quote_volume: Decimal, ts_event: int, ts_init: int) -> None: ...
    @staticmethod
    def from_dict(values: dict[str, Any]) -> BinanceBar: ...
    @staticmethod
    def to_dict(obj: BinanceBar) -> dict[str, Any]: ...

class BinanceTicker(Data):
    instrument_id: Incomplete
    price_change: Incomplete
    price_change_percent: Incomplete
    weighted_avg_price: Incomplete
    prev_close_price: Incomplete
    last_price: Incomplete
    last_qty: Incomplete
    bid_price: Incomplete
    bid_qty: Incomplete
    ask_price: Incomplete
    ask_qty: Incomplete
    open_price: Incomplete
    high_price: Incomplete
    low_price: Incomplete
    volume: Incomplete
    quote_volume: Incomplete
    open_time_ms: Incomplete
    close_time_ms: Incomplete
    first_id: Incomplete
    last_id: Incomplete
    count: Incomplete
    def __init__(self, instrument_id: InstrumentId, price_change: Decimal, price_change_percent: Decimal, weighted_avg_price: Decimal, last_price: Decimal, last_qty: Decimal, open_price: Decimal, high_price: Decimal, low_price: Decimal, volume: Decimal, quote_volume: Decimal, open_time_ms: int, close_time_ms: int, first_id: int, last_id: int, count: int, ts_event: int, ts_init: int, prev_close_price: Decimal | None = None, bid_price: Decimal | None = None, bid_qty: Decimal | None = None, ask_price: Decimal | None = None, ask_qty: Decimal | None = None) -> None: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
    @property
    def ts_event(self) -> int: ...
    @property
    def ts_init(self) -> int: ...
    @staticmethod
    def from_dict(values: dict[str, Any]) -> BinanceTicker: ...
    @staticmethod
    def to_dict(obj: BinanceTicker) -> dict[str, Any]: ...
