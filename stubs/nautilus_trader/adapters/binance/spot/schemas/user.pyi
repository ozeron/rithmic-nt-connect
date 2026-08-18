import msgspec
from nautilus_trader.adapters.binance.common.enums import BinanceEnumParser as BinanceEnumParser, BinanceExecutionType as BinanceExecutionType, BinanceOrderSide as BinanceOrderSide, BinanceOrderStatus as BinanceOrderStatus, BinanceOrderType as BinanceOrderType, BinanceTimeInForce as BinanceTimeInForce
from nautilus_trader.adapters.binance.execution import BinanceCommonExecutionClient as BinanceCommonExecutionClient
from nautilus_trader.adapters.binance.spot.enums import BinanceSpotEventType as BinanceSpotEventType
from nautilus_trader.core.datetime import millis_to_nanos as millis_to_nanos
from nautilus_trader.core.uuid import UUID4 as UUID4
from nautilus_trader.execution.reports import OrderStatusReport as OrderStatusReport
from nautilus_trader.model.enums import LiquiditySide as LiquiditySide, OrderSide as OrderSide, TrailingOffsetType as TrailingOffsetType, TriggerType as TriggerType
from nautilus_trader.model.identifiers import AccountId as AccountId, ClientOrderId as ClientOrderId, InstrumentId as InstrumentId, TradeId as TradeId, VenueOrderId as VenueOrderId
from nautilus_trader.model.objects import AccountBalance as AccountBalance, Currency as Currency, Money as Money, Price as Price, Quantity as Quantity

class BinanceSpotUserMsgData(msgspec.Struct, frozen=True):
    e: BinanceSpotEventType

class BinanceSpotUserMsgWrapper(msgspec.Struct, frozen=True):
    data: BinanceSpotUserMsgData | None = ...
    stream: str | None = ...

class BinanceSpotBalance(msgspec.Struct, frozen=True):
    a: str
    f: str
    l: str
    def parse_to_account_balance(self) -> AccountBalance: ...

class BinanceSpotAccountUpdateMsg(msgspec.Struct, frozen=True):
    e: str
    E: int
    u: int
    B: list[BinanceSpotBalance]
    def parse_to_account_balances(self) -> list[AccountBalance]: ...
    def handle_account_update(self, exec_client: BinanceCommonExecutionClient): ...

class BinanceSpotAccountUpdateWrapper(msgspec.Struct, frozen=True):
    stream: str
    data: BinanceSpotAccountUpdateMsg

class BinanceSpotOrderUpdateData(msgspec.Struct, kw_only=True):
    e: BinanceSpotEventType
    E: int
    s: str
    c: str
    S: BinanceOrderSide
    o: BinanceOrderType
    f: BinanceTimeInForce
    q: str
    p: str
    P: str
    F: str
    g: int
    C: str
    x: BinanceExecutionType
    X: BinanceOrderStatus
    r: str
    i: int
    l: str
    z: str
    L: str
    n: str | None = ...
    N: str | None = ...
    T: int
    t: int
    I: int
    w: bool
    m: bool
    M: bool
    O: int
    Z: str
    Y: str
    Q: str
    W: int | None = ...
    V: str | None = ...
    def parse_to_order_status_report(self, account_id: AccountId, instrument_id: InstrumentId, client_order_id: ClientOrderId, venue_order_id: VenueOrderId, ts_event: int, ts_init: int, enum_parser: BinanceEnumParser) -> OrderStatusReport: ...
    def handle_execution_report(self, exec_client: BinanceCommonExecutionClient): ...

class BinanceSpotOrderUpdateWrapper(msgspec.Struct, frozen=True):
    stream: str
    data: BinanceSpotOrderUpdateData
