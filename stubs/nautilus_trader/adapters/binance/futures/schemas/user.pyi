import msgspec
from nautilus_trader.adapters.binance.common.enums import BinanceEnumParser as BinanceEnumParser, BinanceExecutionType as BinanceExecutionType, BinanceFuturesPositionSide as BinanceFuturesPositionSide, BinanceOrderSide as BinanceOrderSide, BinanceOrderStatus as BinanceOrderStatus, BinanceOrderType as BinanceOrderType, BinanceTimeInForce as BinanceTimeInForce
from nautilus_trader.adapters.binance.execution import BinanceCommonExecutionClient as BinanceCommonExecutionClient
from nautilus_trader.adapters.binance.futures.enums import BinanceFuturesEventType as BinanceFuturesEventType, BinanceFuturesPositionUpdateReason as BinanceFuturesPositionUpdateReason, BinanceFuturesWorkingType as BinanceFuturesWorkingType
from nautilus_trader.core.datetime import millis_to_nanos as millis_to_nanos, unix_nanos_to_dt as unix_nanos_to_dt
from nautilus_trader.core.uuid import UUID4 as UUID4
from nautilus_trader.execution.reports import FillReport as FillReport, OrderStatusReport as OrderStatusReport
from nautilus_trader.model.enums import LiquiditySide as LiquiditySide, OrderSide as OrderSide, OrderStatus as OrderStatus, OrderType as OrderType, TrailingOffsetType as TrailingOffsetType
from nautilus_trader.model.identifiers import AccountId as AccountId, ClientOrderId as ClientOrderId, InstrumentId as InstrumentId, PositionId as PositionId, StrategyId as StrategyId, TradeId as TradeId, VenueOrderId as VenueOrderId
from nautilus_trader.model.objects import AccountBalance as AccountBalance, Currency as Currency, Money as Money, Price as Price, Quantity as Quantity
from nautilus_trader.model.orders import Order as Order

class BinanceFuturesUserMsgData(msgspec.Struct, frozen=True):
    e: BinanceFuturesEventType

class BinanceFuturesUserMsgWrapper(msgspec.Struct, frozen=True):
    data: BinanceFuturesUserMsgData | None = ...
    stream: str | None = ...

class MarginCallPosition(msgspec.Struct, frozen=True):
    s: str
    ps: BinanceFuturesPositionSide
    pa: str
    mt: str
    iw: str
    mp: str
    up: str
    mm: str

class BinanceFuturesMarginCallMsg(msgspec.Struct, frozen=True):
    e: str
    E: int
    cw: float
    p: list[MarginCallPosition]

class BinanceFuturesBalance(msgspec.Struct, frozen=True):
    a: str
    wb: str
    cw: str
    bc: str
    def parse_to_account_balance(self) -> AccountBalance: ...

class BinanceFuturesPosition(msgspec.Struct, frozen=True):
    s: str
    pa: str
    ep: str
    cr: str
    up: str
    mt: str
    iw: str
    ps: BinanceFuturesPositionSide

class BinanceFuturesAccountUpdateData(msgspec.Struct, frozen=True):
    m: BinanceFuturesPositionUpdateReason
    B: list[BinanceFuturesBalance]
    P: list[BinanceFuturesPosition]
    def parse_to_account_balances(self) -> list[AccountBalance]: ...

class BinanceFuturesAccountUpdateMsg(msgspec.Struct, frozen=True):
    e: str
    E: int
    T: int
    a: BinanceFuturesAccountUpdateData
    def handle_account_update(self, exec_client: BinanceCommonExecutionClient): ...

class BinanceFuturesAccountUpdateWrapper(msgspec.Struct, frozen=True):
    stream: str
    data: BinanceFuturesAccountUpdateMsg

class BinanceFuturesOrderData(msgspec.Struct, kw_only=True, frozen=True):
    s: str
    c: str
    S: BinanceOrderSide
    o: BinanceOrderType
    f: BinanceTimeInForce
    q: str
    p: str
    ap: str
    sp: str | None = ...
    x: BinanceExecutionType
    X: BinanceOrderStatus
    i: int
    l: str
    z: str
    L: str
    N: str | None = ...
    n: str | None = ...
    T: int
    t: int
    b: str
    a: str
    m: bool
    R: bool
    wt: BinanceFuturesWorkingType
    ot: BinanceOrderType
    ps: BinanceFuturesPositionSide
    cp: bool | None = ...
    AP: str | None = ...
    cr: str | None = ...
    pP: bool
    si: int
    ss: int
    rp: str
    gtd: int
    W: int | None = ...
    V: str | None = ...
    def parse_to_order_status_report(self, account_id: AccountId, instrument_id: InstrumentId, client_order_id: ClientOrderId, venue_order_id: VenueOrderId, ts_event: int, ts_init: int, enum_parser: BinanceEnumParser) -> OrderStatusReport: ...
    def handle_order_trade_update(self, exec_client: BinanceCommonExecutionClient) -> None: ...

class BinanceFuturesOrderUpdateMsg(msgspec.Struct, frozen=True):
    e: str
    E: int
    T: int
    o: BinanceFuturesOrderData

class BinanceFuturesOrderUpdateWrapper(msgspec.Struct, frozen=True):
    stream: str
    data: BinanceFuturesOrderUpdateMsg

class BinanceFuturesTradeLiteMsg(msgspec.Struct, frozen=True):
    e: str
    E: int
    T: int
    s: str
    c: str
    S: BinanceOrderSide
    q: str
    p: str
    i: int
    l: str
    L: str
    t: int
    m: bool
    def to_order_data(self) -> BinanceFuturesOrderData: ...

class BinanceFuturesTradeLiteWrapper(msgspec.Struct, frozen=True):
    stream: str
    data: BinanceFuturesTradeLiteMsg

class BinanceFuturesAlgoOrderData(msgspec.Struct, kw_only=True, frozen=True):
    caid: str
    aid: int
    at: str
    o: BinanceOrderType
    s: str
    S: BinanceOrderSide
    ps: BinanceFuturesPositionSide
    f: BinanceTimeInForce
    q: str
    X: BinanceOrderStatus
    tp: str
    p: str
    wt: BinanceFuturesWorkingType
    pm: str
    cp: bool
    pP: bool
    R: bool
    tt: int
    gtd: int
    ai: str | None = ...
    ap: str | None = ...
    aq: str | None = ...
    act: str | None = ...
    cr: str | None = ...
    V: str | None = ...
    @property
    def resolved_venue_order_id(self) -> VenueOrderId: ...
    def handle_algo_update(self, exec_client: BinanceCommonExecutionClient, ts_event: int) -> None: ...

class BinanceFuturesAlgoUpdateMsg(msgspec.Struct, frozen=True):
    e: str
    E: int
    T: int
    o: BinanceFuturesAlgoOrderData

class BinanceFuturesAlgoUpdateWrapper(msgspec.Struct, frozen=True):
    stream: str
    data: BinanceFuturesAlgoUpdateMsg
