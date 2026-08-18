from enum import Enum
from nautilus_trader.core.rust.model import AccountType as AccountType, AggregationSource as AggregationSource, AggressorSide as AggressorSide, AssetClass as AssetClass, BookAction as BookAction, BookType as BookType, ContingencyType as ContingencyType, CurrencyType as CurrencyType, InstrumentClass as InstrumentClass, InstrumentCloseType as InstrumentCloseType, LiquiditySide as LiquiditySide, MarketStatus as MarketStatus, MarketStatusAction as MarketStatusAction, OmsType as OmsType, OptionKind as OptionKind, OrderSide as OrderSide, OrderStatus as OrderStatus, OrderType as OrderType, OtoTriggerMode as OtoTriggerMode, PositionAdjustmentType as PositionAdjustmentType, PositionSide as PositionSide, PriceType as PriceType, RecordFlag as RecordFlag, TimeInForce as TimeInForce, TradingState as TradingState, TrailingOffsetType as TrailingOffsetType, TriggerType as TriggerType
from nautilus_trader.model.data import BarAggregation as BarAggregation
from nautilus_trader.model.functions import account_type_from_str as account_type_from_str, account_type_to_str as account_type_to_str, aggregation_source_from_str as aggregation_source_from_str, aggregation_source_to_str as aggregation_source_to_str, aggressor_side_from_str as aggressor_side_from_str, aggressor_side_to_str as aggressor_side_to_str, asset_class_from_str as asset_class_from_str, asset_class_to_str as asset_class_to_str, bar_aggregation_from_str as bar_aggregation_from_str, bar_aggregation_to_str as bar_aggregation_to_str, book_action_from_str as book_action_from_str, book_action_to_str as book_action_to_str, book_type_from_str as book_type_from_str, book_type_to_str as book_type_to_str, contingency_type_from_str as contingency_type_from_str, contingency_type_to_str as contingency_type_to_str, currency_type_from_str as currency_type_from_str, currency_type_to_str as currency_type_to_str, instrument_class_from_str as instrument_class_from_str, instrument_class_to_str as instrument_class_to_str, instrument_close_type_from_str as instrument_close_type_from_str, instrument_close_type_to_str as instrument_close_type_to_str, liquidity_side_from_str as liquidity_side_from_str, liquidity_side_to_str as liquidity_side_to_str, market_status_action_from_str as market_status_action_from_str, market_status_action_to_str as market_status_action_to_str, market_status_from_str as market_status_from_str, market_status_to_str as market_status_to_str, oms_type_from_str as oms_type_from_str, oms_type_to_str as oms_type_to_str, option_kind_from_str as option_kind_from_str, option_kind_to_str as option_kind_to_str, order_side_from_str as order_side_from_str, order_side_to_str as order_side_to_str, order_status_from_str as order_status_from_str, order_status_to_str as order_status_to_str, order_type_from_str as order_type_from_str, order_type_to_str as order_type_to_str, oto_trigger_mode_from_str as oto_trigger_mode_from_str, oto_trigger_mode_to_str as oto_trigger_mode_to_str, position_adjustment_type_from_str as position_adjustment_type_from_str, position_adjustment_type_to_str as position_adjustment_type_to_str, position_side_from_str as position_side_from_str, position_side_to_str as position_side_to_str, price_type_from_str as price_type_from_str, price_type_to_str as price_type_to_str, record_flag_from_str as record_flag_from_str, record_flag_to_str as record_flag_to_str, time_in_force_from_str as time_in_force_from_str, time_in_force_to_str as time_in_force_to_str, trading_state_from_str as trading_state_from_str, trading_state_to_str as trading_state_to_str, trailing_offset_type_from_str as trailing_offset_type_from_str, trailing_offset_type_to_str as trailing_offset_type_to_str, trigger_type_from_str as trigger_type_from_str, trigger_type_to_str as trigger_type_to_str

__all__ = ['AccountType', 'AggregationSource', 'AggressorSide', 'AssetClass', 'BarAggregation', 'BookAction', 'BookType', 'ContingencyType', 'ContinuousFutureAdjustmentType', 'CurrencyType', 'InstrumentClass', 'InstrumentCloseType', 'LiquiditySide', 'MarketStatus', 'MarketStatusAction', 'OmsType', 'OptionKind', 'OrderSide', 'OrderStatus', 'OrderType', 'OtoTriggerMode', 'PositionAdjustmentType', 'PositionSide', 'PriceType', 'RecordFlag', 'TimeInForce', 'TradingState', 'TrailingOffsetType', 'TriggerType', 'account_type_from_str', 'account_type_to_str', 'aggregation_source_from_str', 'aggregation_source_to_str', 'aggressor_side_from_str', 'aggressor_side_to_str', 'asset_class_from_str', 'asset_class_to_str', 'bar_aggregation_from_str', 'bar_aggregation_to_str', 'book_action_from_str', 'book_action_to_str', 'book_type_from_str', 'book_type_to_str', 'contingency_type_from_str', 'contingency_type_to_str', 'currency_type_from_str', 'currency_type_to_str', 'instrument_class_from_str', 'instrument_class_to_str', 'instrument_close_type_from_str', 'instrument_close_type_to_str', 'liquidity_side_from_str', 'liquidity_side_to_str', 'market_status_action_from_str', 'market_status_action_to_str', 'market_status_from_str', 'market_status_to_str', 'oms_type_from_str', 'oms_type_to_str', 'option_kind_from_str', 'option_kind_to_str', 'order_side_from_str', 'order_side_to_str', 'order_status_from_str', 'order_status_to_str', 'order_type_from_str', 'order_type_to_str', 'oto_trigger_mode_from_str', 'oto_trigger_mode_to_str', 'position_adjustment_type_from_str', 'position_adjustment_type_to_str', 'position_side_from_str', 'position_side_to_str', 'price_type_from_str', 'price_type_to_str', 'record_flag_from_str', 'record_flag_to_str', 'time_in_force_from_str', 'time_in_force_to_str', 'trading_state_from_str', 'trading_state_to_str', 'trailing_offset_type_from_str', 'trailing_offset_type_to_str', 'trigger_type_from_str', 'trigger_type_to_str']

class ContinuousFutureAdjustmentType(Enum):
    BACKWARD_SPREAD = 'backward_spread'
    FORWARD_SPREAD = 'forward_spread'
    BACKWARD_RATIO = 'backward_ratio'
    FORWARD_RATIO = 'forward_ratio'
    @property
    def is_ratio(self) -> bool: ...
    @property
    def is_backward(self) -> bool: ...

class AccountType(Enum):
    CASH = 1
    MARGIN = 2
    BETTING = 3

class AggregationSource(Enum):
    EXTERNAL = 1
    INTERNAL = 2

class AggressorSide(Enum):
    NO_AGGRESSOR = 0
    BUYER = 1
    SELLER = 2

class AssetClass(Enum):
    FX = 1
    EQUITY = 2
    COMMODITY = 3
    DEBT = 4
    INDEX = 5
    CRYPTOCURRENCY = 6
    ALTERNATIVE = 7

class BookAction(Enum):
    ADD = 1
    UPDATE = 2
    DELETE = 3
    CLEAR = 4

class BookType(Enum):
    L1_MBP = 1
    L2_MBP = 2
    L3_MBO = 3

class ContingencyType(Enum):
    NO_CONTINGENCY = 0
    OCO = 1
    OTO = 2
    OUO = 3

class CurrencyType(Enum):
    CRYPTO = 1
    FIAT = 2
    COMMODITY_BACKED = 3

class InstrumentClass(Enum):
    SPOT = 1
    SWAP = 2
    FUTURE = 3
    FUTURES_SPREAD = 4
    FORWARD = 5
    CFD = 6
    BOND = 7
    OPTION = 8
    OPTION_SPREAD = 9
    WARRANT = 10
    SPORTS_BETTING = 11
    BINARY_OPTION = 12

class InstrumentCloseType(Enum):
    END_OF_SESSION = 1
    CONTRACT_EXPIRED = 2

class LiquiditySide(Enum):
    NO_LIQUIDITY_SIDE = 0
    MAKER = 1
    TAKER = 2

class MarketStatus(Enum):
    OPEN = 1
    CLOSED = 2
    PAUSED = 3
    SUSPENDED = 5
    NOT_AVAILABLE = 6

class MarketStatusAction(Enum):
    NONE = 0
    PRE_OPEN = 1
    PRE_CROSS = 2
    QUOTING = 3
    CROSS = 4
    ROTATION = 5
    NEW_PRICE_INDICATION = 6
    TRADING = 7
    HALT = 8
    PAUSE = 9
    SUSPEND = 10
    PRE_CLOSE = 11
    CLOSE = 12
    POST_CLOSE = 13
    SHORT_SELL_RESTRICTION_CHANGE = 14
    NOT_AVAILABLE_FOR_TRADING = 15

class OmsType(Enum):
    UNSPECIFIED = 0
    NETTING = 1
    HEDGING = 2

class OptionKind(Enum):
    CALL = 1
    PUT = 2

class OrderSide(Enum):
    NO_ORDER_SIDE = 0
    BUY = 1
    SELL = 2

class OrderStatus(Enum):
    INITIALIZED = 1
    DENIED = 2
    EMULATED = 3
    RELEASED = 4
    SUBMITTED = 5
    ACCEPTED = 6
    REJECTED = 7
    CANCELED = 8
    EXPIRED = 9
    TRIGGERED = 10
    PENDING_UPDATE = 11
    PENDING_CANCEL = 12
    PARTIALLY_FILLED = 13
    FILLED = 14

class OrderType(Enum):
    MARKET = 1
    LIMIT = 2
    STOP_MARKET = 3
    STOP_LIMIT = 4
    MARKET_TO_LIMIT = 5
    MARKET_IF_TOUCHED = 6
    LIMIT_IF_TOUCHED = 7
    TRAILING_STOP_MARKET = 8
    TRAILING_STOP_LIMIT = 9

class OtoTriggerMode(Enum):
    PARTIAL = 0
    FULL = 1

class PositionAdjustmentType(Enum):
    COMMISSION = 1
    FUNDING = 2

class PositionSide(Enum):
    NO_POSITION_SIDE = 0
    FLAT = 1
    LONG = 2
    SHORT = 3

class PriceType(Enum):
    BID = 1
    ASK = 2
    MID = 3
    LAST = 4
    MARK = 5

class RecordFlag(Enum):
    F_LAST = 128
    F_TOB = 64
    F_SNAPSHOT = 32
    F_MBP = 16
    RESERVED_2 = 8
    RESERVED_1 = 4

class TimeInForce(Enum):
    GTC = 1
    IOC = 2
    FOK = 3
    GTD = 4
    DAY = 5
    AT_THE_OPEN = 6
    AT_THE_CLOSE = 7

class TradingState(Enum):
    ACTIVE = 1
    HALTED = 2
    REDUCING = 3

class TrailingOffsetType(Enum):
    NO_TRAILING_OFFSET = 0
    PRICE = 1
    BASIS_POINTS = 2
    TICKS = 3
    PRICE_TIER = 4

class TriggerType(Enum):
    NO_TRIGGER = 0
    DEFAULT = 1
    BID_ASK = 2
    LAST_PRICE = 3
    DOUBLE_LAST = 4
    DOUBLE_BID_ASK = 5
    LAST_OR_BID_ASK = 6
    MID_POINT = 7
    MARK_PRICE = 8
    INDEX_PRICE = 9
