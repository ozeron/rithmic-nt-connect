from _typeshed import Incomplete
from decimal import Decimal
from enum import Enum
from nautilus_trader.adapters.binance.common.enums import BinanceEnumParser as BinanceEnumParser, BinanceOrderType as BinanceOrderType
from nautilus_trader.model.enums import OrderType as OrderType, PositionSide as PositionSide, TimeInForce as TimeInForce, TriggerType as TriggerType
from nautilus_trader.model.orders import Order as Order

class BinanceFuturesContractType(Enum):
    PERPETUAL = 'PERPETUAL'
    CURRENT_MONTH = 'CURRENT_MONTH'
    NEXT_MONTH = 'NEXT_MONTH'
    CURRENT_QUARTER = 'CURRENT_QUARTER'
    NEXT_QUARTER = 'NEXT_QUARTER'
    PERPETUAL_DELIVERING = 'PERPETUAL_DELIVERING'
    CURRENT_QUARTER_DELIVERING = 'CURRENT_QUARTER DELIVERING'
    TRADIFI_PERPETUAL = 'TRADIFI_PERPETUAL'

class BinanceFuturesContractStatus(Enum):
    PENDING_TRADING = 'PENDING_TRADING'
    TRADING = 'TRADING'
    TRADING_HALT = 'TRADING_HALT'
    PRE_DELIVERING = 'PRE_DELIVERING'
    DELIVERING = 'DELIVERING'
    DELIVERED = 'DELIVERED'
    PRE_SETTLE = 'PRE_SETTLE'
    SETTLING = 'SETTLING'
    CLOSE = 'CLOSE'

class BinanceFuturesWorkingType(Enum):
    MARK_PRICE = 'MARK_PRICE'
    CONTRACT_PRICE = 'CONTRACT_PRICE'

class BinanceFuturesMarginType(Enum):
    ISOLATED = 'ISOLATED'
    CROSS = 'CROSSED'

class BinanceFuturesPositionUpdateReason(Enum):
    DEPOSIT = 'DEPOSIT'
    WITHDRAW = 'WITHDRAW'
    ORDER = 'ORDER'
    FUNDING_FEE = 'FUNDING_FEE'
    WITHDRAW_REJECT = 'WITHDRAW_REJECT'
    ADJUSTMENT = 'ADJUSTMENT'
    INSURANCE_CLEAR = 'INSURANCE_CLEAR'
    ADMIN_DEPOSIT = 'ADMIN_DEPOSIT'
    ADMIN_WITHDRAW = 'ADMIN_WITHDRAW'
    MARGIN_TRANSFER = 'MARGIN_TRANSFER'
    MARGIN_TYPE_CHANGE = 'MARGIN_TYPE_CHANGE'
    ASSET_TRANSFER = 'ASSET_TRANSFER'
    OPTIONS_PREMIUM_FEE = 'OPTIONS_PREMIUM_FEE'
    OPTIONS_SETTLE_PROFIT = 'OPTIONS_SETTLE_PROFIT'
    AUTO_EXCHANGE = 'AUTO_EXCHANGE'
    COIN_SWAP_DEPOSIT = 'COIN_SWAP_DEPOSIT'
    COIN_SWAP_WITHDRAW = 'COIN_SWAP_WITHDRAW'
    ADL = 'ADL'

class BinanceFuturesEventType(Enum):
    LISTEN_KEY_EXPIRED = 'listenKeyExpired'
    MARGIN_CALL = 'MARGIN_CALL'
    ACCOUNT_UPDATE = 'ACCOUNT_UPDATE'
    ORDER_TRADE_UPDATE = 'ORDER_TRADE_UPDATE'
    ACCOUNT_CONFIG_UPDATE = 'ACCOUNT_CONFIG_UPDATE'
    TRADE_LITE = 'TRADE_LITE'
    STRATEGY_UPDATE = 'STRATEGY_UPDATE'
    GRID_UPDATE = 'GRID_UPDATE'
    CONDITIONAL_ORDER_TRIGGER_REJECT = 'CONDITIONAL_ORDER_TRIGGER_REJECT'
    ALGO_UPDATE = 'ALGO_UPDATE'

class BinanceFuturesEnumParser(BinanceEnumParser):
    futures_ext_to_int_order_type: Incomplete
    futures_int_to_ext_order_type: Incomplete
    futures_valid_time_in_force: Incomplete
    futures_valid_order_types: Incomplete
    def __init__(self) -> None: ...
    def parse_binance_order_type(self, order_type: BinanceOrderType) -> OrderType: ...
    def parse_internal_order_type(self, order: Order) -> BinanceOrderType: ...
    def parse_binance_trigger_type(self, trigger_type: str) -> TriggerType: ...
    def parse_futures_position_side(self, net_size: Decimal) -> PositionSide: ...
