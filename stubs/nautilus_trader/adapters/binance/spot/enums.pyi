from _typeshed import Incomplete
from enum import Enum
from nautilus_trader.adapters.binance.common.enums import BinanceEnumParser as BinanceEnumParser, BinanceOrderType as BinanceOrderType
from nautilus_trader.model.enums import OrderType as OrderType, TimeInForce as TimeInForce, order_type_to_str as order_type_to_str
from nautilus_trader.model.orders import Order as Order

class BinanceSpotPermissions(Enum):
    SPOT = 'SPOT'
    MARGIN = 'MARGIN'
    LEVERAGED = 'LEVERAGED'
    TRD_GRP_002 = 'TRD_GRP_002'
    TRD_GRP_003 = 'TRD_GRP_003'
    TRD_GRP_004 = 'TRD_GRP_004'
    TRD_GRP_005 = 'TRD_GRP_005'
    TRD_GRP_006 = 'TRD_GRP_006'
    TRD_GRP_007 = 'TRD_GRP_007'
    TRD_GRP_008 = 'TRD_GRP_008'
    TRD_GRP_009 = 'TRD_GRP_009'
    TRD_GRP_010 = 'TRD_GRP_010'
    TRD_GRP_011 = 'TRD_GRP_011'
    TRD_GRP_012 = 'TRD_GRP_012'
    TRD_GRP_013 = 'TRD_GRP_013'
    TRD_GRP_014 = 'TRD_GRP_014'
    TRD_GRP_015 = 'TRD_GRP_015'
    TRD_GRP_016 = 'TRD_GRP_016'
    TRD_GRP_017 = 'TRD_GRP_017'
    TRD_GRP_018 = 'TRD_GRP_018'
    TRD_GRP_019 = 'TRD_GRP_019'
    TRD_GRP_020 = 'TRD_GRP_020'
    TRD_GRP_021 = 'TRD_GRP_021'
    TRD_GRP_022 = 'TRD_GRP_022'
    TRD_GRP_023 = 'TRD_GRP_023'
    TRD_GRP_024 = 'TRD_GRP_024'
    TRD_GRP_025 = 'TRD_GRP_025'
    TRD_GRP_026 = 'TRD_GRP_026'
    TRD_GRP_027 = 'TRD_GRP_027'
    TRD_GRP_028 = 'TRD_GRP_028'
    TRD_GRP_029 = 'TRD_GRP_029'
    TRD_GRP_030 = 'TRD_GRP_030'
    TRD_GRP_031 = 'TRD_GRP_031'
    TRD_GRP_032 = 'TRD_GRP_032'

class BinanceSpotSymbolStatus(Enum):
    PRE_TRADING = 'PRE_TRADING'
    TRADING = 'TRADING'
    POST_TRADING = 'POST_TRADING'
    END_OF_DAY = 'END_OF_DAY'
    HALT = 'HALT'
    AUCTION_MATCH = 'AUCTION_MATCH'
    BREAK = 'BREAK'

class BinanceSpotEventType(Enum):
    outboundAccountPosition = 'outboundAccountPosition'
    balanceUpdate = 'balanceUpdate'
    executionReport = 'executionReport'
    listStatus = 'listStatus'
    listenKeyExpired = 'listenKeyExpired'

class BinanceSpotEnumParser(BinanceEnumParser):
    spot_ext_to_int_order_type: Incomplete
    spot_valid_time_in_force: Incomplete
    spot_valid_order_types: Incomplete
    def __init__(self) -> None: ...
    def parse_binance_order_type(self, order_type: BinanceOrderType) -> OrderType: ...
    def parse_internal_order_type(self, order: Order) -> BinanceOrderType: ...
