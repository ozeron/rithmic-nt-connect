from decimal import Decimal
from nautilus_trader.adapters.binance.common.enums import BinanceErrorCode as BinanceErrorCode
from nautilus_trader.model.enums import OrderType as OrderType
from nautilus_trader.model.identifiers import ClientId as ClientId, Venue as Venue
from typing import Final

BINANCE: Final[str]
BINANCE_VENUE: Final[Venue]
BINANCE_CLIENT_ID: Final[ClientId]
BINANCE_MIN_CALLBACK_RATE: Final[Decimal]
BINANCE_MAX_CALLBACK_RATE: Final[Decimal]
BINANCE_FUTURES_ORDER_COUNT_10S_KEY: Final[str]
BINANCE_FUTURES_ORDER_COUNT_1M_KEY: Final[str]
BINANCE_FUTURES_ORDER_COUNT_KEYS: Final[tuple[str, str]]
BINANCE_SPOT_POST_ONLY_REJECT_MSG: Final[str]
BINANCE_RETRY_ERRORS: set[BinanceErrorCode]
BINANCE_RETRY_WARNINGS: set[BinanceErrorCode]
BINANCE_PRICE_MATCH_VALUES: Final[frozenset[str]]
BINANCE_PRICE_MATCH_ORDER_TYPES: Final[frozenset[OrderType]]
BINANCE_FUTURES_ALGO_ORDER_TYPES: Final[frozenset[OrderType]]
