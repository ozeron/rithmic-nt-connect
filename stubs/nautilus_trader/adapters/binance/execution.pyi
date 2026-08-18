import asyncio
from collections.abc import Callable as Callable
from nautilus_trader.adapters.binance.common.constants import BINANCE_FUTURES_ALGO_ORDER_TYPES as BINANCE_FUTURES_ALGO_ORDER_TYPES, BINANCE_MAX_CALLBACK_RATE as BINANCE_MAX_CALLBACK_RATE, BINANCE_MIN_CALLBACK_RATE as BINANCE_MIN_CALLBACK_RATE, BINANCE_PRICE_MATCH_ORDER_TYPES as BINANCE_PRICE_MATCH_ORDER_TYPES, BINANCE_PRICE_MATCH_VALUES as BINANCE_PRICE_MATCH_VALUES, BINANCE_RETRY_WARNINGS as BINANCE_RETRY_WARNINGS, BINANCE_SPOT_POST_ONLY_REJECT_MSG as BINANCE_SPOT_POST_ONLY_REJECT_MSG
from nautilus_trader.adapters.binance.common.credentials import is_ed25519_private_key as is_ed25519_private_key
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType as BinanceAccountType, BinanceEnumParser as BinanceEnumParser, BinanceEnvironment as BinanceEnvironment, BinanceErrorCode as BinanceErrorCode, BinanceFuturesPositionSide as BinanceFuturesPositionSide, BinanceKeyType as BinanceKeyType, BinanceTimeInForce as BinanceTimeInForce
from nautilus_trader.adapters.binance.common.schemas.account import BinanceOrder as BinanceOrder, BinanceUserTrade as BinanceUserTrade
from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol as BinanceSymbol
from nautilus_trader.adapters.binance.common.urls import get_usdm_ws_route_base_url as get_usdm_ws_route_base_url, get_ws_api_base_url as get_ws_api_base_url, get_ws_private_base_url as get_ws_private_base_url
from nautilus_trader.adapters.binance.config import BinanceExecClientConfig as BinanceExecClientConfig
from nautilus_trader.adapters.binance.http.account import BinanceAccountHttpAPI as BinanceAccountHttpAPI
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient as BinanceHttpClient
from nautilus_trader.adapters.binance.http.error import BinanceError as BinanceError, get_binance_error_code as get_binance_error_code, should_retry as should_retry
from nautilus_trader.adapters.binance.http.market import BinanceMarketHttpAPI as BinanceMarketHttpAPI
from nautilus_trader.adapters.binance.websocket.user import BinanceUserDataWebSocketClient as BinanceUserDataWebSocketClient
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor, LogLevel as LogLevel
from nautilus_trader.common.providers import InstrumentProvider as InstrumentProvider
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.core.datetime import nanos_to_millis as nanos_to_millis, secs_to_millis as secs_to_millis
from nautilus_trader.core.uuid import UUID4 as UUID4
from nautilus_trader.execution.messages import CancelAllOrders as CancelAllOrders, CancelOrder as CancelOrder, GenerateFillReports as GenerateFillReports, GenerateOrderStatusReport as GenerateOrderStatusReport, GenerateOrderStatusReports as GenerateOrderStatusReports, GeneratePositionStatusReports as GeneratePositionStatusReports, ModifyOrder as ModifyOrder, QueryAccount as QueryAccount, SubmitOrder as SubmitOrder, SubmitOrderList as SubmitOrderList
from nautilus_trader.execution.reports import FillReport as FillReport, OrderStatusReport as OrderStatusReport, PositionStatusReport as PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient as LiveExecutionClient
from nautilus_trader.live.retry import RetryManagerPool as RetryManagerPool
from nautilus_trader.model.enums import AccountType as AccountType, OmsType as OmsType, OrderSide as OrderSide, OrderStatus as OrderStatus, OrderType as OrderType, PositionSide as PositionSide, TrailingOffsetType as TrailingOffsetType, TriggerType as TriggerType, order_side_to_str as order_side_to_str, trailing_offset_type_to_str as trailing_offset_type_to_str, trigger_type_to_str as trigger_type_to_str
from nautilus_trader.model.identifiers import AccountId as AccountId, ClientId as ClientId, ClientOrderId as ClientOrderId, InstrumentId as InstrumentId, PositionId as PositionId, Symbol as Symbol, VenueOrderId as VenueOrderId
from nautilus_trader.model.objects import Price as Price, Quantity as Quantity
from nautilus_trader.model.orders import LimitOrder as LimitOrder, MarketOrder as MarketOrder, Order as Order, StopLimitOrder as StopLimitOrder, StopMarketOrder as StopMarketOrder, TrailingStopMarketOrder as TrailingStopMarketOrder
from nautilus_trader.model.position import Position as Position

class BinanceCommonExecutionClient(LiveExecutionClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: BinanceHttpClient, account: BinanceAccountHttpAPI, market: BinanceMarketHttpAPI, enum_parser: BinanceEnumParser, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: InstrumentProvider, account_type: BinanceAccountType, base_url_ws: str, name: str | None, config: BinanceExecClientConfig, environment: BinanceEnvironment, api_key: str, api_secret: str) -> None: ...
    @property
    def use_position_ids(self) -> bool: ...
    @property
    def treat_expired_as_canceled(self) -> bool: ...
    async def generate_order_status_report(self, command: GenerateOrderStatusReport) -> OrderStatusReport | None: ...
    async def generate_order_status_reports(self, command: GenerateOrderStatusReports) -> list[OrderStatusReport]: ...
    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]: ...
    async def generate_position_status_reports(self, command: GeneratePositionStatusReports) -> list[PositionStatusReport]: ...
