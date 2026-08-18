import asyncio
from nautilus_trader.accounting.accounts.margin import MarginAccount as MarginAccount
from nautilus_trader.adapters.binance.common.constants import BINANCE_FUTURES_ALGO_ORDER_TYPES as BINANCE_FUTURES_ALGO_ORDER_TYPES
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType as BinanceAccountType, BinanceEnvironment as BinanceEnvironment, BinanceErrorCode as BinanceErrorCode, BinanceExecutionType as BinanceExecutionType
from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol as BinanceSymbol
from nautilus_trader.adapters.binance.config import BinanceExecClientConfig as BinanceExecClientConfig
from nautilus_trader.adapters.binance.execution import BinanceCommonExecutionClient as BinanceCommonExecutionClient
from nautilus_trader.adapters.binance.futures.enums import BinanceFuturesEnumParser as BinanceFuturesEnumParser, BinanceFuturesEventType as BinanceFuturesEventType
from nautilus_trader.adapters.binance.futures.http.account import BinanceFuturesAccountHttpAPI as BinanceFuturesAccountHttpAPI
from nautilus_trader.adapters.binance.futures.http.market import BinanceFuturesMarketHttpAPI as BinanceFuturesMarketHttpAPI
from nautilus_trader.adapters.binance.futures.providers import BinanceFuturesInstrumentProvider as BinanceFuturesInstrumentProvider
from nautilus_trader.adapters.binance.futures.schemas.account import BinanceFuturesAccountInfo as BinanceFuturesAccountInfo, BinanceFuturesAlgoOrder as BinanceFuturesAlgoOrder, BinanceFuturesDualSidePosition as BinanceFuturesDualSidePosition, BinanceFuturesLeverage as BinanceFuturesLeverage, BinanceFuturesPositionRisk as BinanceFuturesPositionRisk
from nautilus_trader.adapters.binance.futures.schemas.user import BinanceFuturesAccountUpdateMsg as BinanceFuturesAccountUpdateMsg, BinanceFuturesAlgoUpdateMsg as BinanceFuturesAlgoUpdateMsg, BinanceFuturesOrderUpdateMsg as BinanceFuturesOrderUpdateMsg, BinanceFuturesTradeLiteMsg as BinanceFuturesTradeLiteMsg, BinanceFuturesUserMsgData as BinanceFuturesUserMsgData
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient as BinanceHttpClient
from nautilus_trader.adapters.binance.http.error import BinanceError as BinanceError
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.core.datetime import millis_to_nanos as millis_to_nanos, secs_to_millis as secs_to_millis
from nautilus_trader.core.uuid import UUID4 as UUID4
from nautilus_trader.execution.messages import BatchCancelOrders as BatchCancelOrders, CancelAllOrders as CancelAllOrders, CancelOrder as CancelOrder, GenerateOrderStatusReport as GenerateOrderStatusReport, GenerateOrderStatusReports as GenerateOrderStatusReports
from nautilus_trader.execution.reports import OrderStatusReport as OrderStatusReport, PositionStatusReport as PositionStatusReport
from nautilus_trader.model.enums import OrderType as OrderType, order_type_to_str as order_type_to_str, time_in_force_to_str as time_in_force_to_str
from nautilus_trader.model.identifiers import ClientOrderId as ClientOrderId, InstrumentId as InstrumentId
from nautilus_trader.model.orders import Order as Order

class BinanceFuturesExecutionClient(BinanceCommonExecutionClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: BinanceHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: BinanceFuturesInstrumentProvider, base_url_ws: str, config: BinanceExecClientConfig, account_type: BinanceAccountType = ..., name: str | None = None, *, environment: BinanceEnvironment, api_key: str, api_secret: str) -> None: ...
    async def generate_order_status_report(self, command: GenerateOrderStatusReport) -> OrderStatusReport | None: ...
    async def generate_order_status_reports(self, command: GenerateOrderStatusReports) -> list[OrderStatusReport]: ...
