import asyncio
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType as BinanceAccountType, BinanceEnvironment as BinanceEnvironment
from nautilus_trader.adapters.binance.config import BinanceExecClientConfig as BinanceExecClientConfig
from nautilus_trader.adapters.binance.execution import BinanceCommonExecutionClient as BinanceCommonExecutionClient
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient as BinanceHttpClient
from nautilus_trader.adapters.binance.spot.enums import BinanceSpotEnumParser as BinanceSpotEnumParser, BinanceSpotEventType as BinanceSpotEventType
from nautilus_trader.adapters.binance.spot.http.account import BinanceSpotAccountHttpAPI as BinanceSpotAccountHttpAPI
from nautilus_trader.adapters.binance.spot.http.market import BinanceSpotMarketHttpAPI as BinanceSpotMarketHttpAPI
from nautilus_trader.adapters.binance.spot.providers import BinanceSpotInstrumentProvider as BinanceSpotInstrumentProvider
from nautilus_trader.adapters.binance.spot.schemas.account import BinanceSpotAccountInfo as BinanceSpotAccountInfo
from nautilus_trader.adapters.binance.spot.schemas.user import BinanceSpotAccountUpdateMsg as BinanceSpotAccountUpdateMsg, BinanceSpotOrderUpdateData as BinanceSpotOrderUpdateData, BinanceSpotUserMsgData as BinanceSpotUserMsgData
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.core.datetime import millis_to_nanos as millis_to_nanos
from nautilus_trader.execution.messages import BatchCancelOrders as BatchCancelOrders
from nautilus_trader.execution.reports import PositionStatusReport as PositionStatusReport
from nautilus_trader.model.enums import OrderType as OrderType, order_type_to_str as order_type_to_str, time_in_force_to_str as time_in_force_to_str
from nautilus_trader.model.orders import Order as Order

class BinanceSpotExecutionClient(BinanceCommonExecutionClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: BinanceHttpClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: BinanceSpotInstrumentProvider, base_url_ws: str, config: BinanceExecClientConfig, account_type: BinanceAccountType = ..., name: str | None = None, *, environment: BinanceEnvironment, api_key: str, api_secret: str) -> None: ...
