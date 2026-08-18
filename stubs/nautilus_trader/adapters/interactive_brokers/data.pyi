import asyncio
import pandas as pd
from nautilus_trader.adapters.interactive_brokers.client import InteractiveBrokersClient as InteractiveBrokersClient
from nautilus_trader.adapters.interactive_brokers.common import IBContract as IBContract, IB_VENUE as IB_VENUE
from nautilus_trader.adapters.interactive_brokers.config import InteractiveBrokersDataClientConfig as InteractiveBrokersDataClientConfig
from nautilus_trader.adapters.interactive_brokers.parsing.data import timedelta_to_duration_str as timedelta_to_duration_str
from nautilus_trader.adapters.interactive_brokers.providers import InteractiveBrokersInstrumentProvider as InteractiveBrokersInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.core.datetime import dt_to_unix_nanos as dt_to_unix_nanos, time_object_to_dt as time_object_to_dt, unix_nanos_to_dt as unix_nanos_to_dt
from nautilus_trader.data.messages import RequestBars as RequestBars, RequestData as RequestData, RequestInstrument as RequestInstrument, RequestInstruments as RequestInstruments, RequestQuoteTicks as RequestQuoteTicks, RequestTradeTicks as RequestTradeTicks, SubscribeBars as SubscribeBars, SubscribeData as SubscribeData, SubscribeIndexPrices as SubscribeIndexPrices, SubscribeInstrument as SubscribeInstrument, SubscribeInstrumentClose as SubscribeInstrumentClose, SubscribeInstrumentStatus as SubscribeInstrumentStatus, SubscribeInstruments as SubscribeInstruments, SubscribeOrderBook as SubscribeOrderBook, SubscribeQuoteTicks as SubscribeQuoteTicks, SubscribeTradeTicks as SubscribeTradeTicks, UnsubscribeBars as UnsubscribeBars, UnsubscribeData as UnsubscribeData, UnsubscribeIndexPrices as UnsubscribeIndexPrices, UnsubscribeInstrument as UnsubscribeInstrument, UnsubscribeInstrumentClose as UnsubscribeInstrumentClose, UnsubscribeInstrumentStatus as UnsubscribeInstrumentStatus, UnsubscribeInstruments as UnsubscribeInstruments, UnsubscribeOrderBook as UnsubscribeOrderBook, UnsubscribeQuoteTicks as UnsubscribeQuoteTicks, UnsubscribeTradeTicks as UnsubscribeTradeTicks
from nautilus_trader.live.data_client import LiveMarketDataClient as LiveMarketDataClient
from nautilus_trader.model.data import Bar as Bar, BarType as BarType, QuoteTick as QuoteTick, TradeTick as TradeTick
from nautilus_trader.model.enums import BookType as BookType
from nautilus_trader.model.identifiers import ClientId as ClientId, InstrumentId as InstrumentId
from nautilus_trader.model.instruments import Instrument as Instrument
from nautilus_trader.model.instruments.currency_pair import CurrencyPair as CurrencyPair

class InteractiveBrokersDataClient(LiveMarketDataClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, client: InteractiveBrokersClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: InteractiveBrokersInstrumentProvider, ibg_client_id: int, config: InteractiveBrokersDataClientConfig, name: str | None = None, connection_timeout: int = 300) -> None: ...
    @property
    def instrument_provider(self) -> InteractiveBrokersInstrumentProvider: ...
    async def get_historical_ticks_paged(self, instrument_id: InstrumentId, contract: IBContract, tick_type: str, start_date_time: pd.Timestamp, end_date_time: pd.Timestamp, use_rth: bool = True, timeout: int = 60, limit: int = 0) -> list[TradeTick | QuoteTick]: ...
    async def get_historical_bars_chunked(self, bar_type: BarType, contract: IBContract, start_date_time: pd.Timestamp | None = None, end_date_time: pd.Timestamp | None = None, duration: str | None = None, use_rth: bool = True, timeout: int = 60) -> list[Bar]: ...
