import asyncio
from nautilus_trader.adapters.tardis.common import convert_nautilus_bar_type_to_tardis_data_type as convert_nautilus_bar_type_to_tardis_data_type, convert_nautilus_data_type_to_tardis_data_type as convert_nautilus_data_type_to_tardis_data_type, create_instrument_info as create_instrument_info, create_replay_normalized_request_options as create_replay_normalized_request_options, create_stream_normalized_request_options as create_stream_normalized_request_options, get_ws_client_key as get_ws_client_key
from nautilus_trader.adapters.tardis.config import TardisDataClientConfig as TardisDataClientConfig
from nautilus_trader.adapters.tardis.constants import TARDIS as TARDIS
from nautilus_trader.adapters.tardis.providers import TardisInstrumentProvider as TardisInstrumentProvider
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.data.messages import RequestBars as RequestBars, RequestInstrument as RequestInstrument, RequestInstruments as RequestInstruments, RequestQuoteTicks as RequestQuoteTicks, RequestTradeTicks as RequestTradeTicks, SubscribeBars as SubscribeBars, SubscribeFundingRates as SubscribeFundingRates, SubscribeOrderBook as SubscribeOrderBook, SubscribeQuoteTicks as SubscribeQuoteTicks, SubscribeTradeTicks as SubscribeTradeTicks, UnsubscribeBars as UnsubscribeBars, UnsubscribeFundingRates as UnsubscribeFundingRates, UnsubscribeOrderBook as UnsubscribeOrderBook, UnsubscribeQuoteTicks as UnsubscribeQuoteTicks, UnsubscribeTradeTicks as UnsubscribeTradeTicks
from nautilus_trader.live.cancellation import DEFAULT_FUTURE_CANCELLATION_TIMEOUT as DEFAULT_FUTURE_CANCELLATION_TIMEOUT, cancel_tasks_with_timeout as cancel_tasks_with_timeout
from nautilus_trader.live.data_client import LiveMarketDataClient as LiveMarketDataClient
from nautilus_trader.model.data import Bar as Bar, FundingRateUpdate as FundingRateUpdate, OrderBookDelta as OrderBookDelta, QuoteTick as QuoteTick, TradeTick as TradeTick, capsule_to_data as capsule_to_data
from nautilus_trader.model.enums import BookType as BookType, PriceType as PriceType
from nautilus_trader.model.identifiers import ClientId as ClientId, InstrumentId as InstrumentId
from nautilus_trader.model.instruments import Instrument as Instrument

class TardisDataClient(LiveMarketDataClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: TardisInstrumentProvider, config: TardisDataClientConfig, name: str | None) -> None: ...
