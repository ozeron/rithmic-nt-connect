import asyncio
from nautilus_trader.adapters.databento.common import databento_schema_from_nautilus_bar_type as databento_schema_from_nautilus_bar_type, instrument_id_to_pyo3 as instrument_id_to_pyo3
from nautilus_trader.adapters.databento.config import DatabentoDataClientConfig as DatabentoDataClientConfig
from nautilus_trader.adapters.databento.constants import ALL_SYMBOLS as ALL_SYMBOLS, DATABENTO as DATABENTO, PUBLISHERS_FILEPATH as PUBLISHERS_FILEPATH
from nautilus_trader.adapters.databento.enums import DatabentoSchema as DatabentoSchema
from nautilus_trader.adapters.databento.loaders import DatabentoDataLoader as DatabentoDataLoader
from nautilus_trader.adapters.databento.providers import DatabentoInstrumentProvider as DatabentoInstrumentProvider
from nautilus_trader.adapters.databento.types import DatabentoImbalance as DatabentoImbalance, DatabentoStatistics as DatabentoStatistics, DatabentoSubscriptionAck as DatabentoSubscriptionAck, Dataset as Dataset
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.enums import LogColor as LogColor
from nautilus_trader.core import nautilus_pyo3 as nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition as PyCondition
from nautilus_trader.core.uuid import UUID4 as UUID4
from nautilus_trader.data.messages import RequestBars as RequestBars, RequestData as RequestData, RequestInstrument as RequestInstrument, RequestInstruments as RequestInstruments, RequestOrderBookDeltas as RequestOrderBookDeltas, RequestOrderBookDepth as RequestOrderBookDepth, RequestQuoteTicks as RequestQuoteTicks, RequestTradeTicks as RequestTradeTicks, SubscribeBars as SubscribeBars, SubscribeData as SubscribeData, SubscribeInstrument as SubscribeInstrument, SubscribeInstrumentStatus as SubscribeInstrumentStatus, SubscribeInstruments as SubscribeInstruments, SubscribeOrderBook as SubscribeOrderBook, SubscribeQuoteTicks as SubscribeQuoteTicks, SubscribeTradeTicks as SubscribeTradeTicks, UnsubscribeBars as UnsubscribeBars, UnsubscribeData as UnsubscribeData, UnsubscribeInstrument as UnsubscribeInstrument, UnsubscribeInstrumentStatus as UnsubscribeInstrumentStatus, UnsubscribeInstruments as UnsubscribeInstruments, UnsubscribeOrderBook as UnsubscribeOrderBook, UnsubscribeQuoteTicks as UnsubscribeQuoteTicks, UnsubscribeTradeTicks as UnsubscribeTradeTicks
from nautilus_trader.live.cancellation import DEFAULT_FUTURE_CANCELLATION_TIMEOUT as DEFAULT_FUTURE_CANCELLATION_TIMEOUT, cancel_tasks_with_timeout as cancel_tasks_with_timeout
from nautilus_trader.live.data_client import LiveMarketDataClient as LiveMarketDataClient
from nautilus_trader.model.data import Bar as Bar, DataType as DataType, InstrumentStatus as InstrumentStatus, OrderBookDelta as OrderBookDelta, OrderBookDeltas as OrderBookDeltas, OrderBookDepth10 as OrderBookDepth10, QuoteTick as QuoteTick, TradeTick as TradeTick, capsule_to_data as capsule_to_data
from nautilus_trader.model.enums import BarAggregation as BarAggregation, BookType as BookType, RecordFlag as RecordFlag, bar_aggregation_to_str as bar_aggregation_to_str
from nautilus_trader.model.identifiers import ClientId as ClientId, InstrumentId as InstrumentId, Venue as Venue
from nautilus_trader.model.instruments import instruments_from_pyo3 as instruments_from_pyo3

class DatabentoDataClient(LiveMarketDataClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, http_client: nautilus_pyo3.DatabentoHistoricalClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: DatabentoInstrumentProvider, loader: DatabentoDataLoader | None = None, config: DatabentoDataClientConfig | None = None, name: str | None = None) -> None: ...
    def subscribe_order_book_deltas(self, command: SubscribeOrderBook) -> None: ...
    def subscribe_order_book_snapshots(self, command: SubscribeOrderBook) -> None: ...
    def subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None: ...
    def subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None: ...
    def subscribe_bars(self, command: SubscribeBars) -> None: ...
    def subscribe_instrument_status(self, command: SubscribeInstrumentStatus) -> None: ...
