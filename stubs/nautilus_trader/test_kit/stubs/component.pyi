from nautilus_trader.backtest.engine import BacktestEngine as BacktestEngine, BacktestEngineConfig as BacktestEngineConfig
from nautilus_trader.backtest.models import FillModel as FillModel
from nautilus_trader.backtest.node import BacktestNode as BacktestNode
from nautilus_trader.cache.cache import Cache as Cache
from nautilus_trader.common.component import LiveClock as LiveClock, MessageBus as MessageBus
from nautilus_trader.common.factories import OrderFactory as OrderFactory
from nautilus_trader.common.functions import get_event_loop as get_event_loop
from nautilus_trader.core.data import Data as Data
from nautilus_trader.model.currencies import USD as USD
from nautilus_trader.model.enums import AccountType as AccountType, OmsType as OmsType
from nautilus_trader.model.identifiers import TraderId as TraderId, Venue as Venue
from nautilus_trader.model.instruments import Instrument as Instrument
from nautilus_trader.model.objects import Currency as Currency, Money as Money
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog as ParquetDataCatalog
from nautilus_trader.portfolio.portfolio import Portfolio as Portfolio
from nautilus_trader.test_kit.mocks.engines import MockLiveDataEngine as MockLiveDataEngine, MockLiveExecutionEngine as MockLiveExecutionEngine, MockLiveRiskEngine as MockLiveRiskEngine
from nautilus_trader.test_kit.stubs.config import TestConfigStubs as TestConfigStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs as TestIdStubs
from nautilus_trader.trading.strategy import Strategy as Strategy

class TestComponentStubs:
    @staticmethod
    def clock() -> LiveClock: ...
    @staticmethod
    def msgbus() -> MessageBus: ...
    @staticmethod
    def cache() -> Cache: ...
    @staticmethod
    def portfolio() -> Portfolio: ...
    @staticmethod
    def trading_strategy() -> Strategy: ...
    @staticmethod
    def mock_live_data_engine() -> MockLiveDataEngine: ...
    @staticmethod
    def mock_live_exec_engine() -> MockLiveExecutionEngine: ...
    @staticmethod
    def mock_live_risk_engine() -> MockLiveRiskEngine: ...
    @staticmethod
    def order_factory() -> OrderFactory: ...
    @staticmethod
    def backtest_node(catalog: ParquetDataCatalog, engine_config: BacktestEngineConfig) -> BacktestNode: ...
    @staticmethod
    def backtest_engine(config: BacktestEngineConfig | None = None, instrument: Instrument | None = None, ticks: list[Data] | None = None, venue: Venue | None = None, oms_type: OmsType | None = None, account_type: AccountType | None = None, base_currency: Currency | None = None, starting_balances: list[Money] | None = None, fill_model: FillModel | None = None) -> BacktestEngine: ...
