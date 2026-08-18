from nautilus_trader import TEST_DATA_DIR as TEST_DATA_DIR
from nautilus_trader.core.datetime import maybe_dt_to_unix_nanos as maybe_dt_to_unix_nanos
from nautilus_trader.model.objects import Currency as Currency
from nautilus_trader.serialization.arrow.serializer import register_arrow as register_arrow
from nautilus_trader.test_kit.mocks.data import NewsEventData as NewsEventData
from nautilus_trader.trading.filters import NewsImpact as NewsImpact

class TestPersistenceStubs:
    @staticmethod
    def setup_news_event_persistence() -> None: ...
    @staticmethod
    def news_events() -> list[NewsEventData]: ...
