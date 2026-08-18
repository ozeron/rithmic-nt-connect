from nautilus_trader.accounting.accounts.betting import BettingAccount as BettingAccount
from nautilus_trader.accounting.accounts.cash import CashAccount as CashAccount
from nautilus_trader.accounting.accounts.margin import MarginAccount as MarginAccount
from nautilus_trader.accounting.factory import AccountFactory as AccountFactory
from nautilus_trader.core.datetime import dt_to_unix_nanos as dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4 as UUID4
from nautilus_trader.model.enums import ContingencyType as ContingencyType, OrderSide as OrderSide, TimeInForce as TimeInForce, TriggerType as TriggerType
from nautilus_trader.model.identifiers import AccountId as AccountId, ClientOrderId as ClientOrderId, OrderListId as OrderListId, StrategyId as StrategyId, TradeId as TradeId, VenueOrderId as VenueOrderId
from nautilus_trader.model.instruments import Instrument as Instrument
from nautilus_trader.model.orders import LimitOrder as LimitOrder, MarketOrder as MarketOrder, Order as Order, OrderList as OrderList, StopMarketOrder as StopMarketOrder
from nautilus_trader.test_kit.providers import TestInstrumentProvider as TestInstrumentProvider
from nautilus_trader.test_kit.stubs.events import TestEventStubs as TestEventStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs as TestIdStubs

class TestExecStubs:
    @staticmethod
    def cash_account(account_id: AccountId | None = None) -> CashAccount: ...
    @staticmethod
    def margin_account(account_id: AccountId | None = None) -> MarginAccount: ...
    @staticmethod
    def betting_account(account_id: AccountId | None = None) -> BettingAccount: ...
    @staticmethod
    def limit_order(instrument=None, order_side=None, price=None, quantity=None, time_in_force=None, trader_id: TradeId | None = None, strategy_id: StrategyId | None = None, client_order_id: ClientOrderId | None = None, expire_time=None, post_only: bool = False, reduce_only: bool = False, tags=None) -> LimitOrder: ...
    @staticmethod
    def limit_with_stop_market(instrument=None, order_side=None, price=None, quantity=None, time_in_force=None, trader_id: TradeId | None = None, strategy_id: StrategyId | None = None, order_list_id: OrderListId | None = None, entry_client_order_id: ClientOrderId | None = None, sl_client_order_id: ClientOrderId | None = None, sl_trigger_price=None, expire_time=None, tags=None): ...
    @staticmethod
    def market_order(instrument=None, order_side=None, quantity=None, trader_id: TradeId | None = None, strategy_id: StrategyId | None = None, client_order_id: ClientOrderId | None = None, time_in_force=None) -> MarketOrder: ...
    @staticmethod
    def make_submitted_order(order: Order | None = None, instrument: Instrument | None = None, **order_kwargs) -> Order: ...
    @staticmethod
    def make_accepted_order(order: Order | None = None, instrument: Instrument | None = None, account_id: AccountId | None = None, venue_order_id: VenueOrderId | None = None, **order_kwargs) -> Order: ...
    @staticmethod
    def make_filled_order(instrument: Instrument, **kwargs) -> Order: ...
