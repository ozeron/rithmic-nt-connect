from nautilus_trader.core.nautilus_pyo3 import AccountBalance as AccountBalance, Currency as Currency, InstrumentId as InstrumentId, MarginBalance as MarginBalance, Money as Money
from nautilus_trader.test_kit.rust.identifiers_pyo3 import TestIdProviderPyo3 as TestIdProviderPyo3

class TestTypesProviderPyo3:
    @staticmethod
    def account_balance(total: Money = ..., locked: Money = ..., free: Money = ...) -> AccountBalance: ...
    @staticmethod
    def margin_balance(initial: Money = ..., maintenance: Money = ..., instrument_id: InstrumentId = ...) -> MarginBalance: ...
