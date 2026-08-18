from _typeshed import Incomplete
from decimal import Decimal
from nautilus_trader.model.objects import Currency as Currency

class AccountError(Exception): ...

class AccountBalanceNegative(AccountError):
    balance: Incomplete
    currency: Incomplete
    def __init__(self, balance: Decimal, currency: Currency) -> None: ...

class AccountMarginExceeded(AccountError):
    balance: Incomplete
    margin: Incomplete
    currency: Incomplete
    def __init__(self, balance: Decimal, margin: Decimal, currency: Currency) -> None: ...
