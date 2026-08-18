import msgspec
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType as BinanceAccountType
from nautilus_trader.adapters.binance.common.schemas.account import BinanceOrder as BinanceOrder
from nautilus_trader.model.objects import AccountBalance as AccountBalance, Currency as Currency, Money as Money

class BinanceSpotBalanceInfo(msgspec.Struct, frozen=True):
    asset: str
    free: str
    locked: str
    def parse_to_account_balance(self) -> AccountBalance: ...

class BinanceSpotAccountInfo(msgspec.Struct, frozen=True):
    makerCommission: int
    takerCommission: int
    buyerCommission: int
    sellerCommission: int
    canTrade: bool
    canWithdraw: bool
    canDeposit: bool
    updateTime: int
    accountType: BinanceAccountType
    balances: list[BinanceSpotBalanceInfo]
    permissions: list[str]
    def parse_to_account_balances(self) -> list[AccountBalance]: ...

class BinanceSpotOrderOco(msgspec.Struct, frozen=True):
    orderListId: int
    contingencyType: str
    listStatusType: str
    listOrderStatus: str
    listClientOrderId: str
    transactionTime: int
    symbol: str
    orders: list[BinanceOrder] | None = ...
    orderReports: list[BinanceOrder] | None = ...
