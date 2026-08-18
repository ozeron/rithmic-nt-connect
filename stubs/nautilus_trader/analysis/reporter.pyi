import pandas as pd
from nautilus_trader.accounting.accounts.base import Account as Account
from nautilus_trader.core.datetime import unix_nanos_to_dt as unix_nanos_to_dt
from nautilus_trader.model.events import AccountState as AccountState, OrderFilled as OrderFilled
from nautilus_trader.model.orders import Order as Order
from nautilus_trader.model.position import Position as Position

class ReportProvider:
    @staticmethod
    def generate_orders_report(orders: list[Order]) -> pd.DataFrame: ...
    @staticmethod
    def generate_order_fills_report(orders: list[Order]) -> pd.DataFrame: ...
    @staticmethod
    def generate_fills_report(orders: list[Order]) -> pd.DataFrame: ...
    @staticmethod
    def generate_positions_report(positions: list[Position], snapshots: list[Position] | None = None) -> pd.DataFrame: ...
    @staticmethod
    def generate_account_report(account: Account) -> pd.DataFrame: ...
