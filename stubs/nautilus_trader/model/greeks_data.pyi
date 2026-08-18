import numpy as np
from nautilus_trader.core.data import Data as Data
from nautilus_trader.core.datetime import unix_nanos_to_dt as unix_nanos_to_dt, unix_nanos_to_iso8601 as unix_nanos_to_iso8601
from nautilus_trader.core.math import quadratic_interpolation as quadratic_interpolation
from nautilus_trader.model.custom import customdataclass as customdataclass
from nautilus_trader.model.identifiers import InstrumentId as InstrumentId

class GreeksData(Data):
    instrument_id: InstrumentId
    is_call: bool
    strike: float
    expiry: int
    expiry_in_days: int
    expiry_in_years: float
    multiplier: float
    quantity: float
    underlying_price: float
    interest_rate: float
    cost_of_carry: float
    vol: float
    pnl: float
    price: float
    delta: float
    gamma: float
    vega: float
    theta: float
    itm_prob: float
    @classmethod
    def from_delta(cls, instrument_id: InstrumentId, delta: float, multiplier: float, ts_event: int = 0): ...
    def to_portfolio_greeks(self): ...
    def __rmul__(self, quantity): ...

class PortfolioGreeks(Data):
    pnl: float
    price: float
    delta: float
    gamma: float
    vega: float
    theta: float
    def __rmul__(self, quantity): ...
    def __add__(self, other): ...

class YieldCurveData(Data):
    curve_name: str
    tenors: np.ndarray
    interest_rates: np.ndarray
    def __call__(self, expiry_in_years: float) -> float: ...
    def to_dict(self, to_arrow: bool = False): ...
    @classmethod
    def from_dict(cls, data): ...
