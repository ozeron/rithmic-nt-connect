from enum import Enum

class Environment(Enum):
    BACKTEST = 'backtest'
    SANDBOX = 'sandbox'
    LIVE = 'live'
