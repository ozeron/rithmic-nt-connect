import msgspec

class BinanceFuturesCommissionRate(msgspec.Struct, frozen=True):
    symbol: str
    makerCommissionRate: str
    takerCommissionRate: str
