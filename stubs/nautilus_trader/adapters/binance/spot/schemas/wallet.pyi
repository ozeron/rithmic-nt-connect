import msgspec

class BinanceSpotTradeFee(msgspec.Struct, frozen=True):
    symbol: str
    makerCommission: str
    takerCommission: str
