import msgspec

class BinanceListenKey(msgspec.Struct):
    listenKey: str
