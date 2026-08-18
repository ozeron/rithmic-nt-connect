import pyarrow as pa
from _typeshed import Incomplete
from nautilus_trader.common.config import msgspec_encoding_hook as msgspec_encoding_hook
from nautilus_trader.model.instruments import BettingInstrument as BettingInstrument, BinaryOption as BinaryOption, Cfd as Cfd, Commodity as Commodity, CryptoFuture as CryptoFuture, CryptoFuturesSpread as CryptoFuturesSpread, CryptoOption as CryptoOption, CryptoOptionSpread as CryptoOptionSpread, CryptoPerpetual as CryptoPerpetual, CurrencyPair as CurrencyPair, Equity as Equity, FuturesContract as FuturesContract, FuturesSpread as FuturesSpread, IndexInstrument as IndexInstrument, Instrument as Instrument, OptionContract as OptionContract, OptionSpread as OptionSpread, PerpetualContract as PerpetualContract, TokenizedAsset as TokenizedAsset

SCHEMAS: Incomplete

def serialize(obj: Instrument) -> pa.RecordBatch: ...
def deserialize(batch: pa.RecordBatch) -> list[Instrument]: ...
