from nautilus_trader.core.data import Data as Data
from nautilus_trader.serialization.arrow.serializer import register_arrow as register_arrow
from nautilus_trader.serialization.base import register_serializable_type as register_serializable_type

def generate_signal_class(name: str, value_type: type) -> type: ...
