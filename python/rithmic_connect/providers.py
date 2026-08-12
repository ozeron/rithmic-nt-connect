"""Instrument provider for configured Rithmic symbol/exchange pairs."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId, Symbol
from nautilus_trader.model.instruments import FuturesContract
from nautilus_trader.model.objects import Currency, Price, Quantity

from rithmic_connect.constants import VENUE
from rithmic_connect.session import WireSession


def _synthetic_future(symbol: str, exchange: str) -> FuturesContract:
    """Build a minimal FuturesContract so the node can subscribe before full ref-data mapping."""
    instrument_id = InstrumentId(Symbol(symbol), nautilus_venue())
    now = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
    # Far-future expiration placeholder; refined when reference data is wired.
    expiration = now + 90 * 24 * 3600 * 1_000_000_000
    return FuturesContract(
        instrument_id=instrument_id,
        raw_symbol=Symbol(symbol),
        asset_class=AssetClass.INDEX,
        currency=Currency.from_str("USD"),
        price_precision=2,
        price_increment=Price.from_str("0.25"),
        multiplier=Quantity.from_str("1"),
        lot_size=Quantity.from_str("1"),
        underlying=symbol.rstrip("0123456789FGHJKMNQUVXZ") or symbol,
        activation_ns=now,
        expiration_ns=expiration,
        ts_event=now,
        ts_init=now,
        exchange=exchange,
        info={"rithmic_exchange": exchange, "rithmic_symbol": symbol},
    )


def nautilus_venue():
    from nautilus_trader.model.identifiers import Venue

    return Venue(VENUE)


class RithmicInstrumentProvider(InstrumentProvider):
    """Loads instruments for configured (symbol, exchange) pairs via front-month resolution."""

    def __init__(
        self,
        session: WireSession,
        pairs: list[tuple[str, str]],
        config: InstrumentProviderConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        self._session = session
        self._pairs = list(pairs)

    async def load_all_async(self, filters: dict | None = None) -> None:
        _ = filters
        for root, exchange in self._pairs:
            trading_symbol = root
            try:
                front = self._session.get_front_month(root, exchange)
                if isinstance(front, dict):
                    trading_symbol = (
                        front.get("trading_symbol")
                        or front.get("symbol")
                        or root
                    )
                elif isinstance(front, str) and front:
                    trading_symbol = front
            except Exception as exc:  # noqa: BLE001 — soft-fail to configured root
                self._log.warning(f"front-month resolve failed for {root}/{exchange}: {exc}")
            instrument = _synthetic_future(str(trading_symbol), exchange)
            self.add(instrument)
