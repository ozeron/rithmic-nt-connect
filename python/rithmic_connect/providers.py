"""Instrument provider for configured Rithmic symbol/exchange pairs."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId, Symbol
from nautilus_trader.model.instruments import FuturesContract
from nautilus_trader.model.objects import Currency, Price, Quantity

from rithmic_connect.constants import VENUE
from rithmic_connect.session import WireSession


def _parse_expiration_ns(raw: str | None, fallback_ns: int) -> int:
    """Parse Rithmic expiration strings; fall back to ~90 days ahead."""
    if not raw:
        return fallback_ns
    text = str(raw).strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(text[:10] if fmt != "%Y%m%d" else text[:8], fmt)
            return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1_000_000_000)
        except ValueError:
            continue
    return fallback_ns


def _price_increment(tick_size: float | None, price_precision: int) -> Price:
    if tick_size is None or tick_size <= 0:
        tick_size = 0.25
        price_precision = max(price_precision, 2)
    text = f"{tick_size:.{price_precision}f}".rstrip("0").rstrip(".")
    if "." not in text:
        text = f"{text}.0"
    return Price.from_str(text)


def _multiplier(point_value: float | None) -> Quantity:
    if point_value is None or point_value <= 0:
        return Quantity.from_str("1")
    text = f"{point_value:.8f}".rstrip("0").rstrip(".")
    if "." not in text:
        text = f"{text}.0"
    return Quantity.from_str(text)


def future_from_reference(
    ref: dict[str, Any],
    *,
    fallback_symbol: str,
    fallback_exchange: str,
) -> FuturesContract:
    """Build a FuturesContract from a reference-data venue dict."""
    symbol = str(
        ref.get("trading_symbol")
        or ref.get("symbol")
        or fallback_symbol
    )
    exchange = str(
        ref.get("trading_exchange")
        or ref.get("exchange")
        or fallback_exchange
    )
    tick_size = ref.get("tick_size")
    price_precision = int(ref.get("price_precision") or 2)
    if tick_size is not None:
        tick_size = float(tick_size)
    underlying = str(
        ref.get("underlying")
        or ref.get("product_code")
        or symbol.rstrip("0123456789FGHJKMNQUVXZ")
        or symbol
    )
    currency = Currency.from_str(str(ref.get("currency") or "USD"))
    now = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
    expiration = _parse_expiration_ns(
        ref.get("expiration_date"),
        now + 90 * 24 * 3600 * 1_000_000_000,
    )
    instrument_id = InstrumentId(Symbol(symbol), nautilus_venue())
    return FuturesContract(
        instrument_id=instrument_id,
        raw_symbol=Symbol(symbol),
        asset_class=AssetClass.INDEX,
        currency=currency,
        price_precision=price_precision,
        price_increment=_price_increment(tick_size, price_precision),
        multiplier=_multiplier(ref.get("point_value")),
        lot_size=Quantity.from_str("1"),
        underlying=underlying,
        activation_ns=now,
        expiration_ns=expiration,
        ts_event=now,
        ts_init=now,
        exchange=exchange,
        info={
            "rithmic_exchange": exchange,
            "rithmic_symbol": symbol,
            "rithmic_product_code": ref.get("product_code"),
            "rithmic_instrument_type": ref.get("instrument_type"),
            "rithmic_is_tradable": ref.get("is_tradable"),
        },
    )


def _synthetic_future(symbol: str, exchange: str) -> FuturesContract:
    """Build a minimal FuturesContract when reference data is unavailable."""
    return future_from_reference(
        {
            "symbol": symbol,
            "exchange": exchange,
            "tick_size": 0.25,
            "price_precision": 2,
            "currency": "USD",
        },
        fallback_symbol=symbol,
        fallback_exchange=exchange,
    )


def nautilus_venue():
    from nautilus_trader.model.identifiers import Venue

    return Venue(VENUE)


class RithmicInstrumentProvider(InstrumentProvider):
    """Loads instruments for configured (symbol, exchange) pairs via front-month + ref-data."""

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
            get_ref = getattr(self._session, "get_reference_data", None)
            if callable(get_ref):
                try:
                    ref = get_ref(str(trading_symbol), exchange)
                    if isinstance(ref, dict):
                        instrument = future_from_reference(
                            ref,
                            fallback_symbol=str(trading_symbol),
                            fallback_exchange=exchange,
                        )
                except Exception as exc:  # noqa: BLE001 — keep synthetic
                    self._log.warning(
                        f"reference-data resolve failed for {trading_symbol}/{exchange}: {exc}"
                    )
            self.add(instrument)
