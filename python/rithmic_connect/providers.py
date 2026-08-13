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


class InstrumentBuildError(ValueError):
    """Raised when venue reference data cannot build an instrument."""


def _parse_expiration_ns(raw: str) -> int:
    """Parse Rithmic expiration strings; raise if unrecognized."""
    text = str(raw).strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            slice_len = 8 if fmt == "%Y%m%d" else 10
            dt = datetime.strptime(text[:slice_len], fmt)
            return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1_000_000_000)
        except ValueError:
            continue
    raise InstrumentBuildError(f"unrecognized expiration_date: {raw!r}")


def _price_increment(tick_size: float, price_precision: int) -> Price:
    if tick_size <= 0:
        raise InstrumentBuildError(f"tick_size must be > 0, got {tick_size}")
    if price_precision < 0:
        raise InstrumentBuildError(f"price_precision must be >= 0, got {price_precision}")
    text = f"{tick_size:.{price_precision}f}".rstrip("0").rstrip(".")
    if "." not in text:
        text = f"{text}.0"
    return Price.from_str(text)


def _multiplier(point_value: float) -> Quantity:
    if point_value <= 0:
        raise InstrumentBuildError(f"point_value must be > 0, got {point_value}")
    text = f"{point_value:.8f}".rstrip("0").rstrip(".")
    if "." not in text:
        text = f"{text}.0"
    return Quantity.from_str(text)


def future_from_reference(ref: dict[str, Any]) -> FuturesContract:
    """Build a FuturesContract from a reference-data venue dict (no invented fields)."""
    symbol = ref.get("trading_symbol") or ref.get("symbol")
    exchange = ref.get("trading_exchange") or ref.get("exchange")
    tick_size = ref.get("tick_size")
    price_precision = ref.get("price_precision")
    point_value = ref.get("point_value")
    currency_raw = ref.get("currency")
    expiration_raw = ref.get("expiration_date")
    missing = [
        name
        for name, value in (
            ("symbol", symbol),
            ("exchange", exchange),
            ("tick_size", tick_size),
            ("price_precision", price_precision),
            ("point_value", point_value),
            ("currency", currency_raw),
            ("expiration_date", expiration_raw),
        )
        if value is None or value == ""
    ]
    if missing:
        raise InstrumentBuildError(f"reference data missing required fields: {', '.join(missing)}")

    symbol_s = str(symbol)
    exchange_s = str(exchange)
    underlying = ref.get("underlying") or ref.get("product_code")
    if not underlying:
        raise InstrumentBuildError("reference data missing underlying/product_code")

    now = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
    instrument_id = InstrumentId(Symbol(symbol_s), nautilus_venue())
    return FuturesContract(
        instrument_id=instrument_id,
        raw_symbol=Symbol(symbol_s),
        asset_class=AssetClass.INDEX,
        currency=Currency.from_str(str(currency_raw)),
        price_precision=int(price_precision),
        price_increment=_price_increment(float(tick_size), int(price_precision)),
        multiplier=_multiplier(float(point_value)),
        lot_size=Quantity.from_str("1"),
        underlying=str(underlying),
        activation_ns=now,
        expiration_ns=_parse_expiration_ns(str(expiration_raw)),
        ts_event=now,
        ts_init=now,
        exchange=exchange_s,
        info={
            "rithmic_exchange": exchange_s,
            "rithmic_symbol": symbol_s,
            "rithmic_product_code": ref.get("product_code"),
            "rithmic_instrument_type": ref.get("instrument_type"),
            "rithmic_is_tradable": ref.get("is_tradable"),
        },
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
            front = self._session.get_front_month(root, exchange)
            if not isinstance(front, dict):
                raise InstrumentBuildError(
                    f"front-month response must be a dict for {root}/{exchange}, got {type(front)!r}"
                )
            trading_symbol = front.get("trading_symbol")
            trading_exchange = front.get("trading_exchange")
            if not trading_symbol or not trading_exchange:
                raise InstrumentBuildError(
                    f"front-month missing trading_symbol/trading_exchange for {root}/{exchange}"
                )
            get_ref = getattr(self._session, "get_reference_data", None)
            if not callable(get_ref):
                raise InstrumentBuildError("session missing get_reference_data")
            ref = get_ref(str(trading_symbol), str(trading_exchange))
            if not isinstance(ref, dict):
                raise InstrumentBuildError(
                    f"reference data must be a dict for {trading_symbol}/{trading_exchange}"
                )
            self.add(future_from_reference(ref))
