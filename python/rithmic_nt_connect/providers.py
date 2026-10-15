"""Instrument provider for configured Rithmic symbol/exchange pairs."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId, Symbol
from nautilus_trader.model.instruments import FuturesContract
from nautilus_trader.model.objects import Currency, Price, Quantity

from rithmic_nt_connect.session import WireSession


class InstrumentBuildError(ValueError):
    """Raised when venue reference data cannot build an instrument."""


def _parse_expiration_ns(raw: str) -> int:
    """Parse Rithmic expiration strings; raise if unrecognized."""
    text = str(raw).strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            slice_len = 8 if fmt == "%Y%m%d" else 10
            dt = datetime.strptime(text[:slice_len], fmt)
            return int(dt.replace(tzinfo=UTC).timestamp() * 1_000_000_000)
        except ValueError:
            continue
    raise InstrumentBuildError(f"unrecognized expiration_date: {raw!r}")


def _price_increment(tick_size: float, price_precision: int) -> Price:
    if tick_size <= 0:
        raise InstrumentBuildError(f"tick_size must be > 0, got {tick_size}")
    if price_precision < 0:
        raise InstrumentBuildError(
            f"price_precision must be >= 0, got {price_precision}"
        )
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


def future_from_reference(
    ref: dict[str, Any],
    *,
    activation_ns: int | None = None,
) -> FuturesContract:
    """Build a FuturesContract from a reference-data venue dict (no invented fields)."""
    symbol = ref.get("trading_symbol") or ref.get("symbol")
    exchange = ref.get("trading_exchange") or ref.get("exchange")
    tick_size = ref.get("tick_size")
    price_precision = ref.get("price_precision")
    if price_precision is None and tick_size is not None:
        # Some Test reference replies omit display precision. The venue tick
        # still defines the exact decimal increment; never infer it from prices.
        increment = Decimal(str(tick_size))
        if not increment.is_finite() or increment <= 0:
            raise InstrumentBuildError("tick_size must be finite and > 0")
        price_precision = max(0, -int(increment.normalize().as_tuple().exponent))
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
        raise InstrumentBuildError(
            f"reference data missing required fields: {', '.join(missing)}"
        )

    symbol_s = str(symbol)
    exchange_s = str(exchange)
    underlying = ref.get("underlying") or ref.get("product_code")
    if not underlying:
        raise InstrumentBuildError("reference data missing underlying/product_code")
    if tick_size is None or price_precision is None or point_value is None:
        # The required-field check above already rejected None; this narrows the
        # types for the numeric conversions below.
        raise InstrumentBuildError("reference data missing numeric fields")

    now = int(datetime.now(UTC).timestamp() * 1_000_000_000)
    # 0 = tradable for any backtest window. "now" rejects historical orders
    # ("Contract not yet active").
    act_ns = 0 if activation_ns is None else int(activation_ns)
    from rithmic_nt_connect._convert import instrument_id_from_symbol

    instrument_id = InstrumentId.from_str(
        instrument_id_from_symbol(symbol_s, exchange_s)
    )
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
        activation_ns=act_ns,
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


class RithmicInstrumentProvider(InstrumentProvider):
    """Load configured (symbol, exchange) pairs.

    Listed contracts use reference data directly; product roots resolve the
    front month first. IDs use the adapter codec (``NQZ6-CME.RITHMIC``).
    """

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
        from rithmic_nt_connect.exposure import product_root
        from rithmic_nt_connect.historical import (
            load_contract_instrument,
            load_front_month_instrument,
        )

        for symbol, exchange in self._pairs:
            symbol = symbol.strip().upper()
            if product_root(symbol) != symbol:
                instrument = await asyncio.to_thread(
                    load_contract_instrument, self._session, symbol, exchange
                )
            else:
                instrument = await asyncio.to_thread(
                    load_front_month_instrument, self._session, symbol, exchange
                )
            self.add(instrument)
