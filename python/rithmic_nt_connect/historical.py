"""Standalone history + instrument load (IB / Databento historical-client pattern).

Examples and ``RithmicDataClient._request_*`` share this convert path. Wire
windowing / retry / dedup stay in the Rust session.
"""

from __future__ import annotations

from datetime import UTC, datetime

from nautilus_trader.model.data import Bar, BarType, TradeTick
from nautilus_trader.model.instruments import FuturesContract

from rithmic_nt_connect._convert import rithmic_route_from_info
from rithmic_nt_connect.data import (
    bar_type_to_rithmic,
    payloads_to_bars,
    payloads_to_trade_ticks,
)
from rithmic_nt_connect.front_month import resolve_front_month
from rithmic_nt_connect.pandas_compat import patch_nautilus_pandas
from rithmic_nt_connect.providers import future_from_reference
from rithmic_nt_connect.session import WireSession

patch_nautilus_pandas()


def _unix_sec(value: datetime | int | float) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp())
    return int(value)


def load_front_month_instrument(
    session: WireSession,
    root: str,
    exchange: str,
    *,
    activation_ns: int = 0,
) -> FuturesContract:
    """Resolve listed front month and build a ``FuturesContract``."""
    front = resolve_front_month(session, root, exchange)
    ref = session.get_reference_data(
        str(front["trading_symbol"]),
        str(front["trading_exchange"]),
    )
    if not isinstance(ref, dict):
        raise TypeError(
            f"reference data must be a dict for {root}/{exchange}, got {type(ref)!r}"
        )
    return future_from_reference(ref, activation_ns=activation_ns)


def load_trade_ticks(
    session: WireSession,
    instrument: FuturesContract,
    start: datetime | int,
    end: datetime | int,
) -> list[TradeTick]:
    """Load a history window as Nautilus ticks (Rust slices/retries/dedup)."""
    symbol, exchange = rithmic_route_from_info(
        instrument.info or {},
        instrument_id=str(instrument.id),
    )
    raw = session.load_ticks(symbol, exchange, _unix_sec(start), _unix_sec(end))
    ts_init = int(datetime.now(UTC).timestamp() * 1_000_000_000)
    return payloads_to_trade_ticks(
        list(raw or []),
        symbol=symbol,
        exchange=exchange,
        price_precision=int(instrument.price_precision),
        ts_init=ts_init,
    )


def load_time_bars(
    session: WireSession,
    instrument: FuturesContract,
    start: datetime | int,
    end: datetime | int,
    bar_type: BarType,
) -> list[Bar]:
    """Load venue time bars for ``bar_type`` only (no finer aggregation)."""
    symbol, exchange = rithmic_route_from_info(
        instrument.info or {},
        instrument_id=str(instrument.id),
    )
    rithmic_bar_type, period = bar_type_to_rithmic(bar_type)
    raw = session.load_time_bars(
        symbol,
        exchange,
        _unix_sec(start),
        _unix_sec(end),
        rithmic_bar_type,
        period,
    )
    ts_init = int(datetime.now(UTC).timestamp() * 1_000_000_000)
    return payloads_to_bars(
        list(raw or []),
        symbol=symbol,
        exchange=exchange,
        bar_type=bar_type,
        price_precision=int(instrument.price_precision),
        ts_init=ts_init,
    )
