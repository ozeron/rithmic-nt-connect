"""Unit tests for data client conversion + subscribe wiring (mocked session)."""

from __future__ import annotations

from typing import Any

from _stubs import WireSessionStub
from rithmic_nt_connect._convert import (
    bbo_to_fields,
    last_trade_to_fields,
    order_book_to_fields,
)
from rithmic_nt_connect.data import (
    fields_to_order_book_deltas,
    fields_to_quote_tick,
    fields_to_trade_tick,
)


def test_last_trade_fields_to_trade_tick():
    raw = {
        "type": "last_trade",
        "symbol": "NQU6",
        "exchange": "CME",
        "trade_price": 20000.25,
        "trade_size": 2,
        "aggressor": 1,
        "ts_event_ns": 1_700_000_000_000_000_000,
    }
    fields = last_trade_to_fields(raw)
    tick = fields_to_trade_tick(fields, ts_init=1_700_000_000_000_000_001)
    assert str(tick.instrument_id) == "NQU6.RITHMIC"
    assert float(tick.price) == 20000.25


def test_trade_tick_ids_unique_per_message() -> None:
    """Every received print gets a distinct trade id.

    Rithmic has no venue trade id and identical fields can be a genuine second
    print, so even identical (ts, price, size, aggressor) must not collapse.
    """
    base = {
        "type": "last_trade",
        "symbol": "NQU6",
        "exchange": "CME",
        "aggressor": 1,
        "ts_event_ns": 1_700_000_000_000_000_000,
    }
    sweep_a = dict(base, trade_price=20000.25, trade_size=1)
    sweep_b = dict(base, trade_price=20000.0, trade_size=1)
    identical = dict(base, trade_price=20000.25, trade_size=1)

    ids = {
        fields_to_trade_tick(last_trade_to_fields(raw), ts_init=1).trade_id
        for raw in (sweep_a, sweep_b, identical)
    }

    assert len(ids) == 3, "every received message has a distinct trade id"


def test_trade_tick_uses_instrument_price_precision() -> None:
    raw = {
        "type": "last_trade",
        "symbol": "NQU6",
        "exchange": "CME",
        "trade_price": 21012.5,
        "trade_size": 1,
        "aggressor": 1,
        "ts_event_ns": 1_700_000_000_000_000_000,
    }
    fields = last_trade_to_fields(raw)
    inferred = fields_to_trade_tick(fields, ts_init=1)
    assert inferred.price.precision == 1
    tick = fields_to_trade_tick(fields, ts_init=1, price_precision=2)
    assert tick.price.precision == 2
    assert float(tick.price) == 21012.5


def test_bbo_fields_to_quote_tick():
    raw = {
        "type": "bbo",
        "symbol": "NQU6",
        "exchange": "CME",
        "bid_price": 1.0,
        "ask_price": 2.0,
        "bid_size": 3,
        "ask_size": 4,
        "ssboe": 1_700_000_000,
        "usecs": 0,
    }
    fields = bbo_to_fields(raw)
    assert fields is not None
    tick = fields_to_quote_tick(fields, ts_init=1)
    assert float(tick.bid_price) == 1.0
    assert float(tick.ask_price) == 2.0


def test_order_book_fields_to_deltas():
    from nautilus_trader.model.enums import BookAction, RecordFlag

    raw = {
        "type": "order_book",
        "symbol": "NQU6",
        "exchange": "CME",
        "bid_price": [100.0],
        "bid_size": [2],
        "ask_price": [100.25],
        "ask_size": [3],
        "ts_event_ns": 1_700_000_000_000_000_000,
    }
    fields = order_book_to_fields(raw)
    deltas = fields_to_order_book_deltas(fields, ts_init=1)
    assert str(deltas.instrument_id) == "NQU6.RITHMIC"
    # CLEAR + 2 ADD levels
    assert len(deltas.deltas) == 3
    assert deltas.deltas[0].action == BookAction.CLEAR
    assert int(deltas.deltas[0].flags) == int(RecordFlag.F_SNAPSHOT.value)
    assert int(deltas.deltas[-1].flags) == int(
        RecordFlag.F_SNAPSHOT.value | RecordFlag.F_LAST.value
    )


def test_subscribe_contract_is_callable_on_mock_session():
    calls: list[tuple[str, str]] = []

    class Sess(WireSessionStub):
        def subscribe(self, symbol: str, exchange: str) -> None:
            calls.append((symbol, exchange))

    Sess().subscribe("NQU6", "CME")
    assert calls == [("NQU6", "CME")]


def test_reconnectable_poll_error() -> None:
    from rithmic_nt_connect.data import _reconnectable_poll_error

    assert _reconnectable_poll_error(
        RuntimeError("rithmic error: forced logout: forced logout from server")
    )
    assert _reconnectable_poll_error(RuntimeError("rithmic error: connection closed"))
    assert not _reconnectable_poll_error(RuntimeError("parse failed"))


def test_bar_wire_period_keys_cover_venue_seconds_echo() -> None:
    """Dispatch keys must cover the venue's seconds-period event echo.

    The live venue echoed ``period="60"`` for a 1-MINUTE subscription (seconds)
    while the request uses the native period (1). Without the seconds key, a
    live EXTERNAL bar event never matches its registration and is dropped as
    ``time_bar_unsubscribed`` (the node-path bug the dropped TC-D54 live test's
    bar leg caught before it was removed).
    """
    from nautilus_trader.model.data import BarType
    from rithmic_nt_connect.data import bar_wire_period_keys

    assert bar_wire_period_keys(
        BarType.from_str("NQU6.RITHMIC-1-MINUTE-LAST-EXTERNAL")
    ) == [1, 60]
    assert bar_wire_period_keys(
        BarType.from_str("NQU6.RITHMIC-15-MINUTE-LAST-EXTERNAL")
    ) == [15, 900]
    assert bar_wire_period_keys(
        BarType.from_str("NQU6.RITHMIC-1-HOUR-LAST-EXTERNAL")
    ) == [60, 3600]
    assert bar_wire_period_keys(
        BarType.from_str("NQU6.RITHMIC-1-DAY-LAST-EXTERNAL")
    ) == [1, 86400]


def test_time_bar_dispatch_matches_venue_seconds_period() -> None:
    """A live EXTERNAL bar event (seconds-period echo) must not be dropped."""
    from nautilus_trader.model.data import BarType
    from rithmic_nt_connect.data import (
        bar_type_to_rithmic,
        bar_types_for_event,
        bar_wire_period_keys,
    )

    m1 = BarType.from_str("NQU6.RITHMIC-1-MINUTE-LAST-EXTERNAL")
    symbol, exchange = "NQU6", "CME"
    rtype, _period = bar_type_to_rithmic(m1)
    registered: dict = {}
    for p in bar_wire_period_keys(m1):
        registered.setdefault((symbol, exchange, rtype, p), set()).add(m1)

    # Venue echo observed live: period "60" (seconds) for a 1-MINUTE bar.
    found = bar_types_for_event(
        registered,
        {
            "type": "time_bar",
            "symbol": "NQU6",
            "exchange": "CME",
            "bar_type": 2,
            "period": "60",
        },
    )
    assert found == {m1}


def test_bar_resync_subscriptions_dedupe_dual_keys() -> None:
    """Resync must re-issue one native request per BarType, not both keys."""
    from nautilus_trader.model.data import BarType
    from rithmic_nt_connect.data import (
        bar_resync_subscriptions,
        bar_type_to_rithmic,
        bar_wire_period_keys,
    )

    m1 = BarType.from_str("NQU6.RITHMIC-1-MINUTE-LAST-EXTERNAL")
    symbol, exchange = "NQU6", "CME"
    rtype, period = bar_type_to_rithmic(m1)
    registered: dict = {}
    for p in bar_wire_period_keys(m1):
        registered.setdefault((symbol, exchange, rtype, p), set()).add(m1)

    assert bar_resync_subscriptions(registered) == {(symbol, exchange, rtype, period)}


def test_resync_ticker_session_replays_intent() -> None:
    import asyncio

    from rithmic_nt_connect.data import resync_ticker_session

    calls: list[tuple[Any, ...]] = []

    class Sess(WireSessionStub):
        def reset_ticker(self) -> None:
            calls.append(("reset_ticker",))

        def subscribe(self, symbol: str, exchange: str) -> None:
            calls.append(("subscribe", symbol, exchange))

        def subscribe_order_book_summary(self, symbol: str, exchange: str) -> None:
            calls.append(("book", symbol, exchange))

        def subscribe_time_bars(
            self, symbol: str, exchange: str, bar_type: int, period: int
        ) -> None:
            calls.append(("bars", symbol, exchange, bar_type, period))

    asyncio.run(
        resync_ticker_session(
            Sess(),
            {("NQU6", "CME")},
            {("NQU6", "CME")},
            {("NQU6", "CME", 2, 15)},
        )
    )
    assert calls[0] == ("reset_ticker",)
    assert ("subscribe", "NQU6", "CME") in calls
    assert ("book", "NQU6", "CME") in calls
    assert ("bars", "NQU6", "CME", 2, 15) in calls
