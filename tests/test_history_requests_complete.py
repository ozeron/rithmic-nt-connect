"""A history request always completes, never hangs.

NautilusTrader 2: a fetch completes with a typed response (possibly empty); a
failure raises and the engine hands its message to the requester as
``failure_reason``.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from _stubs import WireSessionStub, _CaptureLog, _TestDataClient
from nautilus_trader.core import UUID4
from nautilus_trader.model import BarType, ClientId, InstrumentId
from rithmic_nt_connect.data import BarSequenceGuard, request_bars_error

NQ = "NQU6-CME.RITHMIC"


def _client(session: Any) -> tuple[Any, list[tuple[str, list[Any]]]]:
    # ``Any``: the doubles below replace bound methods, which the checker forbids.
    client: Any = _TestDataClient.__new__(_TestDataClient)
    client._log = _CaptureLog()
    client._instrument_routes = {NQ: ("NQU6", "CME")}
    client._session = session
    client._clock = SimpleNamespace(timestamp_ns=lambda: 1)
    client._skip_counts = {}
    client._skip_last_flush = float("inf")  # keep counts unflushed for asserts
    client._price_precision = lambda _iid: 2
    handled: list[tuple[str, list[Any]]] = []
    client.request_ids = []
    client.client_id = ClientId("RITHMIC")

    def handle_response(response: Any) -> None:
        kind = "bars" if hasattr(response, "bar_type") else "ticks"
        handled.append((kind, list(response.data)))
        client.request_ids.append(response.correlation_id)

    client._handle_response = handle_response
    return client, handled


REQUEST_ID = UUID4()


def _window() -> dict[str, Any]:
    start = datetime(2026, 10, 14, 14, 0, tzinfo=UTC)
    end = datetime(2026, 10, 14, 14, 5, tzinfo=UTC)
    return {
        "start": start,
        "end": end,
        "start_ns": int(start.timestamp()) * 1_000_000_000,
        "end_ns": int(end.timestamp()) * 1_000_000_000,
        "request_id": REQUEST_ID,
        "params": None,
        "limit": 0,
    }


def test_a_tick_request_for_an_unloaded_instrument_fails_with_a_reason() -> None:
    client, handled = _client(WireSessionStub())
    request: Any = SimpleNamespace(
        instrument_id=InstrumentId.from_str("ESZ6-CME.RITHMIC"), **_window()
    )
    with pytest.raises(ValueError, match="route"):
        asyncio.run(client._request_trades(request))
    assert handled == [], "a failure is not reported as an empty success"


def test_a_bar_request_for_an_unloaded_instrument_fails_with_a_reason() -> None:
    client, handled = _client(WireSessionStub())
    bar_type = BarType.from_str("ESZ6-CME.RITHMIC-1-MINUTE-LAST-EXTERNAL")
    request: Any = SimpleNamespace(bar_type=bar_type, **_window())
    with pytest.raises(ValueError, match="route"):
        asyncio.run(client._request_bars(request))
    assert handled == []
    assert request_bars_error(REQUEST_ID) == "history_error"
    assert any("route" in m for m in client._log.messages)


def test_an_empty_bar_history_completes_under_the_request_id() -> None:
    class Session(WireSessionStub):
        def load_time_bars(self, *args: Any) -> list[dict[str, Any]]:
            return []

    client, handled = _client(Session())
    client._instrument_routes = {NQ: ("NQU6", "CME")}
    client._bar_seq = BarSequenceGuard()
    bar_type = BarType.from_str(f"{NQ}-1-MINUTE-LAST-EXTERNAL")
    request: Any = SimpleNamespace(bar_type=bar_type, **_window())
    asyncio.run(client._request_bars(request))
    assert handled == [("bars", [])]
    assert client.request_ids == [REQUEST_ID]
    assert request_bars_error(REQUEST_ID) is None


def test_one_bad_tick_row_is_skipped_not_the_whole_request() -> None:
    # The dict the extension builds for a history tick (history_tick_dict).
    good = {
        "type": "history_tick",
        "symbol": "NQU6",
        "exchange": "CME",
        "close_price": 21000.25,
        "trade_price": 21000.25,
        "trade_size": 1,
        "volume": 1,
        "num_trades": 1,
        "ssboe": 1_790_000_000,
        "usecs": 0,
        "ts_event_ns": 1_790_000_000_000_000_000,
    }
    bad = dict(good, trade_size=0, ts_event_ns=1_790_000_000_000_000_001)

    class Session(WireSessionStub):
        def load_ticks(self, *args: Any) -> list[dict[str, Any]]:
            return [good, bad]

    client, handled = _client(Session())
    request: Any = SimpleNamespace(instrument_id=InstrumentId.from_str(NQ), **_window())
    asyncio.run(client._request_trades(request))
    kind, ticks = handled[0]
    assert kind == "ticks" and len(ticks) == 1
    assert client._skip_counts.get("history_tick_invalid") == 1


def test_unsubscribing_one_bar_period_leaves_the_venue_for_that_period() -> None:
    from rithmic_nt_connect.data import bar_type_to_rithmic, bar_wire_period_keys

    one = BarType.from_str(f"{NQ}-1-MINUTE-LAST-EXTERNAL")
    fifteen = BarType.from_str(f"{NQ}-15-MINUTE-LAST-EXTERNAL")
    calls: list[tuple[Any, ...]] = []

    class Session(WireSessionStub):
        def unsubscribe_time_bars(self, *args: Any) -> None:
            calls.append(args)

    client, _ = _client(Session())
    client._bar_types = {}
    for bar_type in (one, fifteen):
        rtype, _period = bar_type_to_rithmic(bar_type)
        for p in bar_wire_period_keys(bar_type):
            client._bar_types[("NQU6", "CME", rtype, p)] = {bar_type}
    command: Any = SimpleNamespace(bar_types=[fifteen])
    asyncio.run(client._unsubscribe_bars(command))
    assert calls == [("NQU6", "CME", bar_type_to_rithmic(fifteen)[0], 15)]
