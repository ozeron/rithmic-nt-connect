"""Data recovery reconciles local state; the host owns subscription restore."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from _stubs import WireSessionStub, _CaptureLog, _TestDataClient
from rithmic_nt_connect import _lib
from rithmic_nt_connect.data import BookAssembler


def test_resumed_reconciles_without_market_data_replay(tmp_path: Path) -> None:
    async def run() -> None:
        session: Any = _lib.Session.fake(lock_dir=str(tmp_path))
        await asyncio.to_thread(session.connect)
        await asyncio.to_thread(session.subscribe, "NQU6", "CME")
        client = _TestDataClient.__new__(_TestDataClient)
        client._loop = asyncio.get_running_loop()
        client._log = _CaptureLog()
        client._session = session
        client._bar_types = {}
        client._bbo_state = {"NQU6:CME": {"bid_price": 100}}
        client._books = BookAssembler()
        client._recovery = client._build_recovery()
        client._recovery.attach(session)
        session.script.fail_stream("ticker")
        async with asyncio.timeout(3):
            while client._recovery.last_generation != 2:
                await asyncio.sleep(0.005)
        assert client._bbo_state == {}
        calls = session.script.calls()
        assert calls.count("reset_ticker") == 1
        # Original join plus the host's own restore, with no adapter replay.
        assert calls.count("subscribe NQU6 CME") == 2
        session.script.emit(
            "ticker",
            {
                "type": "last_trade",
                "symbol": "NQU6",
                "exchange": "CME",
                "trade_price": 42.0,
            },
        )
        events = await asyncio.to_thread(session.next_events, "ticker", 1, 1000)
        assert events[0]["trade_price"] == pytest.approx(42.0)
        await client._recovery.detach()
        await asyncio.to_thread(session.disconnect)

    asyncio.run(run())


def test_disconnect_drops_listener_and_cancels_reconciliation() -> None:
    class Sess(WireSessionStub):
        def __init__(self) -> None:
            self.unsubscribed = 0
            self.disconnected = False

        def subscribe_session_events(self, listener: Any) -> Any:
            def cancel() -> None:
                self.unsubscribed += 1

            return SimpleNamespace(cancel=cancel)

        def disconnect(self) -> None:
            self.disconnected = True

    async def run() -> None:
        client = _TestDataClient.__new__(_TestDataClient)
        client._loop = asyncio.get_running_loop()
        client._log = _CaptureLog()
        sess = Sess()
        client._session = sess
        client._poll_task = None
        client._history_poll_task = None
        client._recovery = client._build_recovery()
        client._recovery.attach(sess)
        in_flight = asyncio.ensure_future(asyncio.sleep(3600))
        client._recovery.task = in_flight
        await client._disconnect()
        assert in_flight.cancelled()
        assert sess.unsubscribed == 1
        assert sess.disconnected
        assert client._recovery.task is None

    asyncio.run(run())
