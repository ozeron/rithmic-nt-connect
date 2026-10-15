"""Data recovery reconciles local state; the host owns subscription restore."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from _stubs import WireSessionStub, _CaptureLog, _TestDataClient
from rithmic_nt_connect.data import BookAssembler
from rithmic_nt_connect.transport import Resumed


def test_resumed_reconciles_without_market_data_replay() -> None:
    async def run() -> None:
        client = _TestDataClient.__new__(_TestDataClient)
        client._loop = asyncio.get_running_loop()
        client._log = _CaptureLog()
        # Any session call (including subscribe/reset) raises on this stub.
        client._session = WireSessionStub()
        client._bar_types = {}
        client._bbo_state = {"NQU6:CME": {"bid_price": 100}}
        client._books = BookAssembler()
        client._recovery = client._build_recovery()
        client._recovery.on_event(Resumed(generation=2))
        await asyncio.sleep(0)
        assert client._recovery.task is not None
        await client._recovery.task
        assert client._recovery.last_generation == 2
        assert client._bbo_state == {}

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
