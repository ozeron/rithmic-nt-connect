"""FakeVenue-backed ``LiveNode`` fixtures (no credentials).

Sessions are ``_lib.Session.fake``: the production session host and plant code
over a scripted venue. Shared by the LiveNode end-to-end tests and the offline
self-test of the live exec harness.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from nautilus_trader.common import Environment
from nautilus_trader.config import LiveNodeConfig
from nautilus_trader.live import LiveNode
from nautilus_trader.live.clients import DataClientFactory, ExecutionClientFactory
from nautilus_trader.model import BarType, InstrumentId, TraderId
from rithmic_nt_connect import _lib
from rithmic_nt_connect.config import (
    ConnectMode,
    RithmicLiveDataClientConfig,
    RithmicLiveExecClientConfig,
    SessionConfig,
)
from rithmic_nt_connect.data import RithmicDataClient
from rithmic_nt_connect.execution import RithmicExecutionClient
from rithmic_nt_connect.factories import nautilus_account_id
from rithmic_nt_connect.providers import RithmicInstrumentProvider

HAS_FAKE = hasattr(_lib.Session, "fake")

INSTRUMENT_ID = InstrumentId.from_str("NQH6-CME.RITHMIC")
BAR_TYPE = BarType.from_str("NQH6-CME.RITHMIC-1-MINUTE-LAST-EXTERNAL")
TS = 1_780_000_000_000_000_000


class FakeVenueSession:
    """``_lib.Session.fake`` with an expiration on its reference data.

    The scripted venue's reference reply carries no expiration date, which the
    instrument builder requires; every other member is the real session.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.script = inner.script

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def get_reference_data(self, symbol: str, exchange: str) -> dict[str, Any]:
        ref = dict(self._inner.get_reference_data(symbol, exchange))
        # Far future: a simulated venue rejects orders on an expired contract.
        ref.setdefault("expiration_date", "20991218")
        return ref


def session_config() -> SessionConfig:
    return SessionConfig(
        user="fake",
        connect_mode=ConnectMode.DIRECT,
        password="unused",
        symbol="NQ",
        exchange="CME",
        account_id="ACC1",
    )


class Venues:
    def __init__(self, tmp_path: Any) -> None:
        def fake(name: str) -> FakeVenueSession:
            lock_dir = tmp_path / name
            lock_dir.mkdir()
            return FakeVenueSession(
                _lib.Session.fake(
                    lock_dir=str(lock_dir),
                    account_id="ACC1",
                    fcm_id="FCM",
                    ib_id="IB",
                )
            )

        # ``Any``: the fake stands in for a ``WireSession`` by duck typing.
        self.data: Any = fake("data")
        self.exec: Any = fake("exec")
        self.exec_client: RithmicExecutionClient | None = None


def factories(
    venues: Venues,
    on_exec_client: Callable[[RithmicExecutionClient], None] | None = None,
) -> tuple[type, type]:
    class DataFactory(DataClientFactory):
        @staticmethod
        def create(*, name: str, config: Any, cache: Any, clock: Any) -> Any:
            return RithmicDataClient(
                name=name,
                config=config,
                cache=cache,
                clock=clock,
                instrument_provider=RithmicInstrumentProvider(
                    session=venues.data, pairs=[("NQ", "CME")]
                ),
                session=venues.data,
            )

    class ExecFactory(ExecutionClientFactory):
        @staticmethod
        def create(
            *, name: str, config: Any, cache: Any, clock: Any, trader_id: Any
        ) -> Any:
            client = RithmicExecutionClient(
                name=name,
                config=config,
                cache=cache,
                clock=clock,
                trader_id=trader_id,
                account_id=nautilus_account_id(session_config()),
                instrument_provider=RithmicInstrumentProvider(
                    session=venues.exec, pairs=[("NQ", "CME")]
                ),
                session=venues.exec,
            )
            # A node built on another thread reports its client through the
            # callback: native parts of the client must not outlive that thread.
            if on_exec_client is not None:
                on_exec_client(client)
            else:
                venues.exec_client = client
            return client

    return DataFactory, ExecFactory


async def until(predicate: Any, what: str, timeout: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.02)


def notification(kind: str, tag: str, **extra: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "type": "order_notification",
        "source": "rithmic" if kind == "accepted" else "exchange",
        "kind": kind,
        "status": "OPEN" if kind == "accepted" else "COMPLETE",
        "basket_id": "B1",
        "user_tag": tag,
        "symbol": "NQH6",
        "exchange": "CME",
        "account_id": "ACC1",
        "quantity": 1,
        "price": 21000.0,
        "transaction_type": 1,
        "price_type": 1,
        "duration": 1,
        "ts_event_ns": TS,
    }
    row.update(extra)
    return row


def build_node(
    venues: Venues,
    on_exec_client: Callable[[RithmicExecutionClient], None] | None = None,
) -> LiveNode:
    data_factory, exec_factory = factories(venues, on_exec_client)
    return LiveNode.build(
        "RITHMIC-E2E",
        LiveNodeConfig(
            environment=Environment.SANDBOX,
            trader_id=TraderId("E2E-001"),
            timeout_connection_secs=10,
            timeout_reconciliation_secs=5,
            timeout_portfolio_secs=5,
            timeout_disconnection_secs=5,
            delay_post_stop_secs=0,
            data_clients={
                "RITHMIC": RithmicLiveDataClientConfig(session=session_config())
            },
            exec_clients={
                "RITHMIC": RithmicLiveExecClientConfig(
                    session=session_config(), enable_trading=True
                )
            },
        ),
        data_factories={"RITHMIC": data_factory},
        exec_factories={"RITHMIC": exec_factory},
    )


async def pnl_pushes(venues: Venues) -> None:
    """Rithmic pushes account PnL on a short interval; the order plant re-arms
    only after observing it."""
    while True:
        venues.exec.script.emit(
            "pnl",
            {
                "type": "account_pnl",
                "account_id": "ACC1",
                "fcm_id": "FCM",
                "ib_id": "IB",
                "account_balance": "50000.0",
                "cash_on_hand": "50000.0",
            },
        )
        await asyncio.sleep(0.1)
