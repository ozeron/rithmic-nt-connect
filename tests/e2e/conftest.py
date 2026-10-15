"""Shared fixtures for live Rithmic e2e integration tests (tests/e2e).

Data-only by default — never enables trading. Only the ``*_live.py`` suites in
this directory use these; the default unit run (``uv run pytest -q``) skips
them without ``RITHMIC_TEST_DOTENV``.

Every fixture resolves credentials only through ``explicit_test_env()`` — the
repository-root ``.env`` is never consulted, and production / LucidTrading
systems are refused by the adapter before any connection.

``RITHMIC_E2E_CONTRACT=NQZ6`` selects a listed contract through the production
reference-data loader instead of front-month resolution. Its exchange is
``RITHMIC_E2E_EXCHANGE`` (default ``CME``). For Test plants without front-month
or history service, also set ``RITHMIC_HISTORY_READY_PROBE=0`` to bypass the
production connect-time history barrier. History tests still run one bounded
availability probe and skip with its reason when replay is unavailable.
"""

from __future__ import annotations

import multiprocessing
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import pytest
from _gateway_parent import running_parent
from live_gates import parent_trading_env
from rithmic_nt_connect._redact import RedactedEnv
from rithmic_nt_connect.config import ConnectMode, env_truthy, system_kind

if TYPE_CHECKING:
    from multiprocessing.connection import Connection

    from nautilus_trader.model.instruments import FuturesContract
    from rithmic_nt_connect.session import WireSession


@contextmanager
def _connected_data_session() -> Iterator[WireSession]:
    """Connect a market-data session; release its credential flock on exit
    (``disconnect()`` does not release it). A failed disconnect keeps the flock
    and fails teardown (one-session rule).
    """
    from rithmic_nt_connect.config import session_config_from_explicit_test_env
    from rithmic_nt_connect.session import connect_market_data_session

    session = connect_market_data_session(session_config_from_explicit_test_env())
    try:
        yield session
    finally:
        session.disconnect()
        lock = getattr(session, "_lock", None)
        if lock is not None:
            lock.close()


def _test_env() -> dict[str, str] | None:
    """Load only the explicitly selected test env source."""
    from rithmic_nt_connect.config import ConfigError, explicit_test_env

    try:
        return RedactedEnv(explicit_test_env())
    except ConfigError:
        return None


@pytest.fixture(scope="session")
def test_env() -> dict[str, str] | None:
    """Explicit test env values, or ``None`` when ``RITHMIC_TEST_DOTENV``
    is unavailable.
    """
    return _test_env()


@pytest.fixture(scope="session", autouse=True)
def _gateway_parent(test_env) -> Iterator[None]:
    """In gateway mode, start the parent for the whole session.

    The gateway client only attaches (it never spawns), so a live run whose test
    env says ``RITHMIC_CONNECT_MODE=gateway`` needs a parent. Only this process's
    parent holds the Rithmic password. Direct mode needs none.
    """
    from _gateway_parent import gateway_config_from_session
    from rithmic_nt_connect.config import (
        parse_connect_mode,
        session_config_from_explicit_test_env,
    )

    mode = (test_env or {}).get("RITHMIC_CONNECT_MODE", "")
    if not test_env or not mode or parse_connect_mode(mode) != ConnectMode.GATEWAY:
        yield
        return
    password = test_env.get("RITHMIC_PASSWORD")
    if not password:
        yield
        return
    gcfg = gateway_config_from_session(session_config_from_explicit_test_env())
    with running_parent(
        gcfg, password=password, extra_env=parent_trading_env(test_env)
    ):
        yield


@pytest.fixture(scope="session")
def credentials_available(test_env) -> bool:
    """Whether market-data e2e tests may run (user + password present)."""
    return bool(
        test_env and test_env.get("RITHMIC_USER") and test_env.get("RITHMIC_PASSWORD")
    )


@pytest.fixture(scope="session")
def trading_credentials_available(test_env) -> bool:
    """Whether execution e2e tests may run (credentials + RITHMIC_ENABLE_TRADING=1)."""
    return bool(
        test_env
        and test_env.get("RITHMIC_USER")
        and test_env.get("RITHMIC_PASSWORD")
        and env_truthy(test_env.get("RITHMIC_ENABLE_TRADING"))
    )


@pytest.fixture
def require_test_plant(test_env) -> str:
    """Hard guard: refuse exec live tests against a production plant.

    Raises (does not skip) when ``RITHMIC_SYSTEM_NAME`` looks like a production
    system; skips when the explicit test env is unavailable.
    """
    if test_env is None:
        pytest.skip("explicit test env unavailable (RITHMIC_TEST_DOTENV)")
    kind = system_kind(test_env["RITHMIC_SYSTEM_NAME"])
    if kind == "production":
        raise RuntimeError(
            "REFUSING exec live test: RITHMIC_SYSTEM_NAME looks like a production "
            "system. Exec live tests may ONLY run against a test/demo plant."
        )
    if kind != "test":
        raise RuntimeError(
            f"REFUSING exec live test: "
            f"RITHMIC_SYSTEM_NAME={test_env['RITHMIC_SYSTEM_NAME']!r} "
            "is not recognized as a test/demo plant."
        )
    return test_env["RITHMIC_SYSTEM_NAME"]


@pytest.fixture(scope="function")
def live_session(credentials_available: bool) -> Iterator[WireSession]:
    """Return a connected *market data only* session (no PnL/order plants).

    Skipped if credentials are not available.
    """
    if not credentials_available:
        pytest.skip(
            "RITHMIC_TEST_DOTENV with test credentials is not set (safe skip for CI)"
        )

    with _connected_data_session() as session:
        yield session


@pytest.fixture(scope="function")
def live_front_month(live_session):
    """Return (instrument, symbol, exchange) for the override or NQ front month."""
    inst = _live_instrument(live_session)
    info = inst.info or {}
    symbol = str(info.get("rithmic_symbol", ""))
    exchange = str(info.get("rithmic_exchange", ""))
    return inst, symbol, exchange


def _live_instrument(session: WireSession) -> FuturesContract:
    from rithmic_nt_connect.historical import (
        load_contract_instrument,
        load_front_month_instrument,
    )

    contract = os.environ.get("RITHMIC_E2E_CONTRACT", "").strip()
    if contract:
        exchange = os.environ.get("RITHMIC_E2E_EXCHANGE", "CME").strip() or "CME"
        return load_contract_instrument(session, contract, exchange)
    return load_front_month_instrument(session, "NQ", "CME")


@pytest.fixture
def live_contract(live_front_month) -> tuple[str, str]:
    """(symbol, exchange) for market-data-only live tests.

    ``RITHMIC_E2E_CONTRACT`` (+ optional ``RITHMIC_E2E_EXCHANGE``, default CME)
    names an exact listed contract and skips the front-month lookup, for Test
    plants whose front-month reply has no trading symbol. Otherwise the NQ front
    month is resolved as in ``live_front_month``.
    """
    _, symbol, exchange = live_front_month
    return symbol, exchange


def _history_probe_worker(connection: Connection) -> None:
    """Own the probe login so a stuck native read can be stopped without leaving
    a background thread holding the credential flock or a live test's session.
    """
    try:
        with _connected_data_session() as session:
            instrument = _live_instrument(session)
            info = instrument.info or {}
            end = int(time.time()) // 60 * 60 - 60
            connection.send(("ready", ""))
            # One raw replay RPC, without load_time_bars' slicing/retry loop.
            rows = session.probe_time_bars(
                str(info["rithmic_symbol"]),
                str(info["rithmic_exchange"]),
                end - 120,
                end,
                2,
                1,
            )
            available = any(row.get("parsed") for row in rows)
            connection.send(
                ("available", "")
                if available
                else ("unavailable", "two-minute 1m replay returned no bars")
            )
    except Exception as exc:
        connection.send(("unavailable", f"{type(exc).__name__}: {exc}"))
    finally:
        connection.close()


def _history_probe_result() -> tuple[bool, str]:
    # Spawn, never fork a process containing Tokio runtimes. This session-scoped
    # probe finishes and releases its login before function-scoped sessions open.
    ctx = multiprocessing.get_context("spawn")
    receiver, sender = ctx.Pipe(duplex=False)
    process = ctx.Process(target=_history_probe_worker, args=(sender,))
    process.start()
    sender.close()
    try:
        if not receiver.poll(45):
            return False, "probe session did not connect within 45s"
        state, reason = receiver.recv()
        if state == "ready":
            if not receiver.poll(5):
                return False, "two-minute 1m replay timed out after 5s"
            state, reason = receiver.recv()
        return state == "available", reason
    except EOFError:
        return False, "history probe process exited without a result"
    finally:
        receiver.close()
        process.join(timeout=1)
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)
        if process.is_alive():
            process.kill()
            process.join(timeout=2)
        process.close()


@pytest.fixture(scope="session")
def live_history_available(credentials_available: bool) -> None:
    """Probe history once per run; skip only tests that require that plant."""
    if not credentials_available:
        pytest.skip("RITHMIC_TEST_DOTENV with test credentials is not set")
    available, reason = _history_probe_result()
    if not available:
        pytest.skip(f"history plant unavailable on this account: {reason}")


@pytest.fixture(scope="function")
def exec_front_month_instrument(require_test_plant, trading_credentials_available):
    """Load the contract override or NQ front month after the exec safety gates.

    Depends on ``require_test_plant`` and ``trading_credentials_available`` so
    pytest runs the gates *before* this fixture opens any session.
    """
    if not trading_credentials_available:
        pytest.skip(
            "RITHMIC_TEST_DOTENV with test credentials and RITHMIC_ENABLE_TRADING=1 "
            "not set (safe skip for CI)"
        )

    with _connected_data_session() as session:
        return _live_instrument(session)
