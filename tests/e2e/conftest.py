"""Shared fixtures for live Rithmic e2e integration tests (tests/e2e).

Data-only by default — never enables trading. Only the ``*_live.py`` suites in
this directory use these; the default unit run (``uv run pytest -q``) skips
them without ``RITHMIC_TEST_DOTENV``.

Every fixture resolves credentials only through ``explicit_test_env()`` — the
repository-root ``.env`` is never consulted, and production / LucidTrading
systems are refused by the adapter before any connection.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import pytest
from _gateway_parent import running_parent
from _redact import RedactedEnv
from rithmic_nt_connect.config import ConnectMode, env_truthy, system_kind

if TYPE_CHECKING:
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


def parent_trading_env(test_env: dict[str, str] | None) -> dict[str, str]:
    """The trading flag for a harness-started parent (process env wins, as for the
    exec fixtures): the same inputs everywhere, so every user of the session
    parent agrees on its gate.
    """
    trading = env_truthy(
        os.environ.get("RITHMIC_ENABLE_TRADING")
        or (test_env or {}).get("RITHMIC_ENABLE_TRADING")
    )
    return {"RITHMIC_ENABLE_TRADING": "1"} if trading else {}


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
    """Resolve the NQ front month and return (FuturesContract, symbol, exchange)."""
    from rithmic_nt_connect.historical import load_front_month_instrument

    inst = load_front_month_instrument(live_session, "NQ", "CME")
    info = inst.info or {}
    symbol = str(info.get("rithmic_symbol", ""))
    exchange = str(info.get("rithmic_exchange", ""))
    return inst, symbol, exchange


@pytest.fixture(scope="function")
def exec_front_month_instrument(require_test_plant, trading_credentials_available):
    """Resolve the NQ front month after the exec safety gates hold.

    Depends on ``require_test_plant`` and ``trading_credentials_available`` so
    pytest runs the gates *before* this fixture opens any session.
    """
    if not trading_credentials_available:
        pytest.skip(
            "RITHMIC_TEST_DOTENV with test credentials and RITHMIC_ENABLE_TRADING=1 "
            "not set (safe skip for CI)"
        )

    from rithmic_nt_connect.historical import load_front_month_instrument

    with _connected_data_session() as session:
        return load_front_month_instrument(session, "NQ", "CME")
