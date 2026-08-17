"""Shared fixtures for live Rithmic integration tests.

Data-only fixtures — never enables trading.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from rithmic_nt_connect.session import WireSession


def _load_dotenv() -> None:
    """Load .env from the project root if not already loaded."""
    if os.environ.get("RITHMIC_USER") or os.environ.get("RITHMIC_LIVE_USER"):
        return  # already loaded
    from rithmic_nt_connect.config import load_dotenv_files

    root = Path(__file__).resolve().parents[1]
    load_dotenv_files(root / ".env")


def _has_credentials() -> bool:
    _load_dotenv()
    user = os.environ.get("RITHMIC_USER") or os.environ.get("RITHMIC_LIVE_USER")
    pw = os.environ.get("RITHMIC_PASSWORD") or os.environ.get("RITHMIC_LIVE_PW")
    return bool(user and pw)


@pytest.fixture(scope="session")
def credentials_available() -> bool:
    """Skip or fail if no LucidTrading credentials are available."""
    return _has_credentials()


@pytest.fixture(scope="function")
def live_session(credentials_available: bool) -> "WireSession":
    """Return a connected *market data only* session (no PnL/order plants).

    Skipped if credentials are not available.
    """
    if not credentials_available:
        pytest.skip("RITHMIC_USER / RITHMIC_PASSWORD not set (safe skip for CI)")

    from rithmic_nt_connect.session import connect_market_data_session

    session = connect_market_data_session()
    try:
        yield session
    finally:
        try:
            session.disconnect()
        except Exception:
            pass
        # Release the credential flock so the next function-scoped fixture
        # can acquire it (disconnect() does not release the lock).
        lock = getattr(session, "_lock", None)
        if lock is not None:
            try:
                lock.close()
            except Exception:
                pass


@pytest.fixture(scope="function")
def live_front_month(live_session):
    """Resolve the NQ front month and return (FuturesContract, symbol, exchange)."""
    from rithmic_nt_connect.historical import load_front_month_instrument

    inst = load_front_month_instrument(live_session, "NQ", "CME")
    info = inst.info or {}
    symbol = str(info.get("rithmic_symbol", ""))
    exchange = str(info.get("rithmic_exchange", ""))
    return inst, symbol, exchange