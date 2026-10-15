"""Trading-gate helpers shared by live tests (importable; not a conftest)."""

from __future__ import annotations

import os

import pytest
from rithmic_nt_connect.config import env_truthy


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


def open_trading_gates_or_skip(session, test_env: dict[str, str] | None) -> None:
    """Open the session's trading gate, or skip when the run did not opt in.

    Order-plant reads (account discovery, RMS) are trading-gated in both modes.
    The exec client opens the gate in production; a raw test session must do
    the same. In gateway mode the parent decides, and `open_gates` only reports.
    """
    if not parent_trading_env(test_env):
        pytest.skip("needs RITHMIC_ENABLE_TRADING=1 (the order plant is trading-gated)")
    gates = session.open_gates(trading=True, cancel_all=False)
    if not gates.trading:
        pytest.skip("trading not granted (gateway parent started without trading)")
