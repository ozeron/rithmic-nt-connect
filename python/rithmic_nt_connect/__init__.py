"""rithmic-nt-connect — unofficial Rithmic adapter compatible with NautilusTrader."""

from __future__ import annotations

from typing import Any

from rithmic_nt_connect.config import (
    ConfigError,
    ConnectMode,
    RithmicDataClientConfig,
    RithmicExecClientConfig,
    RithmicLiveDataClientConfig,
    RithmicLiveExecClientConfig,
    SessionConfig,
    env_truthy,
    explicit_test_env,
    load_dotenv,
    load_dotenv_files,
    session_config_from_explicit_test_env,
)
from rithmic_nt_connect.constants import ADAPTER_NAME, VENUE
from rithmic_nt_connect.front_month import FrontMonthError, resolve_front_month
from rithmic_nt_connect.systems import list_systems
from rithmic_nt_connect.verify import VerifyReport, run_front_month_verify

__version__ = "0.1.0"

__all__ = [
    "ADAPTER_NAME",
    "VENUE",
    "ConfigError",
    "ConnectMode",
    "FrontMonthError",
    "RithmicDataClientConfig",
    "RithmicExecClientConfig",
    "RithmicLiveDataClientConfig",
    "RithmicLiveDataClientFactory",
    "RithmicLiveExecClientConfig",
    "RithmicLiveExecClientFactory",
    "SessionConfig",
    "VerifyReport",
    "__version__",
    "connect_market_data_session",
    "env_truthy",
    "explicit_test_env",
    "list_systems",
    "load_dotenv",
    "load_dotenv_files",
    "load_front_month_instrument",
    "load_time_bars",
    "load_trade_ticks",
    "resolve_front_month",
    "run_front_month_verify",
    "session_config_from_explicit_test_env",
]


def __getattr__(name: str) -> Any:
    """Lazy-load TradingNode factories (requires nautilus_trader)."""
    if name == "RithmicLiveDataClientFactory":
        from rithmic_nt_connect.factories import RithmicLiveDataClientFactory

        return RithmicLiveDataClientFactory
    if name == "RithmicLiveExecClientFactory":
        from rithmic_nt_connect.factories import RithmicLiveExecClientFactory

        return RithmicLiveExecClientFactory
    if name == "connect_market_data_session":
        from rithmic_nt_connect.session import connect_market_data_session

        return connect_market_data_session
    if name == "load_front_month_instrument":
        from rithmic_nt_connect.historical import load_front_month_instrument

        return load_front_month_instrument
    if name == "load_trade_ticks":
        from rithmic_nt_connect.historical import load_trade_ticks

        return load_trade_ticks
    if name == "load_time_bars":
        from rithmic_nt_connect.historical import load_time_bars

        return load_time_bars
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
