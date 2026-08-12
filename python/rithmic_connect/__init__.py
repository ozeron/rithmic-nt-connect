"""rithmic-connect — unofficial Rithmic adapter compatible with NautilusTrader."""

from __future__ import annotations

from typing import Any

from rithmic_connect.config import ConfigError
from rithmic_connect.config import RithmicDataClientConfig
from rithmic_connect.config import RithmicExecClientConfig
from rithmic_connect.config import SessionConfig
from rithmic_connect.constants import ADAPTER_NAME
from rithmic_connect.constants import VENUE
from rithmic_connect.front_month import FrontMonthError
from rithmic_connect.front_month import resolve_front_month
from rithmic_connect.verify import VerifyReport
from rithmic_connect.verify import run_front_month_verify

__version__ = "0.1.0"

__all__ = [
    "ADAPTER_NAME",
    "VENUE",
    "ConfigError",
    "FrontMonthError",
    "SessionConfig",
    "RithmicDataClientConfig",
    "RithmicExecClientConfig",
    "RithmicLiveDataClientFactory",
    "RithmicLiveExecClientFactory",
    "VerifyReport",
    "resolve_front_month",
    "run_front_month_verify",
    "__version__",
]


def __getattr__(name: str) -> Any:
    """Lazy-load TradingNode factories (requires nautilus_trader)."""
    if name == "RithmicLiveDataClientFactory":
        from rithmic_connect.factories import RithmicLiveDataClientFactory

        return RithmicLiveDataClientFactory
    if name == "RithmicLiveExecClientFactory":
        from rithmic_connect.factories import RithmicLiveExecClientFactory

        return RithmicLiveExecClientFactory
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
