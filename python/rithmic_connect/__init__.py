"""rithmic-connect — unofficial Rithmic adapter compatible with NautilusTrader."""

from __future__ import annotations

from rithmic_connect.config import (
    ConfigError,
    RithmicDataClientConfig,
    RithmicExecClientConfig,
    SessionConfig,
)
from rithmic_connect.constants import ADAPTER_NAME, VENUE

__version__ = "0.1.0"

__all__ = [
    "ADAPTER_NAME",
    "ConfigError",
    "RithmicDataClientConfig",
    "RithmicExecClientConfig",
    "SessionConfig",
    "VENUE",
    "__version__",
]
