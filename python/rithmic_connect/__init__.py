"""rithmic-connect — unofficial Rithmic adapter compatible with NautilusTrader."""

from __future__ import annotations

from rithmic_connect.config import ConfigError
from rithmic_connect.config import RithmicDataClientConfig
from rithmic_connect.config import RithmicExecClientConfig
from rithmic_connect.config import SessionConfig
from rithmic_connect.constants import ADAPTER_NAME
from rithmic_connect.constants import VENUE
from rithmic_connect.factories import RithmicLiveDataClientFactory
from rithmic_connect.factories import RithmicLiveExecClientFactory

__version__ = "0.1.0"

__all__ = [
    "ADAPTER_NAME",
    "VENUE",
    "ConfigError",
    "SessionConfig",
    "RithmicDataClientConfig",
    "RithmicExecClientConfig",
    "RithmicLiveDataClientFactory",
    "RithmicLiveExecClientFactory",
    "__version__",
]
