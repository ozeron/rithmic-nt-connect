"""Venue and adapter constants for rithmic-connect."""

from __future__ import annotations

VENUE: str = "RITHMIC"
"""Nautilus venue identifier for Rithmic-sourced instruments."""

ADAPTER_NAME: str = "RITHMIC"
"""Factory / client name registered on TradingNode."""

DEFAULT_SYSTEM_NAME: str = "LucidTrading"
"""Default R|Protocol system name for LucidTrading prop access."""

DEFAULT_GATEWAY_URL: str = "wss://rprotocol.rithmic.com:443"
"""Default R|Protocol WebSocket URL used with LucidTrading."""

DEFAULT_APP_NAME: str = "rithmic-connect"
DEFAULT_APP_VERSION: str = "0.1.0"
