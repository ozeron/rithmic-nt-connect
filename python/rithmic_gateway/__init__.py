"""Lake-friendly Rithmic gateway client (no Nautilus, no maturin).

Talks plant-semantic protobuf over a unix socket to ``rithmic-gateway``.
"""

from __future__ import annotations

from rithmic_gateway.client import GatewayClient, GatewayError
from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.flock import SessionLock, SessionLockError
from rithmic_gateway.spawn import spawn_gateway

__all__ = [
    "GatewayClient",
    "GatewayConfig",
    "GatewayError",
    "SessionLock",
    "SessionLockError",
    "spawn_gateway",
]
