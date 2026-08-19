"""Lake-friendly Rithmic gateway client (no Nautilus, no maturin).

Talks plant-semantic protobuf over a unix socket to ``rithmic-gateway``.

``GatewayClient`` is imported lazily so ``direct`` plant sessions can take
the credential flock without loading the protobuf gencode at all. The
bundled gencode carries the protobuf 5.29.6 marker — the exact version
``nautilus_trader[ib]==1.231.0`` pins — so the wire client imports on that
runtime (and any newer one). Regenerate it reproducibly from
``proto/rithmic_gateway/v1/session.proto`` with the uv-pinned protoc:
``uv run python scripts/gen_gateway_proto.py`` (grpcio-tools in the dev
dependency group bundles protoc 29.0; the script restores the 5.29.6 marker).
"""

from __future__ import annotations

from typing import Any

from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.flock import SessionLock, SessionLockError, session_flock_held
from rithmic_gateway.history_window import (
    BAR_TYPE_DAILY,
    BAR_TYPE_MINUTE,
    BAR_TYPE_SECOND,
    BAR_TYPE_WEEKLY,
    bar_slice_secs,
    window_slices,
)
from rithmic_gateway.spawn import resolve_gateway_bin, spawn_gateway

__all__ = [
    "BAR_TYPE_DAILY",
    "BAR_TYPE_MINUTE",
    "BAR_TYPE_SECOND",
    "BAR_TYPE_WEEKLY",
    "GatewayClient",
    "GatewayConfig",
    "GatewayError",
    "SessionLock",
    "SessionLockError",
    "bar_slice_secs",
    "resolve_gateway_bin",
    "session_flock_held",
    "spawn_gateway",
    "window_slices",
]


def __getattr__(name: str) -> Any:
    if name in {"GatewayClient", "GatewayError"}:
        from rithmic_gateway.client import GatewayClient, GatewayError

        return GatewayClient if name == "GatewayClient" else GatewayError
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
