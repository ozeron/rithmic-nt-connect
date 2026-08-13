"""Gateway system-name discovery (no login, no Node)."""

from __future__ import annotations

from rithmic_nt_connect.constants import DEFAULT_GATEWAY_URL


def list_systems(url: str | None = None) -> list[str]:
    """Return system names advertised by a Rithmic gateway.

    Opens a WebSocket and sends ``RequestRithmicSystemInfo`` (template 16).
    Does not log in. ``url`` defaults to the LucidTrading production gateway.
    Bare hosts like ``rituz00100.rithmic.com:443`` are prefixed with ``wss://``.
    """
    from rithmic_nt_connect._lib import list_systems as _list_systems

    return _list_systems(url or DEFAULT_GATEWAY_URL)


__all__ = ["list_systems"]
