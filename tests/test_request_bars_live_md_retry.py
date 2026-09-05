"""RequestBars RC2.3 retry helpers."""

from __future__ import annotations

from rithmic_gateway.client import GatewayError
from rithmic_nt_connect.data import (
    _is_live_md_history_denied,
    take_last_request_bars_error,
)


def test_is_live_md_history_denied_codes() -> None:
    assert _is_live_md_history_denied(
        GatewayError("capability_denied", "history Load* refused")
    )
    assert _is_live_md_history_denied(GatewayError("history_denied_live_md", "refused"))
    assert not _is_live_md_history_denied(GatewayError("timeout", "slow"))


def test_take_last_request_bars_error_roundtrip() -> None:
    import rithmic_nt_connect.data as data

    data._last_request_bars_error = "hydrate_blocked_live_md"
    assert take_last_request_bars_error() == "hydrate_blocked_live_md"
    assert take_last_request_bars_error() is None
