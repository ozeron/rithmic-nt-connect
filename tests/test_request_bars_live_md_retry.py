"""RequestBars RC2.3 retry helpers."""

from __future__ import annotations

from rithmic_gateway.client import GatewayError
from rithmic_nt_connect.data import (
    _is_live_md_history_denied,
    request_bars_error,
)


def test_is_live_md_history_denied_codes() -> None:
    assert _is_live_md_history_denied(
        GatewayError("capability_denied", "history Load* refused")
    )
    assert _is_live_md_history_denied(GatewayError("history_denied_live_md", "refused"))
    assert not _is_live_md_history_denied(GatewayError("timeout", "slow"))


def test_request_bars_error_is_per_request() -> None:
    import rithmic_nt_connect.data as data

    data._note_request_bars_error("req-a", "hydrate_blocked_live_md")
    data._note_request_bars_error("req-b", "history_error")
    assert request_bars_error("req-b") == "history_error"
    assert request_bars_error("req-a") == "hydrate_blocked_live_md"
    assert request_bars_error("req-a") is None
