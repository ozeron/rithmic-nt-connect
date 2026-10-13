"""Fill / order history is direct-only until the gateway path is wired (docs/STATUS.md).

AGENTS.md direct <-> gateway parity: a direct-only plant surface is allowed only
while STATUS marks gateway deferred and no gateway-mode caller can reach it.
This pins both halves: the direct ``Session`` has the queries, and neither the
gateway client nor the ``WireSession`` protocol the book/exec use exposes them.
Wiring one side without the other fails here.
"""

from __future__ import annotations

from rithmic_gateway.client import GatewayClient
from rithmic_nt_connect._lib import Session
from rithmic_nt_connect.session import WireSession

HISTORY_QUERIES = (
    "load_fill_history",
    "load_order_history",
    "load_order_history_dates",
)


def test_direct_session_has_history_queries() -> None:
    for name in HISTORY_QUERIES:
        assert callable(getattr(Session, name, None)), name


def test_gateway_and_wire_protocol_do_not_expose_them_yet() -> None:
    for name in HISTORY_QUERIES:
        assert not hasattr(GatewayClient, name), (
            f"GatewayClient.{name} added: wire proto + dispatch + gateway_wire, "
            "then drop the deferral in docs/STATUS.md and this test"
        )
        assert not hasattr(WireSession, name), (
            f"WireSession.{name} requires the gateway path"
        )
