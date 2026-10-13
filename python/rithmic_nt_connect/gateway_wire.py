"""Adapt ``rithmic_gateway.GatewayClient`` to the adapter ``WireSession`` surface.

Plant-level dicts only — conversion to Nautilus types stays in data/execution clients.
"""

from __future__ import annotations

from typing import Any

from rithmic_gateway import GatewayClient, GatewayConfig
from rithmic_gateway.client import GatewayError
from rithmic_gateway.runtime import create_gateway_client


class GatewayWireSession:
    """``WireSession`` façade over a gateway client (no in-process plants).

    Only behaviour that differs from the client lives here; everything else is
    delegated to it. A type checker cannot see delegated members, so
    ``tests/test_wire_session_parity.py`` is what proves this façade still
    exposes the whole ``WireSession`` surface (and ``TransportAware``).
    """

    def __init__(self, client: GatewayClient) -> None:
        self._client = client

    def __getattr__(self, name: str) -> Any:
        # Private names are never delegated: probes such as
        # ``getattr(session, "_inner", session)`` must see "absent", not the
        # client's internals.
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._client, name)

    def reset_ticker(self) -> None:
        """Prefer plant RPC; dual mode falls back to detach + re-dial."""
        try:
            self._client.reset_ticker_plant()
        except GatewayError:
            if getattr(self._client, "shared_runtime", False):
                raise
            self._client.disconnect()
            self._client.connect()

    def load_time_bars(
        self,
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        bar_type: int = 2,
        period: int = 1,
    ) -> list[dict[str, Any]]:
        # A wide 1m window cannot fit one server-chunked RPC (the parent holds a
        # session mutex while slicing internally — minutes for months of bars).
        # Chunk client-side like the lake ingest path (calendar slices, 120s
        # history timeout each) and merge.
        return self._client.load_time_bars_range(
            symbol,
            exchange,
            start_ssboe,
            end_ssboe,
            bar_type=bar_type,
            period=period,
            max_workers=1,
        )

    def subscribe_transport_events(self, listener: Any) -> Any:
        fn = getattr(self._client, "subscribe_transport_events", None)
        if fn is None:
            return lambda: None
        return fn(listener)

    def ensure_transport_live(self) -> int:
        fn = getattr(self._client, "ensure_transport_live", None)
        if fn is not None:
            return int(fn())
        fn2 = getattr(self._client, "reconnect_transport", None)
        if fn2 is not None:
            fn2()
        return int(getattr(self._client, "transport_generation", 0) or 0)

    def reconnect_transport(self) -> None:
        """Demand L3 recovery via runtime (mux); dual clients re-dial."""
        fn = getattr(self._client, "reconnect_transport", None)
        if fn is None:
            raise GatewayError(
                "not_supported",
                "reconnect_transport requires mux gateway client",
            )
        fn()


def gateway_config_from_session(session: Any) -> GatewayConfig:
    """Build ``GatewayConfig`` fingerprint from adapter ``SessionConfig``."""
    return GatewayConfig(
        user=session.user,
        system_name=session.system_name,
        url=session.url,
        env=session.env,
        account_id=session.account_id or "",
        fcm_id=session.fcm_id or "",
        ib_id=session.ib_id or "",
        auth_token=getattr(session, "gateway_auth_token", None) or "",
        listen=getattr(session, "gateway_listen", None),
        spawn_policy=getattr(session, "gateway_spawn_policy", None),
        auto_spawn=bool(getattr(session, "gateway_auto_spawn", True)),
        gateway_bin=getattr(session, "gateway_bin", None),
    )


def create_gateway_wire_session(session: Any) -> GatewayWireSession:
    """Lazy-import safe constructor used by ``create_session``."""
    config = gateway_config_from_session(session)
    mode = getattr(session, "gateway_client_mode", None)
    client = create_gateway_client(config, mode=mode)
    return GatewayWireSession(client)
