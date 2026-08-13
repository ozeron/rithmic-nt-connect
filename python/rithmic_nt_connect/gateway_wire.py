"""Adapt ``rithmic_gateway.GatewayClient`` to the adapter ``WireSession`` surface.

Plant-level dicts only — conversion to Nautilus types stays in data/execution clients.
"""

from __future__ import annotations

from typing import Any

from rithmic_gateway import GatewayClient, GatewayConfig, GatewayError


class GatewayWireSession:
    """WireSession-shaped façade over a gateway client (no in-process Rithmic plants)."""

    def __init__(self, client: GatewayClient) -> None:
        self._client = client
        self._connected = False

    def connect(self) -> None:
        self._client.connect()
        self._connected = True

    def disconnect(self) -> None:
        self._client.disconnect()
        self._connected = False

    def subscribe(self, symbol: str, exchange: str) -> None:
        self._client.subscribe(symbol, exchange)

    def unsubscribe(self, symbol: str, exchange: str) -> None:
        self._client.unsubscribe(symbol, exchange)

    def subscribe_order_book_summary(self, symbol: str, exchange: str) -> None:
        self._client.subscribe_order_book_summary(symbol, exchange)

    def get_front_month(self, symbol: str, exchange: str) -> Any:
        raise NotImplementedError("get_front_month over gateway RPC lands with full plant surface")

    def get_reference_data(self, symbol: str, exchange: str) -> Any:
        raise NotImplementedError(
            "get_reference_data over gateway RPC lands with full plant surface"
        )

    def poll_event(self, timeout_ms: int = 0) -> dict[str, Any] | None:
        return self._client.poll_event(timeout_ms=timeout_ms)

    def load_ticks(
        self, symbol: str, exchange: str, start_ssboe: int, end_ssboe: int
    ) -> list[dict[str, Any]]:
        raise NotImplementedError("load_ticks over gateway RPC lands with full plant surface")

    def load_time_bars(
        self,
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        bar_type: int = 2,
        period: int = 1,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError("load_time_bars over gateway RPC lands with full plant surface")

    def subscribe_time_bars(self, symbol: str, exchange: str, bar_type: int, period: int) -> None:
        raise NotImplementedError(
            "subscribe_time_bars over gateway RPC lands with full plant surface"
        )

    def unsubscribe_time_bars(
        self, symbol: str, exchange: str, bar_type: int, period: int
    ) -> None:
        raise NotImplementedError(
            "unsubscribe_time_bars over gateway RPC lands with full plant surface"
        )

    def poll_history_event(self) -> dict[str, Any] | None:
        return None

    def subscribe_pnl(self) -> None:
        pass

    def disconnect_pnl_plant(self) -> None:
        pass

    def ensure_pnl_plant(self) -> None:
        pass

    def poll_pnl_event(self) -> dict[str, Any] | None:
        return None

    def subscribe_order_updates(self) -> None:
        pass

    def disconnect_order_plant(self) -> None:
        pass

    def place_order(
        self,
        symbol: str,
        exchange: str,
        side: str,
        price_type: str,
        quantity: int,
        user_tag: str,
        price: float | None = None,
        trigger_price: float | None = None,
        duration: str = "DAY",
        trail_by_ticks: int | None = None,
        trail_by_price_id: int | None = None,
    ) -> None:
        try:
            self._client.place_order(
                symbol,
                exchange,
                side,
                price_type,
                quantity,
                user_tag=user_tag,
                price=price,
                trigger_price=trigger_price,
                duration=duration,
                trail_by_ticks=trail_by_ticks,
                trail_by_price_id=trail_by_price_id,
            )
        except GatewayError as exc:
            raise RuntimeError(str(exc)) from exc

    def cancel_order(self, basket_id: str) -> None:
        raise NotImplementedError("cancel_order over gateway RPC lands with full plant surface")

    def modify_order(
        self,
        basket_id: str,
        symbol: str,
        exchange: str,
        quantity: int,
        price_type: str,
        price: float | None = None,
        trigger_price: float | None = None,
        trail_by_ticks: int | None = None,
    ) -> None:
        raise NotImplementedError("modify_order over gateway RPC lands with full plant surface")

    def cancel_all_orders(self) -> None:
        try:
            self._client.cancel_all_orders()
        except GatewayError as exc:
            raise RuntimeError(str(exc)) from exc

    def poll_order_event(self) -> dict[str, Any] | None:
        return None

    def request_plants(self, plants: str) -> None:
        self._client.request_plants(plants)


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
        auto_spawn=bool(getattr(session, "gateway_auto_spawn", True)),
        gateway_bin=getattr(session, "gateway_bin", None),
    )


def create_gateway_wire_session(session: Any) -> GatewayWireSession:
    """Lazy-import safe constructor used by ``create_session``."""
    client = GatewayClient(gateway_config_from_session(session))
    return GatewayWireSession(client)
