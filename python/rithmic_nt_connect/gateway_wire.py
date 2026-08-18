"""Adapt ``rithmic_gateway.GatewayClient`` to the adapter ``WireSession`` surface.

Plant-level dicts only — conversion to Nautilus types stays in data/execution clients.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from rithmic_gateway import GatewayClient, GatewayConfig, GatewayError


def _call[T](fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Propagate gateway errors with ``code`` intact (timeouts must stay unknown)."""
    try:
        return fn(*args, **kwargs)
    except GatewayError:
        raise


class GatewayWireSession:
    """WireSession-shaped façade over a gateway client (no in-process
    Rithmic plants).
    """

    def __init__(self, client: GatewayClient) -> None:
        self._client = client

    @property
    def trading_enabled(self) -> bool:
        """Parent Ready.trading_enabled (read-only; clients cannot elevate)."""
        return self._client.trading_enabled

    @property
    def cancel_all_enabled(self) -> bool:
        return self._client.cancel_all_enabled

    def connect(self) -> None:
        _call(self._client.connect)

    def disconnect(self) -> None:
        _call(self._client.disconnect)

    def subscribe(self, symbol: str, exchange: str) -> None:
        _call(self._client.subscribe, symbol, exchange)

    def unsubscribe(self, symbol: str, exchange: str) -> None:
        _call(self._client.unsubscribe, symbol, exchange)

    def subscribe_order_book_summary(self, symbol: str, exchange: str) -> None:
        _call(self._client.subscribe_order_book_summary, symbol, exchange)

    def unsubscribe_order_book_summary(self, symbol: str, exchange: str) -> None:
        _call(self._client.unsubscribe_order_book_summary, symbol, exchange)

    def get_front_month(self, symbol: str, exchange: str) -> Any:
        return _call(self._client.get_front_month, symbol, exchange)

    def get_reference_data(self, symbol: str, exchange: str) -> Any:
        return _call(self._client.get_reference_data, symbol, exchange)

    def resolved_account(self) -> dict[str, Any] | None:
        return _call(self._client.resolved_account)

    def poll_event(self, timeout_ms: int = 0) -> dict[str, Any] | None:
        return _call(self._client.poll_event, timeout_ms=timeout_ms)

    def load_ticks(
        self, symbol: str, exchange: str, start_ssboe: int, end_ssboe: int
    ) -> list[dict[str, Any]]:
        return _call(self._client.load_ticks, symbol, exchange, start_ssboe, end_ssboe)

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
        return _call(
            self._client.load_time_bars_range,
            symbol,
            exchange,
            start_ssboe,
            end_ssboe,
            bar_type=bar_type,
            period=period,
            max_workers=1,
        )

    def probe_time_bars(
        self,
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        bar_type: int = 2,
        period: int = 1,
    ) -> list[dict[str, Any]]:
        return _call(
            self._client.probe_time_bars,
            symbol,
            exchange,
            start_ssboe,
            end_ssboe,
            bar_type=bar_type,
            period=period,
        )

    def subscribe_time_bars(
        self, symbol: str, exchange: str, bar_type: int, period: int
    ) -> None:
        _call(self._client.subscribe_time_bars, symbol, exchange, bar_type, period)

    def unsubscribe_time_bars(
        self, symbol: str, exchange: str, bar_type: int, period: int
    ) -> None:
        _call(self._client.unsubscribe_time_bars, symbol, exchange, bar_type, period)

    def poll_history_event(self) -> dict[str, Any] | None:
        return _call(self._client.poll_history_event, timeout_ms=0)

    def subscribe_pnl(self) -> None:
        _call(self._client.subscribe_pnl)

    def disconnect_pnl_plant(self) -> None:
        _call(self._client.disconnect_pnl_plant)

    def ensure_pnl_plant(self) -> None:
        _call(self._client.ensure_pnl_plant)

    def ensure_order_plant(self) -> None:
        _call(self._client.ensure_order_plant)

    def poll_pnl_event(self) -> dict[str, Any] | None:
        return _call(self._client.poll_pnl_event, timeout_ms=0)

    def subscribe_order_updates(self) -> None:
        _call(self._client.subscribe_order_updates)

    def load_orders(self, start_ssboe: int, end_ssboe: int) -> list[dict[str, Any]]:
        return _call(self._client.load_orders, start_ssboe, end_ssboe)

    def subscribe_bracket_updates(self) -> None:
        _call(self._client.subscribe_bracket_updates)

    def disconnect_order_plant(self) -> None:
        _call(self._client.disconnect_order_plant)

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
        _call(
            self._client.place_order,
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

    def place_bracket_order(
        self,
        symbol: str,
        exchange: str,
        side: str,
        price_type: str,
        quantity: int,
        localid: str,
        price: float | None = None,
        trigger_price: float | None = None,
        duration: str = "DAY",
        stop_ticks: int | None = None,
        target_ticks: int | None = None,
    ) -> None:
        _call(
            self._client.place_bracket_order,
            symbol,
            exchange,
            side,
            price_type,
            quantity,
            localid=localid,
            price=price,
            trigger_price=trigger_price,
            duration=duration,
            stop_ticks=stop_ticks,
            target_ticks=target_ticks,
        )

    def adjust_bracket_stop(
        self, basket_id: str, ticks: int, level: int | None = None
    ) -> None:
        _call(self._client.adjust_bracket_stop, basket_id, ticks, level)

    def adjust_bracket_target(
        self, basket_id: str, ticks: int, level: int | None = None
    ) -> None:
        _call(self._client.adjust_bracket_target, basket_id, ticks, level)

    def cancel_order(self, basket_id: str) -> None:
        _call(self._client.cancel_order, basket_id)

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
        _call(
            self._client.modify_order,
            basket_id,
            symbol,
            exchange,
            quantity,
            price_type,
            price=price,
            trigger_price=trigger_price,
            trail_by_ticks=trail_by_ticks,
        )

    def cancel_all_orders(self) -> None:
        _call(self._client.cancel_all_orders)

    def poll_order_event(self) -> dict[str, Any] | None:
        return _call(self._client.poll_order_event, timeout_ms=0)

    def request_plants(self, plants: str) -> None:
        _call(self._client.request_plants, plants)


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
