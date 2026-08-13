"""Wire session protocols used by data/exec clients (Rust PyO3 or test doubles)."""

from __future__ import annotations

from typing import Any, Protocol

from rithmic_connect.config import SessionConfig


class TickerSession(Protocol):
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def subscribe(self, symbol: str, exchange: str) -> None: ...
    def unsubscribe(self, symbol: str, exchange: str) -> None: ...
    def subscribe_order_book_summary(self, symbol: str, exchange: str) -> None: ...
    def get_front_month(self, symbol: str, exchange: str) -> Any: ...
    def get_reference_data(self, symbol: str, exchange: str) -> Any: ...
    def poll_event(self, timeout_ms: int = 0) -> dict[str, Any] | None: ...
    def load_ticks(
        self, symbol: str, exchange: str, start_ssboe: int, end_ssboe: int
    ) -> list[dict[str, Any]]: ...
    def load_time_bars(
        self,
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        bar_type: int = 2,
        period: int = 1,
    ) -> list[dict[str, Any]]: ...


class PnlSession(Protocol):
    def subscribe_pnl(self) -> None: ...
    def disconnect_pnl_plant(self) -> None: ...
    def ensure_pnl_plant(self) -> None: ...
    def poll_pnl_event(self) -> dict[str, Any] | None: ...


class OrderSession(Protocol):
    def subscribe_order_updates(self) -> None: ...
    def disconnect_order_plant(self) -> None: ...
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
    ) -> None: ...
    def cancel_order(self, basket_id: str) -> None: ...
    def modify_order(
        self,
        basket_id: str,
        symbol: str,
        exchange: str,
        quantity: int,
        price_type: str,
        price: float | None = None,
        trigger_price: float | None = None,
    ) -> None: ...
    def cancel_all_orders(self) -> None: ...
    def poll_order_event(self) -> dict[str, Any] | None: ...


class WireSession(TickerSession, PnlSession, OrderSession, Protocol):
    """Full multi-plant session facade (composition of ticker / PnL / order)."""


def create_rust_session(session: SessionConfig) -> WireSession:
    """Create the PyO3 session when the extension is built."""
    from rithmic_connect._lib import Session  # type: ignore

    return Session(
        user=session.user,
        password=session.password,
        system_name=session.system_name,
        url=session.url,
        app_name=session.app_name,
        app_version=session.app_version,
        env=session.env,
        account_id=session.account_id,
        fcm_id=session.fcm_id,
        ib_id=session.ib_id,
        beta_url=session.beta_url,
    )


__all__ = [
    "OrderSession",
    "PnlSession",
    "TickerSession",
    "WireSession",
    "create_rust_session",
]
