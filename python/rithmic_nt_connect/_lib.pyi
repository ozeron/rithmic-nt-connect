"""Stub for the compiled maturin extension (``rithmic_nt_connect._lib``).

The Rust PyO3 module ships as a ``.so`` with no type information. The surface
below mirrors the ``WireSession`` protocol (session.py) plus the typed errors
and helpers the adapter imports. Keep in sync with ``crates/rithmic-nt-connect``.
"""

from __future__ import annotations

from typing import Any


# NOTE: the Rust exceptions (pyo3 ``create_exception!``) all subclass
# PyRuntimeError directly. The hierarchy below deliberately mirrors the
# ``errors.py`` fallback classes (``ChannelLaggedError(ChannelError)`` etc.) so
# the two branches of the try/except re-export stay structurally identical to
# the checker.
class ChannelError(RuntimeError):
    """Base for plant channel failures that require resync."""


class AlreadyConnectedError(RuntimeError):
    """Session already connected (idempotent connect is safe to suppress)."""


class ChannelLaggedError(ChannelError):
    """Broadcast receiver lagged; messages were skipped."""


class ChannelClosedError(ChannelError):
    """Plant subscription channel closed."""


class NotConnectedError(ChannelError):
    """Required plant is not connected."""


class ReconciliationUnavailableError(RuntimeError):
    """Order-history reconciliation cannot be answered authoritatively."""


def list_systems(url: str | None = None) -> list[str]:
    """List Rithmic systems via the gateway (template 16); does not log in."""


class Session:
    """Rust plant session (ticker + history, PnL, and lazy order plants)."""

    def __init__(
        self,
        user: str,
        password: str,
        system_name: str,
        url: str,
        app_name: str,
        app_version: str,
        env: str,
        account_id: str | None,
        fcm_id: str | None,
        ib_id: str | None,
        beta_url: str | None,
        plants: str,
    ) -> None: ...

    # -- lifecycle ---------------------------------------------------------
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def request_plants(self, plants: str) -> None: ...

    # -- ticker ------------------------------------------------------------
    def subscribe(self, symbol: str, exchange: str) -> None: ...
    def unsubscribe(self, symbol: str, exchange: str) -> None: ...
    def subscribe_order_book_summary(self, symbol: str, exchange: str) -> None: ...
    def unsubscribe_order_book_summary(self, symbol: str, exchange: str) -> None: ...
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
    def probe_time_bars(
        self,
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        bar_type: int = 2,
        period: int = 1,
    ) -> list[dict[str, Any]]: ...
    def subscribe_time_bars(
        self, symbol: str, exchange: str, bar_type: int, period: int
    ) -> None: ...
    def unsubscribe_time_bars(
        self, symbol: str, exchange: str, bar_type: int, period: int
    ) -> None: ...
    def poll_history_event(self) -> dict[str, Any] | None: ...

    # -- pnl ---------------------------------------------------------------
    def subscribe_pnl(self) -> None: ...
    def disconnect_pnl_plant(self) -> None: ...
    def ensure_pnl_plant(self) -> None: ...
    def poll_pnl_event(self) -> dict[str, Any] | None: ...

    # -- order plant -------------------------------------------------------
    def subscribe_order_updates(self) -> None: ...
    def subscribe_bracket_updates(self) -> None: ...
    def disconnect_order_plant(self) -> None: ...
    def ensure_order_plant(self) -> None: ...
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
    ) -> None: ...
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
    ) -> None: ...
    def adjust_bracket_stop(
        self, basket_id: str, ticks: int, level: int | None = None
    ) -> None: ...
    def adjust_bracket_target(
        self, basket_id: str, ticks: int, level: int | None = None
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
        trail_by_ticks: int | None = None,
    ) -> None: ...
    def cancel_all_orders(self) -> None: ...
    def load_orders(self, start_ssboe: int, end_ssboe: int) -> list[dict[str, Any]]: ...
    def poll_order_event(self) -> dict[str, Any] | None: ...

    # -- account resolution -------------------------------------------------
    def resolved_account(self) -> dict[str, Any] | None: ...
