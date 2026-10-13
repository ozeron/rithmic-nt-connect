"""Stub for the compiled maturin extension (``rithmic_nt_connect._lib``).

The Rust PyO3 module ships as a ``.so`` with no type information. The surface
below mirrors the ``WireSession`` protocol (session.py) plus the typed errors
and helpers the adapter imports. Keep in sync with ``crates/rithmic-nt-connect``.
"""

from __future__ import annotations

from typing import Any

from rithmic_nt_connect.wire_types import AccountRmsInfo, ProductRmsInfo

class OrderPlantPolicy:
    """Order-plant lifecycle policy (rules live in the ``rithmic-exec`` crate).

    ``state`` is the lowercase state name: disconnected, connecting, live,
    resyncing or latched.
    """

    def __init__(self, state: str = "disconnected") -> None: ...
    @property
    def state(self) -> str: ...
    @property
    def latched(self) -> bool: ...
    def begin_connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def latch(self) -> None: ...
    def resync_start(self) -> None: ...
    def resync_complete(self) -> None: ...
    def resync_failed(self) -> None: ...
    def rearm(self, poll_alive: bool) -> bool: ...
    def allow_submit(self) -> bool: ...
    def allow_modify(self) -> bool: ...
    def allow_cancel(self) -> bool: ...
    def reject_reason(self, action: str) -> str: ...

class SessionLockError(RuntimeError):
    """Another live process holds the credential flock."""

class SessionLock:
    """Exclusive credential flock shared with the gateway parent (Rust)."""

    @staticmethod
    def try_acquire(
        user: str, system_name: str, url: str, env: str = "Live"
    ) -> SessionLock: ...
    @property
    def path(self) -> str: ...
    def close(self) -> None: ...

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

class GatewayError(RuntimeError):
    """Gateway-mode failure carrying the wire ``code`` and ``message``.

    ``str(exc)`` is ``"<code>: <message>"``. A request written but not answered
    has code ``outcome_unknown``; a transport failure has ``transport_reset``.
    """

    code: str
    message: str

class UnsupportedInGatewayModeError(RuntimeError):
    """Direct-only call (fill / order history replay) made in gateway mode."""

def list_systems(url: str | None = None) -> list[str]:
    """List Rithmic systems via the gateway (template 16); does not log in."""

class Session:
    """Rust plant session (ticker + history, PnL, and lazy order plants)."""

    def __init__(
        self,
        user: str,
        password: str | None = None,
        system_name: str = ...,
        url: str = ...,
        app_name: str = ...,
        app_version: str = ...,
        env: str = ...,
        account_id: str | None = ...,
        fcm_id: str | None = ...,
        ib_id: str | None = ...,
        beta_url: str | None = ...,
        plants: str = ...,
        connect_mode: str = ...,
        gateway_listen: str | None = ...,
        gateway_auth_token: str | None = ...,
    ) -> None:
        """``connect_mode`` is ``"direct"`` (needs ``password``) or ``"gateway"``
        (attach to a started parent; never takes a password).
        """

    # -- gateway-mode hooks (inert in direct mode) -------------------------
    @property
    def trading_enabled(self) -> bool | None:
        """The parent's trading gate (gateway); ``None`` in direct mode."""
    @property
    def cancel_all_enabled(self) -> bool | None:
        """The parent's cancel-all gate (gateway); ``None`` in direct mode."""
    def transport_generation(self) -> int: ...
    def transport_connected(self) -> bool: ...
    def ensure_transport_live(self) -> int: ...
    def subscribe_transport_events(self, callback: Any) -> None:
        """``callback(kind, generation, attempts, cause)``; ``kind`` is
        ``"down"``, ``"up"`` or ``"recovery_failed"``.
        """

    # -- lifecycle ---------------------------------------------------------
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def reset_ticker(self) -> None: ...
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
    def poll_history_event(self, timeout_ms: int = 0) -> dict[str, Any] | None: ...

    # -- pnl ---------------------------------------------------------------
    def subscribe_pnl(self) -> None: ...
    def disconnect_pnl_plant(self) -> None: ...
    def ensure_pnl_plant(self) -> None: ...
    def poll_pnl_event(self, timeout_ms: int = 0) -> dict[str, Any] | None: ...

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
    def load_product_rms_info(self) -> list[ProductRmsInfo]: ...
    def load_account_rms_info(self) -> list[AccountRmsInfo]: ...
    def load_fill_history(
        self, start_date: int, finish_date: int
    ) -> list[dict[str, Any]]: ...
    def load_order_history_dates(self) -> list[str]: ...
    def load_order_history(self, date: str) -> list[dict[str, Any]]: ...
    def poll_order_event(self, timeout_ms: int = 0) -> dict[str, Any] | None: ...

    # -- account resolution -------------------------------------------------
    def resolved_account(self) -> dict[str, Any] | None: ...
