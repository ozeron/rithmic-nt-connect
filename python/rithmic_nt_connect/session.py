"""Wire session protocols used by data/exec clients (Rust PyO3 or test doubles)."""

from __future__ import annotations

import contextlib
import weakref
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, cast

from rithmic_nt_connect.config import ConfigError, ConnectMode, SessionConfig
from rithmic_nt_connect.errors import AlreadyConnectedError

if TYPE_CHECKING:
    from rithmic_nt_connect.transport import SessionEvent
    from rithmic_nt_connect.wire_types import AccountRmsInfo, ProductRmsInfo


class TickerSession(Protocol):
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def subscribe(self, symbol: str, exchange: str) -> None: ...
    def unsubscribe(self, symbol: str, exchange: str) -> None: ...
    def subscribe_order_book_summary(self, symbol: str, exchange: str) -> None: ...
    def unsubscribe_order_book_summary(self, symbol: str, exchange: str) -> None: ...
    def get_front_month(self, symbol: str, exchange: str) -> Any: ...
    def get_reference_data(self, symbol: str, exchange: str) -> Any: ...
    def poll_event(self, timeout_ms: int = 0) -> dict[str, Any] | None: ...
    def next_events(
        self, stream: str, max_events: int = 256, timeout_ms: int = 100
    ) -> list[dict[str, Any]]: ...
    def load_ticks(
        self, symbol: str, exchange: str, start_ssboe: int, end_ssboe: int, /
    ) -> list[dict[str, Any]]: ...
    def load_time_bars(
        self,
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        /,
        bar_type: int = 2,
        period: int = 1,
    ) -> list[dict[str, Any]]: ...
    def probe_time_bars(
        self,
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        /,
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
    def request_plants(self, plants: str) -> None: ...


class PnlSession(Protocol):
    def subscribe_pnl(self) -> None: ...
    def disconnect_pnl_plant(self) -> None: ...
    def ensure_pnl_plant(self) -> None: ...
    def poll_pnl_event(self) -> dict[str, Any] | None: ...


class OrderSession(Protocol):
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
    def load_orders(
        self, start_ssboe: int, end_ssboe: int, /
    ) -> list[dict[str, Any]]: ...
    def load_product_rms_info(self) -> list[ProductRmsInfo]: ...
    def load_account_rms_info(self) -> list[AccountRmsInfo]: ...
    def load_fill_history(
        self, start_date: int, finish_date: int
    ) -> list[dict[str, Any]]: ...
    def load_order_history_dates(self) -> list[str]: ...
    def load_order_history(self, date: str) -> list[dict[str, Any]]: ...
    def poll_order_event(self) -> dict[str, Any] | None: ...


class Gates(Protocol):
    """Read-only effective host gates returned by the D9 extension API."""

    @property
    def trading(self) -> bool: ...
    @property
    def cancel_all(self) -> bool: ...


class SessionEventSubscription(Protocol):
    def cancel(self) -> None: ...


class WireSession(TickerSession, PnlSession, OrderSession, Protocol):
    """Full multi-plant session facade, identical in both connection modes."""

    def resolved_account(self) -> dict[str, Any] | None: ...
    def open_gates(self, trading: bool, cancel_all: bool) -> Gates: ...
    def subscribe_session_events(
        self, listener: Callable[[SessionEvent], None]
    ) -> SessionEventSubscription: ...


PLANTS_MARKET_DATA = "market_data"
PLANTS_EXECUTION = "execution"


def create_rust_session(
    session: SessionConfig,
    *,
    plants: str = PLANTS_MARKET_DATA,
) -> WireSession:
    """Attach to the Rust registry's host; each caller owns its own intents."""
    if session.password is None:
        raise ConfigError("direct mode requires a password (RITHMIC_PASSWORD)")
    from rithmic_nt_connect._lib import Session

    inner = Session(
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
        connect_mode="direct",
        gateway_listen=session.gateway_listen,
    )
    if plants == PLANTS_EXECUTION:
        inner.request_plants(plants)
    return _FlockedDirectSession(inner)


class _FlockedDirectSession:
    """Forward the direct facade; Rust owns sharing, flock and detach."""

    def __init__(self, inner: WireSession) -> None:
        self._inner = inner

    def connect(self) -> None:
        ensure_connected(self)

    def disconnect(self) -> None:
        self._inner.disconnect()

    def open_gates(self, trading: bool, cancel_all: bool) -> Gates:
        return self._inner.open_gates(trading, cancel_all)

    def subscribe_session_events(
        self, listener: Callable[[SessionEvent], None]
    ) -> SessionEventSubscription:
        return self._inner.subscribe_session_events(listener)

    # -- TickerSession -----------------------------------------------------
    def subscribe(self, symbol: str, exchange: str) -> None:
        self._inner.subscribe(symbol, exchange)

    def unsubscribe(self, symbol: str, exchange: str) -> None:
        self._inner.unsubscribe(symbol, exchange)

    def subscribe_order_book_summary(self, symbol: str, exchange: str) -> None:
        self._inner.subscribe_order_book_summary(symbol, exchange)

    def unsubscribe_order_book_summary(self, symbol: str, exchange: str) -> None:
        self._inner.unsubscribe_order_book_summary(symbol, exchange)

    def get_front_month(self, symbol: str, exchange: str) -> Any:
        return self._inner.get_front_month(symbol, exchange)

    def get_reference_data(self, symbol: str, exchange: str) -> Any:
        return self._inner.get_reference_data(symbol, exchange)

    def resolved_account(self) -> dict[str, Any] | None:
        return self._inner.resolved_account()

    def poll_event(self, timeout_ms: int = 0) -> dict[str, Any] | None:
        return self._inner.poll_event(timeout_ms)

    def next_events(
        self, stream: str, max_events: int = 256, timeout_ms: int = 100
    ) -> list[dict[str, Any]]:
        return self._inner.next_events(stream, max_events, timeout_ms)

    def load_ticks(
        self, symbol: str, exchange: str, start_ssboe: int, end_ssboe: int
    ) -> list[dict[str, Any]]:
        return self._inner.load_ticks(symbol, exchange, start_ssboe, end_ssboe)

    def load_time_bars(
        self,
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        bar_type: int = 2,
        period: int = 1,
    ) -> list[dict[str, Any]]:
        return self._inner.load_time_bars(
            symbol, exchange, start_ssboe, end_ssboe, bar_type, period
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
        return self._inner.probe_time_bars(
            symbol, exchange, start_ssboe, end_ssboe, bar_type, period
        )

    def subscribe_time_bars(
        self, symbol: str, exchange: str, bar_type: int, period: int
    ) -> None:
        self._inner.subscribe_time_bars(symbol, exchange, bar_type, period)

    def unsubscribe_time_bars(
        self, symbol: str, exchange: str, bar_type: int, period: int
    ) -> None:
        self._inner.unsubscribe_time_bars(symbol, exchange, bar_type, period)

    def poll_history_event(self) -> dict[str, Any] | None:
        return self._inner.poll_history_event()

    def request_plants(self, plants: str) -> None:
        self._inner.request_plants(plants)

    # -- PnlSession --------------------------------------------------------
    def subscribe_pnl(self) -> None:
        self._inner.subscribe_pnl()

    def disconnect_pnl_plant(self) -> None:
        self._inner.disconnect_pnl_plant()

    def ensure_pnl_plant(self) -> None:
        self._inner.ensure_pnl_plant()

    def poll_pnl_event(self) -> dict[str, Any] | None:
        return self._inner.poll_pnl_event()

    # -- OrderSession ------------------------------------------------------
    def subscribe_order_updates(self) -> None:
        self._inner.subscribe_order_updates()

    def subscribe_bracket_updates(self) -> None:
        self._inner.subscribe_bracket_updates()

    def disconnect_order_plant(self) -> None:
        self._inner.disconnect_order_plant()

    def ensure_order_plant(self) -> None:
        self._inner.ensure_order_plant()

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
        self._inner.place_order(
            symbol,
            exchange,
            side,
            price_type,
            quantity,
            user_tag,
            price,
            trigger_price,
            duration,
            trail_by_ticks,
            trail_by_price_id,
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
        self._inner.place_bracket_order(
            symbol,
            exchange,
            side,
            price_type,
            quantity,
            localid,
            price,
            trigger_price,
            duration,
            stop_ticks,
            target_ticks,
        )

    def adjust_bracket_stop(
        self, basket_id: str, ticks: int, level: int | None = None
    ) -> None:
        self._inner.adjust_bracket_stop(basket_id, ticks, level)

    def adjust_bracket_target(
        self, basket_id: str, ticks: int, level: int | None = None
    ) -> None:
        self._inner.adjust_bracket_target(basket_id, ticks, level)

    def cancel_order(self, basket_id: str) -> None:
        self._inner.cancel_order(basket_id)

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
        self._inner.modify_order(
            basket_id,
            symbol,
            exchange,
            quantity,
            price_type,
            price,
            trigger_price,
            trail_by_ticks,
        )

    def cancel_all_orders(self) -> None:
        self._inner.cancel_all_orders()

    def load_orders(self, start_ssboe: int, end_ssboe: int) -> list[dict[str, Any]]:
        return self._inner.load_orders(start_ssboe, end_ssboe)

    def load_product_rms_info(self) -> list[ProductRmsInfo]:
        return self._inner.load_product_rms_info()

    def load_account_rms_info(self) -> list[AccountRmsInfo]:
        return self._inner.load_account_rms_info()

    def load_fill_history(
        self, start_date: int, finish_date: int
    ) -> list[dict[str, Any]]:
        return self._inner.load_fill_history(start_date, finish_date)

    def load_order_history_dates(self) -> list[str]:
        return self._inner.load_order_history_dates()

    def load_order_history(self, date: str) -> list[dict[str, Any]]:
        return self._inner.load_order_history(date)

    def poll_order_event(self) -> dict[str, Any] | None:
        return self._inner.poll_order_event()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def ensure_connected(session: Any) -> None:
    """Ensure the host is connected; concurrency is serialized in Rust."""
    _connect_once(session)


def _connect_once(session: Any) -> None:
    inner = getattr(session, "_inner", session)
    try:
        inner.connect()
    except AlreadyConnectedError:
        # The plant session is genuinely already connected (Rust raises this
        # type only from that exact state); a partial connect failure raises
        # any other error and must not be swallowed.
        return


class _RustGatewaySession:
    """The Rust gateway client (one shared socket per process) as a ``WireSession``.

    A thin shell over the extension's ``Session``: every call is the extension's.
    It fans one extension session-event registration out to clients' listeners so
    a client can detach; the extension registration is cancelled when the last
    listener leaves and on disconnect.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._listeners: list[Callable[[SessionEvent], None]] = []
        self._subscription: Any = None

    def __getattr__(self, name: str) -> Any:
        # Private names are never the extension's surface; probes must see them absent.
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._inner, name)

    def open_gates(self, trading: bool, cancel_all: bool) -> Gates:
        return self._inner.open_gates(trading, cancel_all)

    def _fan_out(self, event: SessionEvent) -> None:
        for listener in tuple(self._listeners):
            listener(event)

    def subscribe_session_events(
        self, listener: Callable[[SessionEvent], None]
    ) -> SessionEventSubscription:
        if self._subscription is None and not self._listeners:
            ref = weakref.ref(self)

            def relay(event: SessionEvent) -> None:
                target = ref()
                if target is not None:
                    target._fan_out(event)

            self._subscription = self._inner.subscribe_session_events(relay)
        self._listeners.append(listener)
        ref = weakref.ref(self)

        class _Subscription:
            def cancel(self) -> None:
                target = ref()
                if target is None:
                    return
                with contextlib.suppress(ValueError):
                    target._listeners.remove(listener)
                if not target._listeners:
                    target._cancel_subscription()

        return _Subscription()

    def _cancel_subscription(self) -> None:
        subscription, self._subscription = self._subscription, None
        if subscription is not None:
            subscription.cancel()

    def disconnect(self) -> None:
        self._cancel_subscription()
        self._inner.disconnect()


def create_rust_gateway_session(session: SessionConfig) -> WireSession:
    """A gateway-mode session backed by the Rust client.

    The client holds no password: only the parent (started explicitly) has it.
    """
    from rithmic_nt_connect._lib import Session

    inner = Session(
        user=session.user,
        system_name=session.system_name,
        url=session.url,
        env=session.env,
        account_id=session.account_id,
        fcm_id=session.fcm_id,
        ib_id=session.ib_id,
        connect_mode="gateway",
        gateway_listen=session.gateway_listen,
        gateway_auth_token=session.gateway_auth_token or None,
    )
    return cast(WireSession, _RustGatewaySession(inner))


def create_session(
    session: SessionConfig,
    *,
    plants: str = PLANTS_MARKET_DATA,
) -> WireSession:
    """Create a WireSession for ``session.connect_mode`` (``direct`` or ``gateway``).

    Direct attachments share one Rust host per credential fingerprint.
    Gateway attaches to an explicitly started ``rithmic-gateway`` parent (it
    never spawns one, never holds the password) and never opens plants locally.
    Each call returns a **fresh** facade; data and exec factories share one
    gateway socket.
    """
    if session.connect_mode == ConnectMode.GATEWAY:
        return create_rust_gateway_session(session)
    return create_rust_session(session, plants=plants)


def connect_market_data_session(
    config: SessionConfig | None = None,
) -> WireSession:
    """Create and connect a ticker+history session.

    The new attachment shares the node's Rust host and login. Connect is idempotent.
    """
    session = create_session(
        config if config is not None else SessionConfig.from_env(),
        plants=PLANTS_MARKET_DATA,
    )
    session.connect()
    return session


__all__ = [
    "PLANTS_EXECUTION",
    "PLANTS_MARKET_DATA",
    "OrderSession",
    "PnlSession",
    "TickerSession",
    "WireSession",
    "connect_market_data_session",
    "create_rust_session",
    "create_session",
    "ensure_connected",
]
