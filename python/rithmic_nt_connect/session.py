"""Wire session protocols used by data/exec clients (Rust PyO3 or test doubles)."""

from __future__ import annotations

import threading
from typing import Any, Protocol

from rithmic_nt_connect.config import ConnectMode, SessionConfig
from rithmic_nt_connect.errors import AlreadyConnectedError


def _load_session_lock() -> Any:
    """Load ``SessionLock`` from the pure-Python gateway package.

    The ``rithmic_gateway`` gencode is generated with protoc 5.29.6 to match
    the ``nautilus_trader[ib]==1.231.0`` pin of ``protobuf==5.29.6``, so the
    wire client imports on that runtime (and any newer one). ``rithmic_gateway/__init__.py``
    imports ``GatewayClient`` lazily via ``__getattr__``, so importing
    ``SessionLock`` from ``flock`` does not trigger protobuf at all.
    """
    from rithmic_gateway.flock import SessionLock

    return SessionLock


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
    def subscribe_time_bars(self, symbol: str, exchange: str, bar_type: int, period: int) -> None: ...
    def unsubscribe_time_bars(self, symbol: str, exchange: str, bar_type: int, period: int) -> None: ...
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
    def load_orders(self, start_ssboe: int, end_ssboe: int) -> list[dict[str, Any]]: ...
    def poll_order_event(self) -> dict[str, Any] | None: ...


class WireSession(TickerSession, PnlSession, OrderSession, Protocol):
    """Full multi-plant session facade (composition of ticker / PnL / order)."""


PLANTS_MARKET_DATA = "market_data"
PLANTS_EXECUTION = "execution"


def create_rust_session(
    session: SessionConfig,
    *,
    plants: str = PLANTS_MARKET_DATA,
) -> WireSession:
    """Create the PyO3 session when the extension is built.

    Takes the shared credential flock before constructing plants so legacy
    callers cannot open a second Rithmic login alongside a gateway parent.

    ``plants`` is ``market_data`` (ticker + history) or ``execution``
    (also PnL when the account triple is set). Order plant stays lazy.
    """
    from rithmic_nt_connect._lib import Session

    SessionLock = _load_session_lock()

    lock = SessionLock.try_acquire(
        session.user, session.system_name, session.url, session.env
    )
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
        plants=plants,
    )
    # ``_FlockedDirectSession`` delegates the full facade via ``__getattr__``,
    # so a checker cannot see its protocol conformance; the Rust ``Session``
    # itself is verified against ``WireSession`` above (it is the ``inner``
    # argument's declared type).
    return _FlockedDirectSession(inner, lock)


class _FlockedDirectSession:
    """Keep the credential flock for the life of a direct plant session.

    Data and exec clients share one instance. ``connect()`` is idempotent
    because Nautilus starts both clients in parallel and each calls connect.
    The full ``WireSession`` surface is forwarded explicitly so checkers see
    the protocol conformance (a bare ``__getattr__`` delegate is invisible to
    them); ``__getattr__`` remains as a safety net for future additions.
    """

    def __init__(self, inner: WireSession, lock: Any) -> None:
        self._inner = inner
        self._lock = lock
        self._connect_gate = threading.Lock()

    def connect(self) -> None:
        ensure_connected(self)

    def disconnect(self) -> None:
        with self._connect_gate:
            self._inner.disconnect()

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

    def poll_event(self, timeout_ms: int = 0) -> dict[str, Any] | None:
        return self._inner.poll_event(timeout_ms)

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
        return self._inner.load_time_bars(symbol, exchange, start_ssboe, end_ssboe, bar_type, period)

    def probe_time_bars(
        self,
        symbol: str,
        exchange: str,
        start_ssboe: int,
        end_ssboe: int,
        bar_type: int = 2,
        period: int = 1,
    ) -> list[dict[str, Any]]:
        return self._inner.probe_time_bars(symbol, exchange, start_ssboe, end_ssboe, bar_type, period)

    def subscribe_time_bars(self, symbol: str, exchange: str, bar_type: int, period: int) -> None:
        self._inner.subscribe_time_bars(symbol, exchange, bar_type, period)

    def unsubscribe_time_bars(self, symbol: str, exchange: str, bar_type: int, period: int) -> None:
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

    def adjust_bracket_stop(self, basket_id: str, ticks: int, level: int | None = None) -> None:
        self._inner.adjust_bracket_stop(basket_id, ticks, level)

    def adjust_bracket_target(self, basket_id: str, ticks: int, level: int | None = None) -> None:
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
            basket_id, symbol, exchange, quantity, price_type, price, trigger_price, trail_by_ticks
        )

    def cancel_all_orders(self) -> None:
        self._inner.cancel_all_orders()

    def load_orders(self, start_ssboe: int, end_ssboe: int) -> list[dict[str, Any]]:
        return self._inner.load_orders(start_ssboe, end_ssboe)

    def poll_order_event(self) -> dict[str, Any] | None:
        return self._inner.poll_order_event()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def ensure_connected(session: Any) -> None:
    """Connect once. Safe if the shared plant is already up.

    There is no parallel ``_connected`` boolean: the inner plant session is the
    source of truth for connection state, and ``_connect_once`` swallows the
    typed ``AlreadyConnectedError`` it raises when already up. A stale parallel
    flag drifting True across a delegated ``disconnect()`` is what previously
    made reconnect short-circuit.
    """
    gate = getattr(session, "_connect_gate", None)
    if gate is None:
        _connect_once(session)
        return
    with gate:
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


def create_session(
    session: SessionConfig,
    *,
    plants: str = PLANTS_MARKET_DATA,
) -> WireSession:
    """Create a WireSession for ``session.connect_mode`` (``direct`` or ``gateway``).

    Direct takes the credential flock and opens PyO3 plants in-process.
    Gateway dials ``rithmic-gateway`` and never opens plants locally.
    """
    if session.connect_mode == ConnectMode.GATEWAY:
        from rithmic_nt_connect.gateway_wire import create_gateway_wire_session

        return create_gateway_wire_session(session)

    return create_rust_session(session, plants=plants)


def connect_market_data_session(
    config: SessionConfig | None = None,
) -> WireSession:
    """Create and connect a ticker+history session (no PnL / order plant)."""
    session = create_session(
        config if config is not None else SessionConfig.from_env(),
        plants=PLANTS_MARKET_DATA,
    )
    session.connect()
    return session


__all__ = [
    "OrderSession",
    "PLANTS_EXECUTION",
    "PLANTS_MARKET_DATA",
    "PnlSession",
    "TickerSession",
    "WireSession",
    "connect_market_data_session",
    "create_rust_session",
    "create_session",
    "ensure_connected",
]
