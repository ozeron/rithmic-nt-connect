"""Wire session protocols used by data/exec clients (Rust PyO3 or test doubles)."""

from __future__ import annotations

import contextlib
import threading
from typing import TYPE_CHECKING, Any, Protocol, cast

from rithmic_nt_connect.config import ConnectMode, SessionConfig
from rithmic_nt_connect.errors import AlreadyConnectedError

if TYPE_CHECKING:
    from rithmic_gateway.types import AccountRmsInfo, ProductRmsInfo


def _load_session_lock() -> Any:
    """Load ``SessionLock`` from the pure-Python gateway package.

    The ``rithmic_gateway`` gencode is generated with protoc 5.29.6 to match
    the ``nautilus_trader[ib]==1.231.0`` pin of ``protobuf==5.29.6``, so the
    wire client imports on that runtime (and any newer one).
    ``rithmic_gateway/__init__.py`` imports ``GatewayClient`` lazily via
    ``__getattr__``, so importing
    ``SessionLock`` from ``flock`` does not trigger protobuf at all.
    """
    from rithmic_gateway.flock import SessionLock

    return SessionLock


class TickerSession(Protocol):
    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def reset_ticker(self) -> None: ...
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
    def load_product_rms_info(self) -> list[ProductRmsInfo]: ...
    def load_account_rms_info(self) -> list[AccountRmsInfo]: ...
    def poll_order_event(self) -> dict[str, Any] | None: ...


class WireSession(TickerSession, PnlSession, OrderSession, Protocol):
    """Full multi-plant session facade (composition of ticker / PnL / order)."""

    def resolved_account(self) -> dict[str, Any] | None: ...


PLANTS_MARKET_DATA = "market_data"
PLANTS_EXECUTION = "execution"

# Process-wide singleton for DIRECT sessions: one in-process Rithmic login per
# credential fingerprint, whichever entry point asks first (data factory, exec
# factory, or ``connect_market_data_session``). Gateway is deliberately NOT
# cached — each Nautilus client gets its own ``GatewayClient`` and the parent
# ``rithmic-gateway`` holds the single login. Values are always
# ``_FlockedDirectSession`` (which owns ``acquire`` / the holder refcount);
# ``create_rust_session`` casts on the way out.
_SESSION_CACHE: dict[str, WireSession] = {}


def _session_cache_key(session: SessionConfig) -> str:
    """Credential fingerprint for the direct-session singleton.

    Matches the flock identity (``user`` / ``system_name`` / ``url`` / ``env``)
    plus the account triple and endpoint variants (``beta_url``, ``app_name``,
    ``app_version``), so a Live node and a Demo history session can never
    share, while data/exec factories built from the same env always do.
    Password is deliberately absent: it is not part of the login identity and
    must never appear in keys / reprs / logs.
    """
    return (
        f"{session.connect_mode}:{session.user}:{session.system_name}:{session.url}:"
        f"{session.env}:{session.account_id}:{session.fcm_id}:{session.ib_id}:"
        f"{session.gateway_listen}:{session.beta_url}:{session.app_name}:{session.app_version}"
    )


def create_rust_session(
    session: SessionConfig,
    *,
    plants: str = PLANTS_MARKET_DATA,
) -> WireSession:
    """Create (or reuse) the in-process PyO3 session for ``session``.

    Direct mode is a process-wide singleton keyed by credential fingerprint
    (``_SESSION_CACHE``): the data factory, exec factory, and
    ``connect_market_data_session`` all share one Rithmic login, so
    initializing both clients cannot open two logins that close each other.
    Every hand-out takes a holder (``_FlockedDirectSession.acquire``) and
    ``disconnect`` tears plants down only when the last holder leaves, so one
    client's teardown cannot close a shared session out from under the other.

    Takes the shared credential flock before constructing plants so legacy
    callers cannot open a second Rithmic login alongside a gateway parent.

    ``plants`` is ``market_data`` (ticker + history) or ``execution``
    (also PnL when the account triple is set). Order plant stays lazy. A later
    ``execution`` request on a cached ``market_data`` session unions the PnL
    plant into the set (``request_plants`` attaches it even when already
    connected).
    """
    key = _session_cache_key(session)
    existing = cast(_FlockedDirectSession | None, _SESSION_CACHE.get(key))
    if existing is not None:
        if plants == PLANTS_EXECUTION:
            existing.request_plants(PLANTS_EXECUTION)
        existing.acquire()
        return existing

    from rithmic_nt_connect._lib import Session

    session_lock = _load_session_lock()

    lock = None
    try:
        lock = session_lock.try_acquire(
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
        wrapped = _FlockedDirectSession(inner, lock, cache_key=key)
        _SESSION_CACHE[key] = wrapped
        return wrapped
    except Exception:
        if lock is not None:
            with contextlib.suppress(Exception):
                close = getattr(lock, "close", None)
                if callable(close):
                    close()
        raise


class _FlockedDirectSession:
    """Keep the credential flock for the life of a direct plant session.

    Data and exec clients share one instance. ``connect()`` is idempotent
    because Nautilus starts both clients in parallel and each calls connect.
    The full ``WireSession`` surface is forwarded explicitly so checkers see
    the protocol conformance (a bare ``__getattr__`` delegate is invisible to
    them); ``__getattr__`` remains as a safety net for future additions.
    """

    def __init__(
        self,
        inner: WireSession,
        lock: Any,
        cache_key: str | None = None,
    ) -> None:
        self._inner = inner
        self._lock = lock
        self._cache_key = cache_key
        self._connect_gate = threading.Lock()
        # Holder refcount: the creator holds one; every additional factory /
        # ``connect_market_data_session`` hand-out takes another via
        # ``acquire``. ``disconnect`` tears the inner plants down only when the
        # last holder leaves, so a data-client teardown cannot close a shared
        # session that an exec client (or standalone history load) still uses.
        self._holders = 1

    def acquire(self) -> None:
        """Take another holder on the shared session (factory hand-out)."""
        with self._connect_gate:
            self._holders += 1

    def connect(self) -> None:
        ensure_connected(self)

    def disconnect(self) -> None:
        """Release this holder; tear plants down only when the last one leaves.

        The last holder also releases the credential flock and evicts the
        session from the process singleton, so a stopped node no longer blocks
        a separate process from taking the login (the flock's lifetime is tied
        to the owners, not to GC).
        """
        with self._connect_gate:
            if self._holders <= 0:
                return
            self._holders -= 1
            if self._holders == 0:
                self._inner.disconnect()
                self._release_credential_flock()

    def _release_credential_flock(self) -> None:
        if self._cache_key is not None and _SESSION_CACHE.get(self._cache_key) is self:
            _SESSION_CACHE.pop(self._cache_key, None)
        close = getattr(self._lock, "close", None)
        if callable(close):
            close()

    def reset_ticker(self) -> None:
        """Recreate ONLY the ticker plant, refcount-blind.

        Used by the data client's channel-error resync, which must actually
        recreate the ticker plant even while other holders are live (a
        refcounted ``disconnect`` would be a no-op and the broken stream would
        never recover). Sibling plants (history/PnL/order) stay untouched, so
        the exec client sharing this session is never disturbed. The re-issued
        subscription intent follows in ``resync_ticker_session``.
        """
        with self._connect_gate:
            self._inner.reset_ticker()

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
        # The direct PyO3 ``Session.poll_event`` is a non-blocking ``try_recv``
        # poll and takes no timeout argument (the gateway client's blocking
        # ``_poll_filtered`` accepts ``timeout_ms``); a ``0`` forward would
        # raise ``TypeError`` on the direct path. The facade keeps the shared
        # signature but ignores the arg on the direct path.
        del timeout_ms
        return self._inner.poll_event()

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

    Direct is a process-wide singleton per credential fingerprint (see
    ``create_rust_session``): the data factory, exec factory, and
    ``connect_market_data_session`` share one in-process Rithmic login.
    Gateway dials ``rithmic-gateway`` and never opens plants locally; each
    call returns a **fresh** ``GatewayClient`` — the parent owns the single
    login, and sharing one client would interleave tick and order polls.
    """
    if session.connect_mode == ConnectMode.GATEWAY:
        from rithmic_nt_connect.gateway_wire import create_gateway_wire_session

        return create_gateway_wire_session(session)

    return create_rust_session(session, plants=plants)


def connect_market_data_session(
    config: SessionConfig | None = None,
) -> WireSession:
    """Create and connect a ticker+history session.

    Direct mode returns the process-wide singleton for these credentials (so a
    standalone history load shares the node's login instead of opening a second
    one that would close it); the session may already carry the PnL plant when
    an exec client requested it first. Connect is idempotent.
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
