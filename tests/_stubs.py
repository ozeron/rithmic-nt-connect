"""Shared test doubles.

``WireSessionStub`` implements the full ``WireSession`` protocol so unit-test
fakes are structurally conformant (a checker cannot see partial duck-typed
doubles). Members are deliberately permissive (``*args``/``**kwargs``): the
stub's job is conformance, not signature enforcement — subclasses override with
their own signatures and the unused members raise loudly instead of passing
silently.
"""

from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, cast

from nautilus_trader.model.identifiers import (
    AccountId,
    ClientOrderId,
    InstrumentId,
    VenueOrderId,
)
from rithmic_gateway.types import AccountRmsInfo, ProductRmsInfo
from rithmic_nt_connect.execution import RithmicExecutionClient
from rithmic_nt_connect.session import WireSession


class WireSessionStub:
    """Base class for ``WireSession`` test doubles.

    Every protocol member raises ``NotImplementedError`` unless overridden, so a
    double is honest about which members it provides and tests fail loudly if
    the code under test reaches one it does not stub.
    """

    # Shared-session internals that ``ensure_connected`` / ``_connect_once``
    # read via ``getattr``: absence means "no flock gate / no inner session".
    # (``_inner`` is deliberately NOT defaulted here: ``_connect_once`` treats
    # a missing ``_inner`` as "the session itself".)
    _connect_gate = None
    # Parent-gateway trading gate read via ``getattr(..., default=True)``.
    trading_enabled = True

    def __getattr__(self, name: str) -> Any:
        # Catch-all for members added to ``WireSession`` in the future: fail
        # loudly rather than silently return a bogus value.
        raise NotImplementedError(f"WireSessionStub has no member {name!r}")

    def connect(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def disconnect(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def reset_ticker(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def subscribe(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def unsubscribe(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def subscribe_order_book_summary(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def unsubscribe_order_book_summary(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def get_front_month(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def get_reference_data(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def resolved_account(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def poll_event(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def load_ticks(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def load_time_bars(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def probe_time_bars(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def subscribe_time_bars(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def unsubscribe_time_bars(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def poll_history_event(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def request_plants(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def subscribe_pnl(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def disconnect_pnl_plant(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def ensure_pnl_plant(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def poll_pnl_event(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def subscribe_order_updates(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def subscribe_bracket_updates(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def disconnect_order_plant(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def ensure_order_plant(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def place_order(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def place_bracket_order(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def adjust_bracket_stop(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def adjust_bracket_target(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def cancel_order(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def modify_order(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def cancel_all_orders(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def load_orders(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def load_product_rms_info(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def load_account_rms_info(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def poll_order_event(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError


class FaultInjectingSession(WireSessionStub):
    """Wire-session double that can cut the channel between a state-changing
    send and its venue result (transport e2e).

    ``fault`` names the operation that fails *after* "send": the raise happens
    inside the session call, so the adapter sees a transport failure with an
    unknown venue outcome (never a venue reject). ``load_orders`` returns
    ``working_orders`` (normalized wire rows) so recovery drains can resolve
    in-flight orders by ``user_tag``.
    """

    def __init__(
        self,
        *,
        working_orders: list[dict[str, object]] | None = None,
        fault: str | None = None,
        on_load_orders: Callable[[], None] | None = None,
        product_rms_rows: list[ProductRmsInfo] | None = None,
        account_rms_rows: list[AccountRmsInfo] | None = None,
        account_rms_fault: bool = False,
    ) -> None:
        self.working_orders = list(working_orders or [])
        self.fault = fault
        self.calls: list[str] = []
        self._notifications: list[dict[str, object]] = []
        self._inner = self  # ``_connect_once`` treats a plain session as itself
        # Optional hook run inside ``load_orders``: lets a test simulate
        # concurrent live-stream activity (accept/fill/latch) occurring while
        # the adapter awaits the drain.
        self.on_load_orders = on_load_orders
        self.product_rms_rows = list(product_rms_rows or [])
        self.account_rms_rows = list(account_rms_rows or [])
        self.account_rms_fault = account_rms_fault

    def load_product_rms_info(self) -> list[ProductRmsInfo]:
        self.calls.append("load_product_rms_info")
        return list(self.product_rms_rows)

    def load_account_rms_info(self) -> list[AccountRmsInfo]:
        self.calls.append("load_account_rms_info")
        if self.account_rms_fault:
            raise ConnectionError("account rms unavailable")
        return list(self.account_rms_rows)

    def connect(self) -> None:
        self.calls.append("connect")

    def subscribe_pnl(self) -> None:
        self.calls.append("subscribe_pnl")

    def resolved_account(self) -> dict[str, object] | None:
        return None

    def poll_pnl_event(self) -> dict[str, object] | None:
        return None

    def subscribe_order_updates(self) -> None:
        self.calls.append("subscribe_order_updates")

    def subscribe_bracket_updates(self) -> None:
        self.calls.append("subscribe_bracket_updates")

    def disconnect_order_plant(self) -> None:
        self.calls.append("disconnect_order_plant")

    def poll_order_event(self) -> dict[str, object] | None:
        return self._notifications.pop(0) if self._notifications else None

    def queue_notification(self, fields: dict[str, object]) -> None:
        self._notifications.append(fields)

    def load_orders(self, start: int, end: int) -> list[dict[str, object]]:
        self.calls.append("load_orders")
        if self.on_load_orders is not None:
            self.on_load_orders()
        return list(self.working_orders)

    def place_order(self, *args: object) -> None:
        self.calls.append("place_order")
        if self.fault == "submit":
            raise ConnectionError("channel cut after submit send")

    def cancel_order(self, venue_id: str) -> None:
        self.calls.append(f"cancel_order:{venue_id}")
        if self.fault == "cancel":
            raise ConnectionError("channel cut after cancel send")

    def modify_order(self, *args: object) -> None:
        self.calls.append("modify_order")
        if self.fault == "modify":
            raise ConnectionError("channel cut after modify send")

    def cancel_all_orders(self) -> None:
        self.calls.append("cancel_all_orders")
        if self.fault == "cancel_all":
            raise ConnectionError("channel cut after cancel_all send")


class _Log:
    """No-op logger for test doubles (superset of the per-file variants)."""

    def debug(self, *args: object, **kwargs: object) -> None:
        pass

    def info(self, *args: object, **kwargs: object) -> None:
        pass

    def warning(self, *args: object, **kwargs: object) -> None:
        pass

    def error(self, *args: object, **kwargs: object) -> None:
        pass

    def exception(self, *args: object, **kwargs: object) -> None:
        pass


class _CaptureLog(_Log):
    """Captures debug/warn/error so tests can assert on log level."""

    def __init__(self) -> None:
        self.debugs: list[str] = []
        self.messages: list[str] = []

    def debug(self, *args: object, **kwargs: object) -> None:
        self.debugs.append(" ".join(str(a) for a in args))

    def error(self, *args: object, **kwargs: object) -> None:
        self.messages.append(" ".join(str(a) for a in args))

    def warning(self, *args: object, **kwargs: object) -> None:
        self.messages.append(" ".join(str(a) for a in args))


class _CacheStub:
    """Minimal cache: venue<->client mapping + tracked-order presence."""

    def __init__(self) -> None:
        self._venue_to_client: dict[str, ClientOrderId] = {}
        self._client_to_venue: dict[str, VenueOrderId] = {}
        self._orders: dict[str, object] = {}
        self._instruments: dict[str, object] = {}

    def client_order_id(self, venue_order_id: VenueOrderId) -> ClientOrderId | None:
        return self._venue_to_client.get(venue_order_id.value)

    def venue_order_id(self, client_order_id: ClientOrderId) -> VenueOrderId | None:
        return self._client_to_venue.get(client_order_id.value)

    def add_venue_order_id(self, client: ClientOrderId, venue: VenueOrderId) -> None:
        self._client_to_venue[client.value] = venue
        self._venue_to_client[venue.value] = client

    def order(self, client_order_id: ClientOrderId) -> object | None:
        # Presence in the cache is the adapter's source of truth for "tracked".
        return self._orders.get(client_order_id.value)

    def instrument(self, instrument_id: InstrumentId) -> object | None:
        # Mirrors the real cache's contract: None when the id is unknown.
        return self._instruments.get(str(instrument_id))


class _TestClient(RithmicExecutionClient):
    """Test double: skips the Cython base's ``__init__`` and re-exposes the
    cdef read-only ``_log`` / ``_clock`` / ``_cache`` / ``account_id`` as
    writable properties, so real adapter methods run on a bare instance."""

    # The cdef base declares these read-only; re-expose as writable via
    # name-mangled storage. ``object.__getattribute__`` / ``__setattr__`` keep
    # the dynamic slots invisible to the type checker (no ``type: ignore``).

    @property
    def _log(self) -> _Log:
        return cast(_Log, object.__getattribute__(self, "_TestClient__log"))

    @_log.setter
    def _log(self, value: _Log) -> None:
        object.__setattr__(self, "_TestClient__log", value)

    @property
    def _clock(self) -> SimpleNamespace:
        return cast(
            SimpleNamespace, object.__getattribute__(self, "_TestClient__clock")
        )

    @_clock.setter
    def _clock(self, value: SimpleNamespace) -> None:
        object.__setattr__(self, "_TestClient__clock", value)

    @property
    def _cache(self) -> Any:
        return object.__getattribute__(self, "_TestClient__cache")

    @_cache.setter
    def _cache(self, value: Any) -> None:
        object.__setattr__(self, "_TestClient__cache", value)

    @property
    def account_id(self) -> AccountId | None:
        return cast(
            AccountId | None, object.__getattribute__(self, "_TestClient__account_id")
        )

    @account_id.setter
    def account_id(self, value: AccountId | None) -> None:
        object.__setattr__(self, "_TestClient__account_id", value)


def _conforms() -> None:
    """Static proof: the stub satisfies the ``WireSession`` protocol."""
    double: WireSession = WireSessionStub()
    _ = double
    _ = FaultInjectingSession()
