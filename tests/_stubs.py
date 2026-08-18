"""Shared test doubles.

``WireSessionStub`` implements the full ``WireSession`` protocol so unit-test
fakes are structurally conformant (a checker cannot see partial duck-typed
doubles). Members are deliberately permissive (``*args``/``**kwargs``): the
stub's job is conformance, not signature enforcement — subclasses override with
their own signatures and the unused members raise loudly instead of passing
silently.
"""

from __future__ import annotations

from typing import Any

from rithmic_nt_connect.session import WireSession


class WireSessionStub:
    """Base class for ``WireSession`` test doubles.

    Every protocol member raises ``NotImplementedError`` unless overridden, so a
    double is honest about which members it provides and tests fail loudly if
    the code under test reaches one it does not stub.
    """

    def __getattr__(self, name: str) -> Any:
        # Catch-all for members added to ``WireSession`` in the future: fail
        # loudly rather than silently return a bogus value.
        raise NotImplementedError(f"WireSessionStub has no member {name!r}")

    def connect(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def disconnect(self, *args: Any, **kwargs: Any) -> None:
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

    def poll_order_event(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError


def _conforms() -> None:
    """Static proof: the stub satisfies the ``WireSession`` protocol."""
    double: WireSession = WireSessionStub()
    _ = double
