"""Order-plant lifecycle state machine and trading policy.

The rules (transition table, latch, re-arm race token, trading policy) live in
the ``rithmic-exec`` Rust crate and are pinned by its exhaustive transition
test and ``tests/test_order_plant.py``. This module is the binding: it keeps the
``OrderPlantState`` enum the exec client matches on and delegates every
transition and decision to the extension.
"""

from __future__ import annotations

from enum import Enum

from rithmic_nt_connect._lib import OrderPlantPolicy as _CorePolicy


class OrderPlantState(Enum):
    DISCONNECTED = "disconnected"  # transport down / not armed; commands blocked
    CONNECTING = "connecting"  # re-arm barrier in progress; not yet armed
    LIVE = "live"  # armed; submit / modify / cancel allowed
    RESYNCING = "resyncing"  # mid-session resync; cancels allowed
    LATCHED = "latched"  # blocked pending a recon cycle (re-arm)


class OrderPlantPolicy:
    """Owns every order-plant state transition and the trading policy.

    ``state`` is read-only outside the policy; ``latched`` is derived (a pure
    function of state). ``rearm`` is the only arming transition.
    """

    def __init__(self, state: OrderPlantState = OrderPlantState.DISCONNECTED) -> None:
        self._core = _CorePolicy(state.value)

    @property
    def state(self) -> OrderPlantState:
        return OrderPlantState(self._core.state)

    @property
    def latched(self) -> bool:
        """True while commands are blocked pending a recon cycle."""
        return self._core.latched

    # --- transitions ---

    def begin_connect(self) -> None:
        """Enter the re-arm barrier (transport subscribed, not yet armed)."""
        self._core.begin_connect()

    def disconnect(self) -> None:
        """Transport down / not armed: commands blocked."""
        self._core.disconnect()

    def latch(self) -> None:
        """An anomaly that needs a recon cycle: commands blocked until a
        successful re-arm. Callers log the reason themselves."""
        self._core.latch()

    def resync_start(self) -> None:
        """Begin a mid-session transport resync (cancels stay available)."""
        self._core.resync_start()

    def resync_complete(self) -> None:
        """A mid-session resync succeeded: live again (unless latched or down)."""
        self._core.resync_complete()

    def resync_failed(self) -> None:
        """A mid-session resync failed: transport down, commands blocked."""
        self._core.resync_failed()

    def rearm(self, *, poll_alive: bool) -> bool:
        """Finish the re-arm barrier; False when the plant stays un-armed."""
        return self._core.rearm(poll_alive)

    # --- policy ---

    def allow_submit(self) -> bool:
        return self._core.allow_submit()

    def allow_modify(self) -> bool:
        return self._core.allow_modify()

    def allow_cancel(self) -> bool:
        return self._core.allow_cancel()

    def reject_reason(self, action: str) -> str:
        return self._core.reject_reason(action)
