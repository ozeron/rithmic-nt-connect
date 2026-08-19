"""Order-plant lifecycle state machine and trading policy.

Root-cause design (2026-08-19, Oracle + Macroscope review rounds): the
readiness/recovery protocol is an *explicit machine*, not scattered state
assignments. The plant state is private and moves only through the transition
methods below — execution code never assigns ``state`` directly, so the set of
possible transitions is finite and pinned by the transition table in
``tests/test_order_plant.py``.

``LATCHED`` is a real state: "an operation's venue outcome is unknown, commands
blocked until a recon cycle (a successful re-arm)". It is not a parallel flag on
the client, and it cannot be collapsed into the transport states — a resync
must not re-arm a latched plant, and a failed re-arm leaves recon pending. Only
``latch`` enters it; the recovery out of it is a successful ``rearm`` (a new
``begin_connect``/``disconnect`` also moves out of it, but commands stay
blocked by the un-armed/down state). The re-arm race token is ``rearm``: it
arms only from ``CONNECTING`` with a live poll task, so any anomaly during the
barrier (a newer latch, a stream failure, a mid-drain resync) leaves the plant
un-armed.
"""

from __future__ import annotations

from enum import Enum


class OrderPlantState(Enum):
    DISCONNECTED = "disconnected"  # transport down / not armed; commands blocked
    CONNECTING = "connecting"  # re-arm barrier in progress; not yet armed
    LIVE = "live"  # armed; submit / modify / cancel allowed
    RESYNCING = "resyncing"  # mid-session transport resync; cancels allowed
    LATCHED = "latched"  # blocked pending a recon cycle (re-arm)


class OrderPlantPolicy:
    """Owns every order-plant state transition and the trading policy.

    ``state`` is read-only outside the policy; ``latched`` is derived (a pure
    function of state). ``rearm`` is the only arming transition.
    """

    def __init__(self, state: OrderPlantState = OrderPlantState.DISCONNECTED) -> None:
        self._state = state

    @property
    def state(self) -> OrderPlantState:
        return self._state

    @property
    def latched(self) -> bool:
        """True while commands are blocked pending a recon cycle."""
        return self._state is OrderPlantState.LATCHED

    # --- transitions ---

    def begin_connect(self) -> None:
        """Enter the re-arm barrier (transport subscribed, not yet armed)."""
        self._state = OrderPlantState.CONNECTING

    def disconnect(self) -> None:
        """Transport down / not armed: commands blocked."""
        self._state = OrderPlantState.DISCONNECTED

    def latch(self) -> None:
        """An anomaly that needs a recon cycle: commands blocked until a
        successful re-arm (``rearm``). A mid-session resync never clears it.
        Callers log the reason themselves; the machine only needs state."""
        self._state = OrderPlantState.LATCHED

    def resync_start(self) -> None:
        """Begin a mid-session transport resync (cancels stay available).

        From ``LATCHED`` the plant stays blocked (a resync never clears a
        latch). From ``DISCONNECTED`` it stays down: a dead/broken stream must
        not re-arm via a resync — only the full-connect re-arm barrier (drain
        + PnL gate) may arm the plant. From ``CONNECTING`` it latches: a
        channel error during the re-arm barrier means the drain may predate the
        drop, so the plant must not enter the cancel-enabled ``RESYNCING`` path
        (whose ``resync_complete`` would re-arm it to ``LIVE`` mid-barrier).
        The barrier's ``rearm`` then fails over ``LATCHED`` and the plant stays
        un-armed.
        """
        if self._state in (OrderPlantState.LATCHED, OrderPlantState.DISCONNECTED):
            return
        if self._state is OrderPlantState.CONNECTING:
            self._state = OrderPlantState.LATCHED
            return
        self._state = OrderPlantState.RESYNCING

    def resync_complete(self) -> None:
        """A mid-session resync succeeded: live again (unless latched or down).

        ``CONNECTING`` is left untouched: a resync completing while the re-arm
        barrier is running must not arm the plant — ``rearm`` is the only
        arming transition and it fails on any state other than ``CONNECTING``.
        """
        if self._state is not OrderPlantState.RESYNCING:
            return
        self._state = OrderPlantState.LIVE

    def resync_failed(self) -> None:
        """A mid-session resync failed: transport down, commands blocked. A
        latched plant stays latched (recon still pending)."""
        if self._state is OrderPlantState.LATCHED:
            return
        self._state = OrderPlantState.DISCONNECTED

    def rearm(self, *, poll_alive: bool) -> bool:
        """Finish the re-arm barrier: armed (``LIVE``) only if the plant is
        still ``CONNECTING`` — no newer latch / stream failure during the
        drain — and the poll task is alive. A newer latch survives untouched;
        any other raced state falls to ``DISCONNECTED``. Returns False when
        the plant stays un-armed."""
        if self._state is OrderPlantState.LATCHED:
            # A newer latch during the drain: keep it (recon still pending).
            return False
        if self._state is not OrderPlantState.CONNECTING:
            self._state = OrderPlantState.DISCONNECTED
            return False
        if not poll_alive:
            # Dead poll task: the drain ran over a stream that cannot deliver.
            self._state = OrderPlantState.DISCONNECTED
            return False
        self._state = OrderPlantState.LIVE
        return True

    # --- policy ---

    def allow_submit(self) -> bool:
        return self._state is OrderPlantState.LIVE

    def allow_modify(self) -> bool:
        return self._state is OrderPlantState.LIVE

    def allow_cancel(self) -> bool:
        # Cancels remain available during resync (risk-reducing); blocked when
        # down, un-armed, or latched (a recon cycle is required first).
        return self._state in {OrderPlantState.LIVE, OrderPlantState.RESYNCING}

    def load_orders_available(self) -> bool:
        return True

    def reject_reason(self, action: str) -> str:
        return f"order plant {self._state.value}; {action} blocked"
