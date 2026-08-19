"""Transition-table tests for the order-plant state machine.

Root-cause coverage (2026-08-19): the readiness/recovery protocol is an
explicit machine — every state transition is listed in ``TRANSITIONS`` below
and asserted, so the class of review finding that keeps re-occurring ("another
combination of plant state / latch / stream facts") is a checked class rather
than a per-scenario patch. The latch is an owned bit of the machine: ``latched``
is a pure function of state, ``rearm`` is the only arming transition, and a
mid-session resync can never clear a latch or re-arm a down plant.
"""

from __future__ import annotations

import pytest
from rithmic_nt_connect._order_plant import OrderPlantPolicy, OrderPlantState

# (source_state, event) -> target_state. This table IS the executable spec of
# the plant lifecycle; add a row whenever a transition is added or changed.
TRANSITIONS: dict[tuple[OrderPlantState, str], OrderPlantState] = {
    # begin_connect: any state enters the re-arm barrier.
    (OrderPlantState.DISCONNECTED, "begin_connect"): OrderPlantState.CONNECTING,
    (OrderPlantState.LIVE, "begin_connect"): OrderPlantState.CONNECTING,
    (OrderPlantState.RESYNCING, "begin_connect"): OrderPlantState.CONNECTING,
    (OrderPlantState.LATCHED, "begin_connect"): OrderPlantState.CONNECTING,
    (OrderPlantState.CONNECTING, "begin_connect"): OrderPlantState.CONNECTING,
    # disconnect: transport down / not armed.
    (OrderPlantState.DISCONNECTED, "disconnect"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.CONNECTING, "disconnect"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.LIVE, "disconnect"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.RESYNCING, "disconnect"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.LATCHED, "disconnect"): OrderPlantState.DISCONNECTED,
    # latch: an anomaly that needs a recon cycle.
    (OrderPlantState.DISCONNECTED, "latch"): OrderPlantState.LATCHED,
    (OrderPlantState.CONNECTING, "latch"): OrderPlantState.LATCHED,
    (OrderPlantState.LIVE, "latch"): OrderPlantState.LATCHED,
    (OrderPlantState.RESYNCING, "latch"): OrderPlantState.LATCHED,
    (OrderPlantState.LATCHED, "latch"): OrderPlantState.LATCHED,
    # rearm: the ONLY arming transition — CONNECTING + live poll task.
    (OrderPlantState.CONNECTING, "rearm(alive)"): OrderPlantState.LIVE,
    (OrderPlantState.CONNECTING, "rearm(dead)"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.DISCONNECTED, "rearm(alive)"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.DISCONNECTED, "rearm(dead)"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.LIVE, "rearm(alive)"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.LIVE, "rearm(dead)"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.RESYNCING, "rearm(alive)"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.RESYNCING, "rearm(dead)"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.LATCHED, "rearm(alive)"): OrderPlantState.LATCHED,
    (OrderPlantState.LATCHED, "rearm(dead)"): OrderPlantState.LATCHED,
    # resync_start: LIVE enters RESYNCING; CONNECTING latches (a channel error
    # during the re-arm barrier must not enter the cancel-enabled RESYNCING
    # path, whose resync_complete would re-arm the plant mid-barrier); LATCHED
    # stays blocked; DISCONNECTED stays down (a dead stream must not re-arm via
    # resync).
    (OrderPlantState.LIVE, "resync_start"): OrderPlantState.RESYNCING,
    (OrderPlantState.CONNECTING, "resync_start"): OrderPlantState.LATCHED,
    (OrderPlantState.RESYNCING, "resync_start"): OrderPlantState.RESYNCING,
    (OrderPlantState.LATCHED, "resync_start"): OrderPlantState.LATCHED,
    (OrderPlantState.DISCONNECTED, "resync_start"): OrderPlantState.DISCONNECTED,
    # resync_complete: RESYNCING -> LIVE; CONNECTING stays in the barrier;
    # LATCHED/DISCONNECTED stay as they are.
    (OrderPlantState.RESYNCING, "resync_complete"): OrderPlantState.LIVE,
    (OrderPlantState.CONNECTING, "resync_complete"): OrderPlantState.CONNECTING,
    (OrderPlantState.LATCHED, "resync_complete"): OrderPlantState.LATCHED,
    (OrderPlantState.DISCONNECTED, "resync_complete"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.LIVE, "resync_complete"): OrderPlantState.LIVE,
    # resync_failed: transport down; a latched plant stays latched.
    (OrderPlantState.RESYNCING, "resync_failed"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.CONNECTING, "resync_failed"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.LIVE, "resync_failed"): OrderPlantState.DISCONNECTED,
    (OrderPlantState.LATCHED, "resync_failed"): OrderPlantState.LATCHED,
    (OrderPlantState.DISCONNECTED, "resync_failed"): OrderPlantState.DISCONNECTED,
}

# Every (state, event) pair must be pinned: a transition added without a table
# row fails this completeness check.
EVENTS = {
    "begin_connect",
    "disconnect",
    "latch",
    "rearm(alive)",
    "rearm(dead)",
    "resync_start",
    "resync_complete",
    "resync_failed",
}


def _apply(plant: OrderPlantPolicy, event: str) -> None:
    if event == "begin_connect":
        plant.begin_connect()
    elif event == "disconnect":
        plant.disconnect()
    elif event == "latch":
        plant.latch("test")
    elif event == "rearm(alive)":
        plant.rearm(poll_alive=True)
    elif event == "rearm(dead)":
        plant.rearm(poll_alive=False)
    elif event == "resync_start":
        plant.resync_start()
    elif event == "resync_complete":
        plant.resync_complete()
    elif event == "resync_failed":
        plant.resync_failed()
    else:
        raise AssertionError(f"unknown event: {event}")


def test_transition_table_is_complete() -> None:
    """Every (state, event) pair has a pinned target — the table cannot drift
    from the implementation."""
    missing = {
        (state, event)
        for state in OrderPlantState
        for event in EVENTS
        if (state, event) not in TRANSITIONS
    }
    assert not missing, f"transition table missing rows: {sorted(missing)}"


@pytest.mark.parametrize(
    ("source", "event", "target"),
    [(s, e, t) for (s, e), t in TRANSITIONS.items()],
)
def test_transition_table(
    source: OrderPlantState, event: str, target: OrderPlantState
) -> None:
    plant = OrderPlantPolicy(source)
    _apply(plant, event)
    assert plant.state is target


def test_latch_bit_follows_state() -> None:
    """The latch is an owned bit of the machine: true only in LATCHED, and
    cleared exactly when rearm succeeds (LIVE)."""
    for state in OrderPlantState:
        plant = OrderPlantPolicy(state)
        assert plant.latched is (state is OrderPlantState.LATCHED)
    plant = OrderPlantPolicy(OrderPlantState.LATCHED)
    plant.begin_connect()
    assert not plant.latched  # derived: LATCHED is the blocked state; the
    # barrier (CONNECTING) is "recon in progress", not "blocked pending recon"
    assert plant.rearm(poll_alive=True)
    assert plant.state is OrderPlantState.LIVE
    assert not plant.latched


def test_allow_policy_is_a_pure_function_of_state() -> None:
    """submit/modify only LIVE; cancel also RESYNCING; latched/down block
    everything (a recon cycle is required first)."""
    for state in OrderPlantState:
        plant = OrderPlantPolicy(state)
        assert plant.allow_submit() is (state is OrderPlantState.LIVE)
        assert plant.allow_modify() is (state is OrderPlantState.LIVE)
        assert plant.allow_cancel() is (
            state in (OrderPlantState.LIVE, OrderPlantState.RESYNCING)
        )
        assert plant.load_orders_available()


def test_rearm_is_the_race_token() -> None:
    """rearm arms only from CONNECTING with a live poll task; a newer latch
    during the barrier survives untouched."""
    # A latch raised during the barrier survives; rearm returns False.
    plant = OrderPlantPolicy()
    plant.begin_connect()
    plant.latch("overfill mid-drain")
    assert not plant.rearm(poll_alive=True)
    assert plant.state is OrderPlantState.LATCHED
    assert plant.latched
    # A dead poll task cannot deliver: un-armed.
    plant = OrderPlantPolicy()
    plant.begin_connect()
    assert not plant.rearm(poll_alive=False)
    assert plant.state is OrderPlantState.DISCONNECTED
    # Happy path: CONNECTING + live poll task arms and clears the latch.
    plant = OrderPlantPolicy(OrderPlantState.LATCHED)
    plant.begin_connect()
    assert plant.rearm(poll_alive=True)
    assert plant.state is OrderPlantState.LIVE
    assert not plant.latched


def test_latch_survives_mid_session_resync() -> None:
    """A transport resync never clears a latch: commands stay blocked until a
    successful re-arm."""
    plant = OrderPlantPolicy(OrderPlantState.LATCHED)
    plant.resync_start()
    assert plant.state is OrderPlantState.LATCHED
    plant.resync_complete()
    assert plant.state is OrderPlantState.LATCHED
    plant.resync_failed()
    assert plant.state is OrderPlantState.LATCHED


def test_down_plant_cannot_rearm_via_resync() -> None:
    """After a resync failure (or any down state), a later resync must not arm
    the plant: only the full-connect re-arm barrier (drain + PnL gate) arms it.
    This is the combination the table makes visible — DISCONNECTED -> resync ->
    LIVE would re-enable commands without re-observing venue state."""
    plant = OrderPlantPolicy(OrderPlantState.LIVE)
    plant.resync_start()
    plant.resync_failed()
    assert plant.state is OrderPlantState.DISCONNECTED
    plant.resync_start()
    assert plant.state is OrderPlantState.DISCONNECTED
    plant.resync_complete()
    assert plant.state is OrderPlantState.DISCONNECTED
    assert not plant.allow_submit()


def test_mid_barrier_resync_cannot_arm() -> None:
    """A channel error + successful resync DURING the reconnect barrier must
    never arm the plant: CONNECTING -> resync -> LIVE would let strategies
    submit while the drain/PnL gate is still running. The resync latches the
    barrier instead, and rearm fails over LATCHED (Oracle #1)."""
    plant = OrderPlantPolicy()
    plant.begin_connect()
    assert plant.state is OrderPlantState.CONNECTING
    # Channel error on the order stream while the drain is in flight.
    plant.resync_start()
    assert plant.state is OrderPlantState.LATCHED
    # The transport resubscribe succeeds: still blocked, never LIVE.
    plant.resync_complete()
    assert plant.state is OrderPlantState.LATCHED
    assert not plant.allow_submit()
    assert not plant.allow_modify()
    assert not plant.allow_cancel()
    # The barrier finishes: rearm cannot arm over LATCHED.
    assert not plant.rearm(poll_alive=True)
    assert plant.state is OrderPlantState.LATCHED
    assert plant.latched
