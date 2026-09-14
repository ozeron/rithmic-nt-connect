"""Tests for PlantPoller collaborator."""

import asyncio
from unittest.mock import Mock

from rithmic_nt_connect.polling import PlantPoller


def test_plant_poller_normal_event_dispatch() -> None:
    events = [{"type": "order_notification", "status": "open"}, None]
    idx = 0

    def fake_poll():
        nonlocal idx
        if idx < len(events):
            ev = events[idx]
            idx += 1
            return ev
        return None

    handled = []

    def on_event(ev):
        handled.append(ev)

    poller = PlantPoller(
        name="order",
        poll_fn=fake_poll,
        on_event=on_event,
        on_resync=Mock(),
    )

    # First event
    res = asyncio.run(poller.poll_iteration(0.05, 0))
    assert res == (0.05, 0)
    assert len(handled) == 1

    # Second event (None)
    res2 = asyncio.run(poller.poll_iteration(0.05, 0))
    assert res2 == (0.05, 0)
    assert len(handled) == 1


def test_plant_poller_transient_error_latches_after_max() -> None:
    def fake_poll():
        raise RuntimeError("flaky network")

    latched = []

    poller = PlantPoller(
        name="order",
        poll_fn=fake_poll,
        on_event=Mock(),
        on_resync=Mock(),
        max_transient=3,
        on_latch=lambda tag, reason: latched.append((tag, reason)),
    )

    # Streak 1
    res1 = asyncio.run(poller.poll_iteration(0.05, 0))
    assert res1 == (0.05, 1)
    assert len(latched) == 0

    # Streak 2
    res2 = asyncio.run(poller.poll_iteration(0.05, 1))
    assert res2 == (0.05, 2)
    assert len(latched) == 0

    # Streak 3 -> Latches and returns None to stop stream
    res3 = asyncio.run(poller.poll_iteration(0.05, 2))
    assert res3 is None
    assert len(latched) == 1
    assert latched[0][0] == "order poll stream failure"
