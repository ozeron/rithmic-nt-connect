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


def test_plant_poller_gap_runs_on_gap_until_it_succeeds_and_keeps_the_batch() -> None:
    """A gap is reported once, so its recovery is retried; events after it in the
    same batch are still handled."""
    attempts = []

    async def on_gap() -> None:
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("venue busy")

    handled = []
    resync = Mock()
    failed = Mock()
    poller = PlantPoller(
        name="order",
        poll_fn=lambda: None,
        batch_fn=lambda: [
            {"type": "gap", "stream": "order", "skipped": 4},
            {"type": "order_notification", "status": "open"},
        ],
        on_event=handled.append,
        on_resync=resync,
        on_gap=on_gap,
        on_resync_failed=failed,
    )

    res = asyncio.run(poller.poll_iteration(0.05, 0))
    assert res == (0.05, 0)
    assert len(attempts) == 3
    assert failed.call_count == 2
    resync.assert_not_called()
    assert handled == [{"type": "order_notification", "status": "open"}]


def test_an_unreadable_event_triggers_recovery_and_the_stream_continues() -> None:
    """One notification the adapter cannot read must not stop the order stream:
    the venue state is re-read (as after a gap) and later events are handled."""
    from rithmic_nt_connect.errors import UnreadableEventError

    handled: list[str] = []
    recoveries: list[int] = []

    def on_event(ev):
        if ev["status"] == "garbled":
            raise UnreadableEventError("missing symbol")
        handled.append(ev["status"])

    async def on_gap():
        recoveries.append(1)

    poller = PlantPoller(
        name="order",
        poll_fn=Mock(),
        batch_fn=lambda: [
            {"type": "order_notification", "status": "garbled"},
            {"type": "order_notification", "status": "open"},
        ],
        on_event=on_event,
        on_resync=Mock(),
        on_gap=on_gap,
        on_latch=lambda *a: (_ for _ in ()).throw(AssertionError("latched")),
    )
    res = asyncio.run(poller.poll_iteration(0.05, 0))
    assert res is not None, "the poller keeps running"
    assert handled == ["open"]
    assert recoveries == [1]
