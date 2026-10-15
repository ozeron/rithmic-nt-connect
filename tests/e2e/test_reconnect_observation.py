"""Offline checks for the live reconnect test's timeout diagnostics."""

from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest
import test_reconnect_live as reconnect
from test_reconnect_live import wait_for_any_event


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> None:
    ticks = iter(float(n) for n in range(10))
    monkeypatch.setattr(
        reconnect,
        "time",
        SimpleNamespace(monotonic=lambda: next(ticks), sleep=lambda _: None),
    )


def _poll(events: list[dict[str, Any] | None]):
    queued: Iterator[dict[str, Any] | None] = iter(events)
    return lambda: next(queued)


@pytest.mark.usefixtures("clock")
def test_timeout_distinguishes_empty_polls_from_other_records() -> None:
    with pytest.raises(AssertionError) as failure:
        wait_for_any_event(
            _poll([None, {"type": "order_book"}, {"type": "bbo"}]),
            ("last_trade",),
            timeout_sec=4,
        )
    message = str(failure.value)
    assert "empty_polls=1" in message
    assert "'order_book': 1" in message and "'bbo': 1" in message


@pytest.mark.usefixtures("clock")
def test_timeout_reports_raw_trade_timestamps_before_filtering() -> None:
    with pytest.raises(AssertionError) as failure:
        wait_for_any_event(
            _poll(
                [
                    {
                        "type": "last_trade",
                        "symbol": "NQZ6",
                        "exchange": "CME",
                        "ts_event_ns": 100,
                    },
                    None,
                    {"type": "last_trade", "ts_event_ns": 200},
                ]
            ),
            ("last_trade",),
            timeout_sec=4,
            accept=lambda event: event["ts_event_ns"] > 300,
        )
    message = str(failure.value)
    assert "'last_trade': 2" in message
    assert "filtered_target_events=2" in message
    assert "('NQZ6', 'CME'): 1" in message
    assert "last_trade_ts_event_ns=[100, 200]" in message


@pytest.mark.usefixtures("clock")
def test_fresh_trade_passes_without_changing_the_selected_record() -> None:
    fresh = {"type": "last_trade", "ts_event_ns": 400}
    assert (
        wait_for_any_event(
            _poll([{"type": "last_trade", "ts_event_ns": 100}, None, fresh]),
            ("last_trade",),
            timeout_sec=4,
            accept=lambda event: event["ts_event_ns"] > 300,
        )
        is fresh
    )
