"""A failed order command is rejected, not sent, or unknown: only unknown latches.

The class is assigned where the error arises (Rust, both modes) and carried by the
error's ``code``; the exec client turns it into the right Nautilus event.
"""

from __future__ import annotations

from typing import Any

import pytest
from rithmic_nt_connect.execution import (
    RithmicExecutionClient,
    _failure_class,
    _failure_reason,
)


def _error(code: str | None, message: str = "boom") -> Exception:
    exc = Exception(message)
    if code is not None:
        setattr(exc, "code", code)  # noqa: B010
    return exc


@pytest.mark.parametrize(
    ("code", "want"),
    [
        ("venue_rejected", "rejected"),
        ("not_sent", "not_sent"),
        ("trading_disabled", "not_sent"),
        ("cancel_all_denied", "not_sent"),
        ("not_ready", "not_sent"),
        ("outcome_unknown", "unknown"),
        ("venue_unknown", "unknown"),
        ("transport_reset", "unknown"),
        (None, "unknown"),
    ],
)
def test_the_error_code_decides_the_class(code: str | None, want: str) -> None:
    assert _failure_class(_error(code)) == want


def test_an_unrecognised_failure_is_treated_as_unknown() -> None:
    # Anything that may have reached the venue must latch, never be called a reject.
    assert _failure_class(TimeoutError("late")) == "unknown"
    assert _failure_class(RuntimeError("plant gone")) == "unknown"


def test_reasons_say_which_class_it_was() -> None:
    assert _failure_reason("rejected", _error("venue_rejected", "price")).startswith(
        "venue rejected: "
    )
    assert _failure_reason("not_sent", _error("not_sent", "no route")).startswith(
        "not sent: "
    )


class _Log:
    def __init__(self) -> None:
        self.errors: list[str] = []

    def error(self, message: str) -> None:
        self.errors.append(message)


class _Host:
    """Just what ``_order_command_failed`` touches."""

    def __init__(self) -> None:
        self._log = _Log()
        self.latched: list[tuple[str, BaseException]] = []

    def _mark_order_plant_failed(self, action: str, exc: BaseException) -> None:
        self.latched.append((action, exc))


def _handle(host: _Host, exc: Exception) -> list[str]:
    rejected: list[str] = []
    method: Any = RithmicExecutionClient._order_command_failed
    method(host, "place_order", exc, reject=rejected.append)
    return rejected


def test_a_venue_reject_rejects_the_order_and_keeps_the_plant_trading() -> None:
    host = _Host()
    rejected = _handle(host, _error("venue_rejected", "price outside limits"))
    assert rejected == ["venue rejected: price outside limits"]
    assert host.latched == []


def test_a_request_that_was_never_sent_rejects_the_order_and_keeps_trading() -> None:
    host = _Host()
    rejected = _handle(host, _error("not_sent", "no trade route"))
    assert rejected == ["not sent: no trade route"]
    assert host.latched == []


def test_an_outcome_that_may_be_live_latches_and_does_not_reject() -> None:
    host = _Host()
    exc = _error("outcome_unknown", "socket dropped after write")
    rejected = _handle(host, exc)
    assert rejected == [], "an order that may be live is never reported rejected"
    assert host.latched == [("place_order", exc)]


def test_an_error_without_a_class_is_unknown() -> None:
    host = _Host()
    exc: Any = RuntimeError("something else")
    assert _handle(host, exc) == []
    assert host.latched == [("place_order", exc)]
