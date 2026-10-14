"""S7: throughput through the PyO3 ``Session`` into Python dicts.

Synthetic (fake plant, no credentials) and measurement-only: it prints one JSON
line per scenario and, when ``RITHMIC_PERF_OUT`` is set, writes them to that
file. Enable with ``RITHMIC_PERF=1``. ``RITHMIC_PERF_EVENTS`` sets the number of
ticks per scenario (default 2000). The Rust load harness (``rithmic-load``)
covers the server side; this covers the Python boundary.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

import pytest
from gateway_fake import FakeParent, build_fake_gateway, running_fake
from rithmic_nt_connect.errors import ChannelLaggedError

pytestmark = [
    pytest.mark.perf,
    pytest.mark.skipif(
        os.environ.get("RITHMIC_PERF") != "1", reason="set RITHMIC_PERF=1 to run"
    ),
]

SYMBOL = "NQH6"
EXCHANGE = "CME"
EVENTS = int(os.environ.get("RITHMIC_PERF_EVENTS", "2000"))
DEADLINE_SEC = float(os.environ.get("RITHMIC_PERF_DEADLINE", "120"))
_RESULTS: list[dict[str, Any]] = []


@pytest.fixture(scope="module")
def fake_bin():
    binary = build_fake_gateway()
    if binary is None:
        pytest.skip("rithmic-gateway-fake unavailable (no cargo / build failed)")
    return binary


@pytest.fixture(scope="module", autouse=True)
def _write_results():
    yield
    out = os.environ.get("RITHMIC_PERF_OUT")
    if out:
        with open(out, "w", encoding="utf-8") as fh:
            json.dump(_RESULTS, fh, indent=2)


def _session(parent: FakeParent) -> Any:
    from rithmic_nt_connect._lib import Session

    cfg = parent.config()
    session = Session(
        user=cfg.user,
        system_name=cfg.system_name,
        url=cfg.url,
        env=cfg.env,
        connect_mode="gateway",
        gateway_listen=cfg.listen,
    )
    session.connect()
    return session


def _record(name: str, **fields: Any) -> dict[str, Any]:
    row = {"scenario": name, **fields}
    _RESULTS.append(row)
    print(json.dumps(row))
    return row


def _drain_blocking(session: Any, want: int) -> tuple[int, int, float]:
    """Returns (received, skipped_by_reported_gaps, seconds).

    A burst larger than the client queue is reported as ``ChannelLaggedError``
    (never silent); ``received + skipped`` accounts for every event.
    """
    got = skipped = 0
    start = time.perf_counter()
    deadline = start + DEADLINE_SEC
    while got + skipped < want and time.perf_counter() < deadline:
        try:
            if session.poll_event(timeout_ms=100) is not None:
                got += 1
        except ChannelLaggedError as exc:
            skipped += _lag_count(exc)
    return got, skipped, time.perf_counter() - start


def _lag_count(exc: Exception) -> int:
    """The skipped count from a ``ChannelLaggedError`` message (``plant:skipped``)."""
    try:
        return int(str(exc).rsplit(":", 1)[-1])
    except ValueError:
        return 0


def test_ffi_blocking_poll_throughput(fake_bin) -> None:
    with running_fake(fake_bin) as parent:
        session = _session(parent)
        try:
            session.subscribe(SYMBOL, EXCHANGE)
            parent.push_last_trades(SYMBOL, EVENTS)
            got, skipped, secs = _drain_blocking(session, EVENTS)
        finally:
            session.disconnect()
    _record(
        "py_blocking_poll_event",
        events=EVENTS,
        received=got,
        reported_skipped=skipped,
        seconds=round(secs, 3),
        events_per_sec=round(got / secs, 1) if secs else 0.0,
    )
    assert got + skipped >= EVENTS, "every event is received or reported as skipped"


def test_ffi_to_thread_poll_throughput(fake_bin) -> None:
    """The adapter's shape: one ``asyncio.to_thread`` hop per polled event."""

    async def run(session: Any) -> tuple[int, int, float]:
        got = skipped = 0
        start = time.perf_counter()
        deadline = start + DEADLINE_SEC
        while got + skipped < EVENTS and time.perf_counter() < deadline:
            try:
                event = await asyncio.to_thread(session.poll_event, 0)
            except ChannelLaggedError as exc:
                skipped += _lag_count(exc)
                continue
            if event is None:
                await asyncio.sleep(0.01)
            else:
                got += 1
        return got, skipped, time.perf_counter() - start

    with running_fake(fake_bin) as parent:
        session = _session(parent)
        try:
            session.subscribe(SYMBOL, EXCHANGE)
            parent.push_last_trades(SYMBOL, EVENTS)
            got, skipped, secs = asyncio.run(run(session))
        finally:
            session.disconnect()
    _record(
        "py_to_thread_poll_event",
        events=EVENTS,
        received=got,
        reported_skipped=skipped,
        seconds=round(secs, 3),
        events_per_sec=round(got / secs, 1) if secs else 0.0,
    )
    assert got + skipped >= EVENTS, "every event is received or reported as skipped"


def test_ffi_empty_poll_cost(fake_bin) -> None:
    with running_fake(fake_bin) as parent:
        session = _session(parent)
        try:
            n = 5000
            start = time.perf_counter()
            for _ in range(n):
                session.poll_event(timeout_ms=0)
            secs = time.perf_counter() - start
        finally:
            session.disconnect()
    _record(
        "py_empty_poll_cost",
        polls=n,
        seconds=round(secs, 4),
        us_per_poll=round(secs / n * 1e6, 1),
    )
