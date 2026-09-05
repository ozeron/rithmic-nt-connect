"""Unit tests for GatewayRuntime TransportRecovery (L3)."""

from __future__ import annotations

import threading
import time

import pytest
from rithmic_gateway.transport_recovery import (
    FaultDomain,
    RecoveryFailed,
    TransportDown,
    TransportFault,
    TransportFaultKind,
    TransportPhase,
    TransportRecovery,
    TransportRecoveryError,
    TransportUp,
    fault_from_gateway_code,
)


def test_fault_from_gateway_code_r4_capability_is_none() -> None:
    assert fault_from_gateway_code("capability_denied", observed_gen=1) is None
    assert fault_from_gateway_code("history_denied_live_md", observed_gen=1) is None


def test_fault_from_gateway_code_maps_unix() -> None:
    fault = fault_from_gateway_code("eof", observed_gen=3)
    assert fault is not None
    assert fault.kind is TransportFaultKind.UNIX_EOF
    assert fault.observed_gen == 3
    assert fault.domain is FaultDomain.TRANSPORT


def test_report_fault_one_down_one_up_single_dial() -> None:
    dials = {"n": 0}
    connected = {"v": False}

    def replace() -> None:
        dials["n"] += 1
        connected["v"] = True

    recovery = TransportRecovery(
        replace_transport=replace,
        is_connected=lambda: connected["v"],
        max_attempts=3,
        initial_backoff_sec=0.01,
        max_backoff_sec=0.05,
        ensure_timeout_sec=2.0,
    )
    recovery.mark_live_after_connect()
    connected["v"] = True
    events: list[object] = []
    recovery.subscribe(events.append)

    connected["v"] = False
    recovery.report_fault(
        TransportFault(
            kind=TransportFaultKind.UNIX_EOF,
            observed_gen=recovery.generation,
        )
    )
    gen = recovery.ensure_live()

    assert dials["n"] == 1
    assert gen == 2
    assert isinstance(events[0], TransportDown)
    assert isinstance(events[1], TransportUp)
    assert events[1].gen == 2


def test_duplicate_fault_single_flight() -> None:
    dials = {"n": 0}
    gate = threading.Event()
    connected = {"v": False}

    def replace() -> None:
        dials["n"] += 1
        gate.wait(2.0)
        connected["v"] = True

    recovery = TransportRecovery(
        replace_transport=replace,
        is_connected=lambda: connected["v"],
        max_attempts=3,
        initial_backoff_sec=0.01,
        ensure_timeout_sec=2.0,
    )
    recovery.mark_live_after_connect()
    connected["v"] = True
    downs = {"n": 0}

    def on_event(evt: object) -> None:
        if isinstance(evt, TransportDown):
            downs["n"] += 1

    recovery.subscribe(on_event)
    fault = TransportFault(
        kind=TransportFaultKind.UNIX_EOF, observed_gen=recovery.generation
    )
    connected["v"] = False
    recovery.report_fault(fault)
    recovery.report_fault(fault)
    recovery.report_fault(fault)
    gate.set()
    recovery.ensure_live()
    assert downs["n"] == 1
    assert dials["n"] == 1


def test_stale_fault_ignored() -> None:
    dials = {"n": 0}
    recovery = TransportRecovery(
        replace_transport=lambda: dials.__setitem__("n", dials["n"] + 1),
        is_connected=lambda: True,
    )
    recovery.mark_live_after_connect()
    recovery.report_fault(
        TransportFault(kind=TransportFaultKind.UNIX_EOF, observed_gen=0)
    )
    assert dials["n"] == 0
    assert recovery.phase is TransportPhase.LIVE


def test_capability_domain_never_dials() -> None:
    dials = {"n": 0}
    recovery = TransportRecovery(
        replace_transport=lambda: dials.__setitem__("n", dials["n"] + 1),
        is_connected=lambda: True,
    )
    recovery.mark_live_after_connect()
    recovery.report_fault(
        TransportFault(
            kind=TransportFaultKind.UNIX_EOF,
            observed_gen=1,
            domain=FaultDomain.CAPABILITY,
        )
    )
    assert dials["n"] == 0


def test_stopping_ignores_fault() -> None:
    dials = {"n": 0}
    recovery = TransportRecovery(
        replace_transport=lambda: dials.__setitem__("n", dials["n"] + 1),
        is_connected=lambda: False,
    )
    recovery.mark_live_after_connect()
    recovery.mark_stopping()
    recovery.report_fault(
        TransportFault(kind=TransportFaultKind.UNIX_EOF, observed_gen=1)
    )
    assert dials["n"] == 0
    with pytest.raises(TransportRecoveryError):
        recovery.ensure_live()


def test_recovery_failed_emits_and_ensure_times_out() -> None:
    def replace() -> None:
        raise RuntimeError("dial refused")

    recovery = TransportRecovery(
        replace_transport=replace,
        is_connected=lambda: False,
        max_attempts=2,
        initial_backoff_sec=0.01,
        max_backoff_sec=0.02,
        ensure_timeout_sec=0.5,
    )
    recovery.mark_live_after_connect()
    events: list[object] = []
    recovery.subscribe(events.append)
    recovery.report_fault(
        TransportFault(kind=TransportFaultKind.UNIX_EOF, observed_gen=1)
    )
    with pytest.raises(TransportRecoveryError):
        recovery.ensure_live()
    assert any(isinstance(e, RecoveryFailed) for e in events)
    # Later ensure after FAILED may start a new flight; budget still applies.
    time.sleep(0.05)
