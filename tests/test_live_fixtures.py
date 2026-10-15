"""Offline coverage of live fixture selection and history availability gates."""

from __future__ import annotations

import importlib
import inspect
from pathlib import Path

import pytest
from _stubs import WireSessionStub
from test_providers import COMPLETE_REF, LISTED_REF


@pytest.fixture
def live_fixtures(monkeypatch):
    # Match pytest's e2e import path without activating its live fixture tree.
    monkeypatch.syspath_prepend(str(Path(__file__).parent / "e2e"))
    return importlib.import_module("e2e.conftest")


class _Session(WireSessionStub):
    def get_front_month(self, symbol, exchange):
        assert (symbol, exchange) == ("NQ", "CME")
        return {"trading_symbol": "NQU6", "trading_exchange": "CME"}

    def get_reference_data(self, symbol, exchange):
        assert exchange == "CME"
        return dict(LISTED_REF if symbol == "NQZ6" else COMPLETE_REF)


@pytest.fixture(autouse=True)
def _contract_environment(monkeypatch):
    monkeypatch.delenv("RITHMIC_E2E_CONTRACT", raising=False)
    monkeypatch.delenv("RITHMIC_E2E_EXCHANGE", raising=False)


@pytest.mark.parametrize("exchange", [None, "", "CME"])
def test_contract_override_builds_real_instrument_and_shared_route(
    monkeypatch, live_fixtures, exchange
):
    monkeypatch.setenv("RITHMIC_E2E_CONTRACT", "NQZ6")
    if exchange is not None:
        monkeypatch.setenv("RITHMIC_E2E_EXCHANGE", exchange)
    instrument, symbol, venue = inspect.unwrap(live_fixtures.live_front_month)(
        _Session()
    )
    assert str(instrument.id) == "NQZ6-CME.RITHMIC"
    assert (symbol, venue) == ("NQZ6", "CME")
    assert inspect.unwrap(live_fixtures.live_contract)((instrument, symbol, venue)) == (
        symbol,
        venue,
    )


def test_without_override_still_resolves_front_month(live_fixtures):
    instrument, symbol, exchange = inspect.unwrap(live_fixtures.live_front_month)(
        _Session()
    )
    assert str(instrument.id) == "NQU6-CME.RITHMIC"
    assert (symbol, exchange) == ("NQU6", "CME")


@pytest.mark.parametrize("contract", [None, "NQZ6"])
def test_exec_fixture_uses_same_instrument_loader(monkeypatch, live_fixtures, contract):
    from contextlib import contextmanager

    if contract:
        monkeypatch.setenv("RITHMIC_E2E_CONTRACT", contract)

    @contextmanager
    def connected():
        yield _Session()

    monkeypatch.setattr(live_fixtures, "_connected_data_session", connected)
    # Exercise the fixture's loader after the exec safety gates have passed.
    instrument = inspect.unwrap(live_fixtures.exec_front_month_instrument)("Test", True)
    expected, *_ = inspect.unwrap(live_fixtures.live_front_month)(_Session())
    assert instrument.id == expected.id
    assert instrument.info == expected.info


@pytest.mark.parametrize(
    ("messages", "expected", "terminated"),
    [
        ([("ready", ""), ("available", "")], (True, ""), False),
        (
            [("ready", ""), ("unavailable", "request timed out")],
            (False, "request timed out"),
            False,
        ),
        (
            [("ready", "")],
            (False, "two-minute 1m replay timed out after 5s"),
            True,
        ),
    ],
)
def test_history_probe_has_bounded_replay_and_reaps_worker(
    monkeypatch, live_fixtures, messages, expected, terminated
):
    from types import SimpleNamespace

    deadlines = []
    state = {"alive": True, "terminated": False, "closed": False}

    class Connection:
        def poll(self, timeout):
            deadlines.append(timeout)
            return bool(messages)

        def recv(self):
            return messages.pop(0)

        def close(self):
            pass

    class Process:
        def __init__(self, *, target, args):
            assert target is live_fixtures._history_probe_worker

        def start(self):
            pass

        def join(self, *, timeout):
            if not terminated:
                state["alive"] = False

        def is_alive(self):
            return state["alive"]

        def terminate(self):
            state["terminated"] = True
            state["alive"] = False

        def kill(self):
            raise AssertionError("terminate should have reaped the worker")

        def close(self):
            state["closed"] = True

    context = SimpleNamespace(
        Pipe=lambda **kwargs: (Connection(), Connection()), Process=Process
    )
    monkeypatch.setattr(
        live_fixtures.multiprocessing, "get_context", lambda mode: context
    )
    assert live_fixtures._history_probe_result() == expected
    assert deadlines == [45, 5]
    assert state == {"alive": False, "terminated": terminated, "closed": True}


def test_history_fixture_skips_with_probe_reason(monkeypatch, live_fixtures):
    monkeypatch.setattr(
        live_fixtures, "_history_probe_result", lambda: (False, "request timed out")
    )
    with pytest.raises(
        pytest.skip.Exception,
        match="history plant unavailable on this account: request timed out",
    ):
        inspect.unwrap(live_fixtures.live_history_available)(True)


def _offline_probe_worker(connection):
    connection.send(("ready", ""))
    connection.send(("available", ""))
    connection.close()


def test_history_probe_spawn_transport_offline(monkeypatch, live_fixtures):
    monkeypatch.setattr(live_fixtures, "_history_probe_worker", _offline_probe_worker)
    assert live_fixtures._history_probe_result() == (True, "")
