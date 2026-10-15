"""Rust gateway client behind ``create_session``: selector, wrapper, callbacks."""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest
from gateway_fake import FakeParent, build_fake_gateway, running_fake
from rithmic_nt_connect.config import ConnectMode, SessionConfig
from rithmic_nt_connect.errors import NotConnectedError
from rithmic_nt_connect.session import (
    _RustGatewaySession,
    create_rust_gateway_session,
    create_session,
)
from rithmic_nt_connect.transport import (
    Interrupted,
    RecoveryFailed,
    Resumed,
)

_lib = pytest.importorskip("rithmic_nt_connect._lib")


def _gateway_cfg(**kwargs: Any) -> SessionConfig:
    return SessionConfig(user="u-rust-gw", connect_mode=ConnectMode.GATEWAY, **kwargs)


@pytest.fixture(scope="module")
def fake_bin():
    binary = build_fake_gateway()
    if binary is None:
        pytest.skip("rithmic-gateway-fake unavailable (no cargo / build failed)")
    return binary


# -- factory ----------------------------------------------------------------------


def test_gateway_mode_is_always_the_rust_client() -> None:
    assert isinstance(create_session(_gateway_cfg()), _RustGatewaySession)


def test_each_gateway_session_is_a_fresh_facade() -> None:
    cfg = _gateway_cfg()
    assert create_session(cfg) is not create_session(cfg)


# -- the extension in gateway mode (no network) ---------------------------------------


def test_the_rust_gateway_session_needs_no_password_and_direct_still_does() -> None:
    _lib.Session(user="u", connect_mode="gateway")  # no password: fine
    with pytest.raises(ValueError, match="password"):
        _lib.Session(user="u")  # direct mode requires one
    with pytest.raises(ValueError, match="connect_mode"):
        _lib.Session(user="u", password="pw", connect_mode="pooled")


def test_gateway_mode_rejects_remote_listen_urls() -> None:
    with pytest.raises(ValueError, match="not implemented"):
        _lib.Session(
            user="u", connect_mode="gateway", gateway_listen="tcp://127.0.0.1:7600"
        )


def test_history_reads_are_served_in_gateway_mode() -> None:
    # Not refused by mode: an unconnected client fails like any other read.
    session = _lib.Session(user="u", connect_mode="gateway")
    for call in (
        lambda: session.load_fill_history(20260101, 20260102),
        lambda: session.load_order_history("20260101"),
        lambda: session.load_order_history_dates(),
    ):
        with pytest.raises(NotConnectedError):
            call()


@pytest.mark.parametrize(
    ("value", "field"),
    [
        (_lib.Gates(False, True), "trading"),
        (_lib.Interrupted(1, "offline"), "generation"),
        (_lib.Resumed(2), "generation"),
        (_lib.RecoveryFailed(1, 3, "exhausted"), "attempts"),
    ],
)
def test_frozen_session_values_are_extension_types(value: Any, field: str) -> None:
    from rithmic_nt_connect import transport

    assert transport.Interrupted is _lib.Interrupted
    assert transport.Resumed is _lib.Resumed
    assert transport.RecoveryFailed is _lib.RecoveryFailed
    assert transport.TransportDown is _lib.Interrupted
    assert transport.TransportUp is _lib.Resumed
    with pytest.raises(AttributeError):
        setattr(value, field, 42)


def test_gates_are_closed_by_default_in_both_modes_and_only_direct_can_open_them() -> (
    None
):
    direct = _lib.Session(user="u", password="pw")
    gates = direct.open_gates(trading=False, cancel_all=False)
    assert not gates.trading and not gates.cancel_all
    gates = direct.open_gates(trading=True, cancel_all=False)
    assert gates.trading and not gates.cancel_all
    gates = direct.open_gates(trading=False, cancel_all=True)
    assert gates.trading and gates.cancel_all
    gates = direct.open_gates(trading=False, cancel_all=False)
    assert gates.trading and gates.cancel_all

    gateway = _lib.Session(user="u", connect_mode="gateway")
    gates = gateway.open_gates(trading=True, cancel_all=True)
    assert not gates.trading and not gates.cancel_all


def test_a_gateway_session_before_connect_is_not_connected() -> None:
    session = _lib.Session(user="u", connect_mode="gateway")
    with pytest.raises(_lib.NotConnectedError):
        session.subscribe("NQH6", "CME")
    with pytest.raises(_lib.NotConnectedError):
        session.poll_event()


def test_a_missing_parent_is_a_typed_gateway_error_with_the_launcher_command(
    tmp_path: Any,
) -> None:
    session = _lib.Session(
        user="u-missing-parent",
        connect_mode="gateway",
        gateway_listen=f"unix:///tmp/rgw-missing-{id(tmp_path)}.sock",
    )
    with pytest.raises(_lib.GatewayError) as err:
        session.connect()
    assert err.value.code == "parent_not_running"
    assert "python -m rithmic_gateway" in str(err.value)


# -- the wrapper -----------------------------------------------------------------


def test_the_wrapper_delegates_everything_else_to_the_extension() -> None:
    class _Inner:
        trading_enabled = False

        def subscribe(self, symbol: str, exchange: str) -> str:
            return f"{symbol}.{exchange}"

    wrapper = _RustGatewaySession(_Inner())
    assert wrapper.subscribe("NQ", "CME") == "NQ.CME"  # type: ignore[attr-defined]
    # The adapter's pre-send gate reads exactly this attribute.
    assert getattr(wrapper, "trading_enabled", True) is False


@pytest.mark.parametrize("wrapped", [False, True], ids=["extension", "wrapper"])
def test_two_session_listeners_can_cancel_independently(
    tmp_path: Any, wrapped: bool
) -> None:
    inner: Any = _lib.Session.fake(lock_dir=str(tmp_path))
    session = _RustGatewaySession(inner) if wrapped else inner
    session.connect()
    session.subscribe("NQH6", "CME")
    first: list[Any] = []
    second: list[Any] = []
    got_first = threading.Event()
    got_second = threading.Event()

    def listener_first(event: Any) -> None:
        first.append(event)
        if isinstance(event, Resumed):
            got_first.set()

    def listener_second(event: Any) -> None:
        second.append(event)
        if isinstance(event, Resumed):
            got_second.set()

    sub_a = session.subscribe_session_events(listener_first)
    sub_b = session.subscribe_session_events(listener_second)
    inner.script.fail_stream("ticker")
    assert got_first.wait(3) and got_second.wait(3)
    assert [type(e).__name__ for e in first] == ["Interrupted", "Resumed"]
    assert [type(e).__name__ for e in second] == ["Interrupted", "Resumed"]
    sub_a.cancel()
    sub_a.cancel()
    got_second.clear()
    inner.script.fail_stream("ticker")
    assert got_second.wait(3)
    assert len(first) == 2
    assert len(second) == 4
    sub_b.cancel()
    session.disconnect()


def test_a_direct_listener_can_make_a_blocking_session_call(tmp_path: Any) -> None:
    session: Any = _lib.Session.fake(lock_dir=str(tmp_path))
    session.connect()
    session.subscribe("NQH6", "CME")
    done = threading.Event()
    seen: list[Any] = []
    errors: list[BaseException] = []

    def listener(event: Any) -> None:
        if isinstance(event, Resumed):
            try:
                seen.append(
                    (
                        threading.current_thread().name,
                        session.next_events("ticker", timeout_ms=0),
                    )
                )
            except BaseException as exc:  # NOSONAR - includes PyO3 runtime panics
                errors.append(exc)
            finally:
                done.set()

    subscription = session.subscribe_session_events(listener)
    session.script.fail_stream("ticker")
    assert done.wait(3)
    assert not errors
    assert seen[0][0] != threading.main_thread().name
    subscription.cancel()
    session.disconnect()


def test_cancelled_listener_does_not_keep_the_session_alive(tmp_path: Any) -> None:
    inner: Any = _lib.Session.fake(lock_dir=str(tmp_path))
    wrapper = _RustGatewaySession(inner)
    wrapper.connect()
    script = inner.script
    subscription = wrapper.subscribe_session_events(lambda event: None)
    subscription.cancel()
    del wrapper, inner
    import gc

    gc.collect()
    deadline = time.monotonic() + 3
    while "disconnect" not in script.calls():
        assert time.monotonic() < deadline
        time.sleep(0.005)


def test_recovery_exhaustion_delivers_failure_and_registry_replaces_the_host(
    tmp_path: Any,
) -> None:
    old: Any = _lib.Session.fake(lock_dir=str(tmp_path))
    old.connect()
    old.subscribe("NQH6", "CME")
    events: list[Any] = []
    failed = threading.Event()

    def listener(event: Any) -> None:
        events.append(event)
        if isinstance(event, RecoveryFailed):
            failed.set()

    subscription = old.subscribe_session_events(listener)
    for _ in range(3):
        old.script.reject_next("reset_ticker", "offline")
    old.script.fail_stream("ticker")
    assert failed.wait(3)
    assert isinstance(events[0], Interrupted)
    assert isinstance(events[-1], RecoveryFailed)
    assert events[-1].attempts == 3
    replacement: Any = _lib.Session.fake(lock_dir=str(tmp_path))
    replacement.connect()
    assert replacement.script.calls().count("connect") == 1
    with pytest.raises(RuntimeError, match="new host"):
        old.connect()
    subscription.cancel()
    old.disconnect()
    with pytest.raises(RuntimeError, match=r"detached|new host"):
        old.connect()
    replacement.disconnect()


def test_create_rust_gateway_session_never_passes_a_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class _Recorder:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(_lib, "Session", _Recorder)
    create_rust_gateway_session(_gateway_cfg(gateway_listen="unix:///tmp/x.sock"))
    assert "password" not in captured
    assert captured["connect_mode"] == "gateway"
    assert captured["gateway_listen"] == "unix:///tmp/x.sock"


def test_the_transport_callback_reaches_python_from_a_runtime_thread(
    fake_bin,
) -> None:
    """Down then Up (automatic recovery) arrive through the real extension."""
    with running_fake(fake_bin) as parent:
        wrapper: Any = create_rust_gateway_session(_session_cfg_for(parent))
        wrapper.connect()
        events: list[Any] = []
        got_up = threading.Event()

        def _listener(event: Any) -> None:
            events.append((threading.current_thread().name, event))
            if isinstance(event, Resumed):
                got_up.set()

        wrapper.subscribe_session_events(_listener)
        wrapper.subscribe("NQH6", "CME")

        parent.restart()
        assert got_up.wait(30), events
        kinds = [type(e).__name__ for _, e in events]
        assert kinds[0] == "Interrupted"
        assert "Resumed" in kinds
        # Delivered off the main thread, without deadlocking the interpreter.
        assert all(name != threading.main_thread().name for name, _ in events)
        up = next(e for _, e in events if isinstance(e, Resumed))
        assert up.generation >= 2
        wrapper.disconnect()


def _session_cfg_for(parent: FakeParent) -> SessionConfig:
    cfg = parent.config()
    return SessionConfig(
        user=cfg.user,
        connect_mode=ConnectMode.GATEWAY,
        system_name=cfg.system_name,
        url=cfg.url,
        env="Live",
        gateway_listen=cfg.listen,
    )


def test_a_gateway_session_round_trips_through_create_session(fake_bin) -> None:
    with running_fake(fake_bin) as parent:
        session = create_session(_session_cfg_for(parent))
        session.connect()
        session.subscribe("NQH6", "CME")
        parent.push_last_trades("NQH6", 1)
        deadline = time.monotonic() + 10
        event = None
        while event is None and time.monotonic() < deadline:
            event = session.poll_event(100)
        assert event is not None and event["symbol"] == "NQH6"
        assert event["type"] == "last_trade"
        session.disconnect()


def test_private_names_are_not_delegated_to_the_extension() -> None:
    wrapper = _RustGatewaySession(_lib.Session(user="unused", connect_mode="gateway"))
    assert not hasattr(wrapper, "_missing")
    assert hasattr(wrapper, "subscribe_session_events")
