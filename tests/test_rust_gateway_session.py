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


class _TransportInner:
    """Stands in for the extension: one callback registration, fired by hand."""

    def __init__(self) -> None:
        self.registrations = 0
        self.callback: Any = None
        self.cancelled = 0
        self.disconnected = 0

    def subscribe_session_events(self, callback: Any) -> Any:
        self.registrations += 1
        self.callback = callback
        owner = self

        class _Subscription:
            def cancel(self) -> None:
                owner.cancelled += 1

        return _Subscription()

    def disconnect(self) -> None:
        self.disconnected += 1


def test_the_wrapper_bridges_rust_callbacks_to_adapter_events() -> None:
    seen: list[Any] = []
    inner = _TransportInner()
    wrapper = _RustGatewaySession(inner)
    unsubscribe = wrapper.subscribe_session_events(seen.append)
    assert callable(unsubscribe.cancel)

    inner.callback(Interrupted(generation=1, cause=None))
    inner.callback(Resumed(generation=2))
    inner.callback(RecoveryFailed(generation=2, attempts=8, cause="gone"))
    assert [type(e).__name__ for e in seen] == [
        "Interrupted",
        "Resumed",
        "RecoveryFailed",
    ]


def test_listeners_share_one_registration_and_can_detach() -> None:
    first: list[Any] = []
    second: list[Any] = []
    inner = _TransportInner()
    wrapper = _RustGatewaySession(inner)
    drop_first = wrapper.subscribe_session_events(first.append)
    wrapper.subscribe_session_events(second.append)
    assert inner.registrations == 1

    inner.callback(Resumed(generation=1))
    drop_first.cancel()
    drop_first.cancel()  # detaching twice is harmless
    inner.callback(Resumed(generation=2))
    assert len(first) == 1
    assert len(second) == 2


def test_the_extension_registration_is_cancelled_when_the_last_listener_leaves() -> (
    None
):
    inner = _TransportInner()
    wrapper = _RustGatewaySession(inner)
    drop_a = wrapper.subscribe_session_events(lambda e: None)
    drop_b = wrapper.subscribe_session_events(lambda e: None)
    drop_a.cancel()
    assert inner.cancelled == 0, "another listener is still attached"
    drop_b.cancel()
    assert inner.cancelled == 1
    drop_b.cancel()  # idempotent
    assert inner.cancelled == 1
    # A later listener registers afresh.
    wrapper.subscribe_session_events(lambda e: None)
    assert inner.registrations == 2


def test_disconnect_cancels_the_registration_and_reaches_the_extension() -> None:
    inner = _TransportInner()
    wrapper = _RustGatewaySession(inner)
    wrapper.subscribe_session_events(lambda e: None)
    wrapper.disconnect()
    assert (inner.cancelled, inner.disconnected) == (1, 1)


def test_the_extension_callback_does_not_keep_the_session_alive() -> None:
    """The extension's task holds its callback for the life of the process, so the
    callback must not reference the wrapper (and through it the whole session)."""
    import gc
    import weakref

    inner = _TransportInner()
    wrapper = _RustGatewaySession(inner)
    wrapper.subscribe_session_events(lambda e: None)
    relay = inner.callback  # what the extension's task would keep
    ref = weakref.ref(wrapper)
    del wrapper
    gc.collect()
    assert ref() is None, "the relay held the session alive"
    relay(Resumed(generation=1))  # a late event after the session is gone is harmless


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
    wrapper = _RustGatewaySession(_TransportInner())
    assert not hasattr(wrapper, "_missing")
    assert hasattr(wrapper, "subscribe_session_events")
