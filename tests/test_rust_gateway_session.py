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
    RecoveryFailed,
    TransportDown,
    TransportUp,
    event_from_rust,
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
    assert direct.is_direct is True
    # Direct mode owns its gates: closed until configured, opened (never closed).
    assert direct.trading_enabled is False
    assert direct.cancel_all_enabled is False
    direct.configure_gates(trading_enabled=True)
    assert (direct.trading_enabled, direct.cancel_all_enabled) == (True, False)
    direct.configure_gates(cancel_all_enabled=True)
    direct.configure_gates()  # asking for nothing closes nothing
    assert (direct.trading_enabled, direct.cancel_all_enabled) == (True, True)
    assert direct.ensure_transport_live() == 0
    assert direct.transport_connected() is True
    assert direct.transport_generation() == 0

    gateway = _lib.Session(user="u", connect_mode="gateway")
    assert gateway.is_direct is False
    assert gateway.trading_enabled is False  # nothing advertised before connect
    with pytest.raises(AttributeError):
        gateway.trading_enabled = True  # type: ignore[misc]
    # The parent owns a gateway's gates: a client cannot open them.
    with pytest.raises(RuntimeError, match="owned by the gateway parent"):
        gateway.configure_gates(trading_enabled=True)


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


def test_transport_events_are_built_for_the_adapter() -> None:
    down = event_from_rust("down", 3, 0, None)
    assert isinstance(down, TransportDown) and down.gen == 3
    assert down.fault.kind.value == "unix_eof"
    up = event_from_rust("up", 4, 0, None)
    assert isinstance(up, TransportUp) and up.gen == 4
    failed = event_from_rust("recovery_failed", 4, 8, "parent gone")
    assert isinstance(failed, RecoveryFailed)
    assert (failed.gen, failed.attempts) == (4, 8)
    assert "parent gone" in str(failed.cause)
    assert event_from_rust("nonsense", 1, 0, None) is None


class _TransportInner:
    """Stands in for the extension: one callback registration, fired by hand."""

    def __init__(self) -> None:
        self.registrations = 0
        self.callback: Any = None
        self.cancelled = 0
        self.disconnected = 0

    def subscribe_transport_events(self, callback: Any) -> Any:
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
    unsubscribe = wrapper.subscribe_transport_events(seen.append)
    assert callable(unsubscribe)

    inner.callback("down", 1, 0, None)
    inner.callback("up", 2, 0, None)
    inner.callback("recovery_failed", 2, 8, "gone")
    inner.callback("nonsense", 2, 0, None)
    assert [type(e).__name__ for e in seen] == [
        "TransportDown",
        "TransportUp",
        "RecoveryFailed",
    ]


def test_listeners_share_one_registration_and_can_detach() -> None:
    first: list[Any] = []
    second: list[Any] = []
    inner = _TransportInner()
    wrapper = _RustGatewaySession(inner)
    drop_first = wrapper.subscribe_transport_events(first.append)
    wrapper.subscribe_transport_events(second.append)
    assert inner.registrations == 1

    inner.callback("up", 1, 0, None)
    drop_first()
    drop_first()  # detaching twice is harmless
    inner.callback("up", 2, 0, None)
    assert len(first) == 1
    assert len(second) == 2


def test_the_extension_registration_is_cancelled_when_the_last_listener_leaves() -> (
    None
):
    inner = _TransportInner()
    wrapper = _RustGatewaySession(inner)
    drop_a = wrapper.subscribe_transport_events(lambda e: None)
    drop_b = wrapper.subscribe_transport_events(lambda e: None)
    drop_a()
    assert inner.cancelled == 0, "another listener is still attached"
    drop_b()
    assert inner.cancelled == 1
    drop_b()  # idempotent
    assert inner.cancelled == 1
    # A later listener registers afresh.
    wrapper.subscribe_transport_events(lambda e: None)
    assert inner.registrations == 2


def test_disconnect_cancels_the_registration_and_reaches_the_extension() -> None:
    inner = _TransportInner()
    wrapper = _RustGatewaySession(inner)
    wrapper.subscribe_transport_events(lambda e: None)
    wrapper.disconnect()
    assert (inner.cancelled, inner.disconnected) == (1, 1)


def test_the_extension_callback_does_not_keep_the_session_alive() -> None:
    """The extension's task holds its callback for the life of the process, so the
    callback must not reference the wrapper (and through it the whole session)."""
    import gc
    import weakref

    inner = _TransportInner()
    wrapper = _RustGatewaySession(inner)
    wrapper.subscribe_transport_events(lambda e: None)
    relay = inner.callback  # what the extension's task would keep
    ref = weakref.ref(wrapper)
    del wrapper
    gc.collect()
    assert ref() is None, "the relay held the session alive"
    relay("up", 1, 0, None)  # a late event after the session is gone is harmless


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
            if isinstance(event, TransportUp):
                got_up.set()

        wrapper.subscribe_transport_events(_listener)
        wrapper.subscribe("NQH6", "CME")

        parent.restart()
        assert got_up.wait(30), events
        kinds = [type(e).__name__ for _, e in events]
        assert kinds[0] == "TransportDown"
        assert "TransportUp" in kinds
        # Delivered off the main thread, without deadlocking the interpreter.
        assert all(name != threading.main_thread().name for name, _ in events)
        up = next(e for _, e in events if isinstance(e, TransportUp))
        assert up.gen >= 2
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
    assert hasattr(wrapper, "subscribe_transport_events")
