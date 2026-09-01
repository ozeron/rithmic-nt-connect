"""Tests for gateway mux frame routing."""

from __future__ import annotations

import importlib.util
import os
import socket
import struct
import threading
import time
from pathlib import Path

import pytest
from rithmic_gateway.client import GatewayError
from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.framing import encode_frame
from rithmic_gateway.mux import GatewayMux
from rithmic_gateway.runtime import GatewayRuntimeRegistry, create_gateway_client
from rithmic_gateway.v1 import session_pb2 as pb

_shared = importlib.util.spec_from_file_location(
    "test_gateway_shared_consumers",
    Path(__file__).with_name("test_gateway_shared_consumers.py"),
)
assert _shared and _shared.loader
_shared_mod = importlib.util.module_from_spec(_shared)
_shared.loader.exec_module(_shared_mod)
_serve_shared_md_parent = _shared_mod._serve_shared_md_parent


@pytest.fixture(autouse=True)
def _reset_registry() -> None:
    GatewayRuntimeRegistry.reset_for_tests()
    yield
    GatewayRuntimeRegistry.reset_for_tests()


def _serve_md_and_order_events(sock_path: Path) -> threading.Thread:
    if sock_path.exists():
        sock_path.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock_path))
    server.listen(2)
    server.settimeout(8.0)
    ready = threading.Event()

    def _run() -> None:
        ready.set()
        try:
            conn, _ = server.accept()
        except Exception:
            server.close()
            return
        with conn:
            hs = conn.recv(4)
            (n,) = struct.unpack("!I", hs)
            conn.recv(n)
            conn.sendall(
                encode_frame(
                    pb.Frame(
                        ready=pb.Ready(
                            scopes=["md", "trade"],
                            trading_enabled=False,
                            cancel_all_enabled=False,
                            gateway_instance_id=7,
                            transport_generation=1,
                        )
                    ).SerializeToString()
                )
            )
            for _ in range(2):
                header = conn.recv(4)
                (n,) = struct.unpack("!I", header)
                req = pb.Frame()
                req.ParseFromString(conn.recv(n))
                conn.sendall(
                    encode_frame(
                        pb.Frame(
                            request_id=req.request_id, ack=pb.Ack()
                        ).SerializeToString()
                    )
                )
                which = req.WhichOneof("body")
                if which == "subscribe":
                    conn.sendall(
                        encode_frame(
                            pb.Frame(
                                event=pb.Event(
                                    last_trade=pb.LastTrade(
                                        symbol=req.subscribe.symbol,
                                        exchange=req.subscribe.exchange,
                                        trade_price=1.0,
                                        trade_size=1,
                                    )
                                )
                            ).SerializeToString()
                        )
                    )
                elif which == "subscribe_order_updates":
                    conn.sendall(
                        encode_frame(
                            pb.Frame(
                                event=pb.Event(
                                    order_notification=pb.OrderNotification(
                                        basket_id="B1",
                                        notify_type_name="submitted",
                                        symbol="NQH6",
                                        exchange="CME",
                                    )
                                )
                            ).SerializeToString()
                        )
                    )
        server.close()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    assert ready.wait(1.0)
    time.sleep(0.05)
    return thread


def test_mux_routes_md_and_exec_events_to_separate_pollers() -> None:
    sock = Path(f"/tmp/rgw-mux-route-{os.getpid()}.sock")
    try:
        _serve_md_and_order_events(sock)
        cfg = GatewayConfig(
            user="u-mux-route",
            system_name="LucidTrading",
            url="wss://example",
            listen=f"unix://{sock}",
            auto_spawn=False,
            attest_flock=False,
            spawn_timeout_sec=3.0,
        )
        md_client = create_gateway_client(cfg, mode="mux")
        exec_client = create_gateway_client(cfg, mode="mux")
        md_client.connect()
        exec_client.connect()
        md_client.subscribe("NQH6", "CME")
        exec_client.subscribe_order_updates()

        md_evt = md_client.poll_event(timeout_ms=500)
        order_evt = exec_client.poll_order_event(timeout_ms=500)

        assert md_evt is not None
        assert md_evt.get("type") == "last_trade"
        assert order_evt is not None
        assert order_evt.get("type") == "order_notification"
        assert md_client.poll_order_event(timeout_ms=0) is None
        assert exec_client.poll_event(timeout_ms=0) is None

        md_client.disconnect()
        exec_client.disconnect()
    finally:
        if sock.exists():
            sock.unlink()


def test_mux_stays_connected_when_parent_is_quiet() -> None:
    sock = Path(f"/tmp/rgw-mux-idle-{os.getpid()}.sock")
    try:
        _serve_shared_md_parent(sock, clients=2)
        cfg = GatewayConfig(
            user="u-mux-idle",
            system_name="LucidTrading",
            url="wss://example",
            listen=f"unix://{sock}",
            auto_spawn=False,
            attest_flock=False,
            spawn_timeout_sec=3.0,
        )
        client = create_gateway_client(cfg, mode="mux")
        client.connect()
        mux = client._runtime.mux
        assert mux._sock is not None
        assert mux._sock.gettimeout() is None
        time.sleep(0.15)
        assert mux.is_connected()
        client.disconnect()
    finally:
        if sock.exists():
            sock.unlink()


def test_mux_reconnect_clears_buffered_events_and_notifies_listeners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = GatewayConfig(
        user="u-mux-reconnect",
        system_name="LucidTrading",
        url="wss://example",
        auto_spawn=False,
        attest_flock=False,
    )
    mux = GatewayMux(cfg)
    mux._md_events.append({"type": "last_trade"})
    mux._transport_generation = 3
    notified: list[int] = []
    mux.add_generation_listener(notified.append)
    monkeypatch.setattr(mux, "connect", lambda: None)

    mux.reconnect()

    assert not mux._md_events
    assert notified == [3]


def test_mux_reset_ticker_never_disconnects_transport() -> None:
    sock = Path(f"/tmp/rgw-mux-reset-{os.getpid()}.sock")
    try:
        _serve_shared_md_parent(sock, clients=2)
        cfg = GatewayConfig(
            user="u-mux-reset",
            system_name="LucidTrading",
            url="wss://example",
            listen=f"unix://{sock}",
            auto_spawn=False,
            attest_flock=False,
            spawn_timeout_sec=3.0,
        )
        from rithmic_nt_connect.gateway_wire import GatewayWireSession

        client = create_gateway_client(cfg, mode="mux")
        session = GatewayWireSession(client)
        session.connect()

        calls: list[str] = []

        def _boom() -> None:
            calls.append("plant")
            raise GatewayError("plant_reset_failed", "mock")

        client.reset_ticker_plant = _boom  # type: ignore[method-assign]
        real_disconnect = client.disconnect

        def _track_disconnect() -> None:
            calls.append("disconnect")
            real_disconnect()

        client.disconnect = _track_disconnect  # type: ignore[method-assign]

        with pytest.raises(GatewayError, match="plant_reset_failed"):
            session.reset_ticker()
        assert calls == ["plant"]
        session.disconnect()
    finally:
        if sock.exists():
            sock.unlink()
