"""Unit tests for pure-Python ``rithmic_gateway`` client + binary resolution."""

from __future__ import annotations

import contextlib
import os
import socket
import struct
import threading
import time
from pathlib import Path

import pytest
from rithmic_gateway.binary import (
    GatewayBinaryError,
    _wheel_bin,
    resolve_gateway_bin,
)
from rithmic_gateway.client import GatewayClient, GatewayError
from rithmic_gateway.config import (
    GatewayConfig,
    GatewayConfigError,
    default_unix_path,
    parse_listen_url,
)
from rithmic_gateway.framing import encode_frame
from rithmic_gateway.v1 import session_pb2 as pb


def test_parse_listen_rejects_tls() -> None:
    with pytest.raises(GatewayConfigError, match="not implemented"):
        parse_listen_url("tls://0.0.0.0:7600")


def test_parse_listen_unix() -> None:
    assert parse_listen_url("unix:///tmp/rithmic.sock") == "/tmp/rithmic.sock"


def test_default_unix_path_stable() -> None:
    a = default_unix_path("u", "LucidTrading", "wss://rprotocol.rithmic.com:443")
    b = default_unix_path("u", "LucidTrading", "wss://rprotocol.rithmic.com:443")
    assert a == b
    assert a.endswith(".sock")


def test_resolve_bin_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RITHMIC_GATEWAY_BIN", raising=False)
    monkeypatch.delenv("CARGO_TARGET_DIR", raising=False)
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setattr("rithmic_gateway.binary._checkout_root", lambda _start: None)
    with pytest.raises(GatewayBinaryError, match="RITHMIC_GATEWAY_BIN"):
        resolve_gateway_bin(None)


def test_the_dev_fallback_never_searches_the_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``target/release/rithmic-gateway`` under an untrusted cwd is never run."""
    planted = tmp_path / "target" / "release" / "rithmic-gateway"
    planted.parent.mkdir(parents=True)
    planted.write_text("#!/bin/sh\nexit 0\n")
    planted.chmod(0o755)
    (tmp_path / "Cargo.toml").write_text("[workspace]\n")
    (tmp_path / "crates" / "rithmic-gateway").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("RITHMIC_GATEWAY_BIN", raising=False)
    monkeypatch.delenv("CARGO_TARGET_DIR", raising=False)
    monkeypatch.setenv("PATH", str(tmp_path / "nobin"))
    monkeypatch.setattr("rithmic_gateway.binary._wheel_bin", lambda: None)
    monkeypatch.setattr("rithmic_gateway.binary._checkout_root", lambda _start: None)
    with pytest.raises(GatewayBinaryError):
        resolve_gateway_bin(None)


def test_resolve_bin_explicit_missing(tmp_path: Path) -> None:
    with pytest.raises(GatewayBinaryError, match="not found"):
        resolve_gateway_bin(str(tmp_path / "missing-bin"))


def test_resolve_bin_prefers_the_wheel_binary_over_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The binary the wheel installed must win over PATH / target fallback."""
    monkeypatch.delenv("RITHMIC_GATEWAY_BIN", raising=False)
    monkeypatch.delenv("CARGO_TARGET_DIR", raising=False)
    bundled = tmp_path / "bundled" / "rithmic-gateway"
    bundled.parent.mkdir(parents=True)
    bundled.write_text("#!/bin/sh\nexit 0\n")
    bundled.chmod(0o755)
    monkeypatch.setattr("rithmic_gateway.binary._wheel_bin", lambda: str(bundled))
    # PATH points at an empty dir with no binary; target fallback also empty.
    monkeypatch.setenv("PATH", str(tmp_path / "nobin"))
    (tmp_path / "nobin").mkdir(exist_ok=True)
    monkeypatch.setattr("rithmic_gateway.binary._checkout_root", lambda _start: None)
    assert resolve_gateway_bin(None) == str(bundled.resolve())


def test_resolve_bin_prefers_explicit_and_env_over_the_wheel_binary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """explicit and RITHMIC_GATEWAY_BIN must still override the wheel binary."""
    bundled = tmp_path / "bundled" / "rithmic-gateway"
    bundled.parent.mkdir(parents=True)
    bundled.write_text("x")
    bundled.chmod(0o755)
    monkeypatch.setattr("rithmic_gateway.binary._wheel_bin", lambda: str(bundled))

    env_bin = tmp_path / "env-bin" / "rithmic-gateway"
    env_bin.parent.mkdir(parents=True)
    env_bin.write_text("x")
    env_bin.chmod(0o755)
    monkeypatch.setenv("RITHMIC_GATEWAY_BIN", str(env_bin))
    assert resolve_gateway_bin(None) == str(env_bin.resolve())

    explicit = tmp_path / "explicit" / "rithmic-gateway"
    explicit.parent.mkdir(parents=True)
    explicit.write_text("x")
    explicit.chmod(0o755)
    assert resolve_gateway_bin(str(explicit)) == str(explicit.resolve())


def _scripts_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the environment's scripts dir (where a wheel installs its binary)
    at ``tmp_path/scripts``."""
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    monkeypatch.setattr(
        "rithmic_gateway.binary.sysconfig.get_path", lambda name: str(scripts)
    )
    monkeypatch.setattr(
        "rithmic_gateway.binary.sys.executable", str(scripts / "python")
    )
    return scripts


def test_wheel_bin_requires_executable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_wheel_bin() returns None unless the installed binary is executable."""
    binary = _scripts_dir(tmp_path, monkeypatch) / "rithmic-gateway"
    binary.write_text("#!/bin/sh\n")
    assert _wheel_bin() is None
    binary.chmod(0o755)
    assert _wheel_bin() == str(binary.resolve())


def test_client_handshake_auth_failed() -> None:
    sock = Path(f"/tmp/rgw-auth-{os.getpid()}.sock")
    if sock.exists():
        sock.unlink()

    def _serve() -> None:
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(sock))
        server.listen(1)
        server.settimeout(5.0)
        conn, _ = server.accept()
        with conn:
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            _ = conn.recv(n)
            err = pb.Frame(
                error=pb.ErrorResponse(
                    code="auth_failed", message="auth_token rejected"
                )
            )
            conn.sendall(encode_frame(err.SerializeToString()))
        server.close()

    threading.Thread(target=_serve, daemon=True).start()
    time.sleep(0.05)
    cfg = GatewayConfig(
        user="u",
        system_name="LucidTrading",
        url="wss://example",
        listen=f"unix://{sock}",
        attest_flock=False,
        auth_token="bad",
    )
    client = GatewayClient(cfg, rpc_timeout_sec=2.0)
    try:
        with pytest.raises(GatewayError, match="auth_failed"):
            client.connect()
    finally:
        if sock.exists():
            sock.unlink()


def test_load_ticks_adds_trade_price_aliases() -> None:
    sock = Path(f"/tmp/rgw-ticks-{os.getpid()}.sock")
    if sock.exists():
        sock.unlink()

    def _serve() -> None:
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(sock))
        server.listen(1)
        conn, _ = server.accept()
        with conn:
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            _ = conn.recv(n)  # handshake
            conn.sendall(
                encode_frame(
                    pb.Frame(
                        ready=pb.Ready(scopes=["md", "history"])
                    ).SerializeToString()
                )
            )
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            req = pb.Frame()
            req.ParseFromString(conn.recv(n))
            tick = pb.HistoryTick(
                symbol="NQ",
                exchange="CME",
                close_price=1.5,
                num_trades=3,
                ssboe=1,
                usecs=0,
            )
            resp = pb.Frame(
                request_id=req.request_id,
                load_ticks_response=pb.LoadTicksResponse(ticks=[tick]),
            )
            conn.sendall(encode_frame(resp.SerializeToString()))
        server.close()

    threading.Thread(target=_serve, daemon=True).start()
    time.sleep(0.05)
    cfg = GatewayConfig(
        user="u",
        system_name="LucidTrading",
        url="wss://example",
        listen=f"unix://{sock}",
        attest_flock=False,
    )
    client = GatewayClient(cfg, rpc_timeout_sec=2.0)
    try:
        client.connect()
        rows = client.load_ticks("NQ", "CME", 1, 2)
        assert rows[0]["trade_price"] == pytest.approx(1.5)
        assert int(rows[0]["trade_size"]) == 3
        client.disconnect()
    finally:
        if sock.exists():
            sock.unlink()


def _serve_ready(
    sock_path: Path, *, trading: bool = False, cancel_all: bool = False
) -> None:
    """One-shot mock parent: Handshake → Ready, then gate place/cancel_all."""
    if sock_path.exists():
        sock_path.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock_path))
    server.listen(1)
    server.settimeout(5.0)
    ready_evt = threading.Event()
    ready_evt.set()  # bound+listening before clients dial

    def _run() -> None:
        try:
            conn, _ = server.accept()
        except Exception:
            server.close()
            return
        with conn:
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            payload = conn.recv(n)
            frame = pb.Frame()
            frame.ParseFromString(payload)
            assert frame.WhichOneof("body") == "handshake"
            # Handshake must not carry a password field on the schema.
            assert "password" not in [f.name for f in frame.handshake.DESCRIPTOR.fields]
            ready = pb.Ready(
                scopes=["md", "history", "pnl"] + (["trade"] if trading else []),
                trading_enabled=trading,
                cancel_all_enabled=cancel_all,
            )
            conn.sendall(encode_frame(pb.Frame(ready=ready).SerializeToString()))
            # One RPC
            header = conn.recv(4)
            if not header:
                return
            (n,) = struct.unpack("!I", header)
            payload = conn.recv(n)
            req = pb.Frame()
            req.ParseFromString(payload)
            which = req.WhichOneof("body")
            if which == "place_order" and not trading:
                resp = pb.Frame(
                    request_id=req.request_id,
                    error=pb.ErrorResponse(
                        code="trading_disabled",
                        message="place_order denied: parent trading disabled",
                    ),
                )
            elif which == "cancel_all_orders" and not cancel_all:
                resp = pb.Frame(
                    request_id=req.request_id,
                    error=pb.ErrorResponse(
                        code="cancel_all_denied",
                        message="cancel_all denied",
                    ),
                )
            else:
                resp = pb.Frame(request_id=req.request_id, ack=pb.Ack())
            conn.sendall(encode_frame(resp.SerializeToString()))
        server.close()

    threading.Thread(target=_run, daemon=True).start()
    assert ready_evt.wait(1.0)


def test_client_handshake_ready_and_place_denied() -> None:
    sock = Path(f"/tmp/rgw-test-{os.getpid()}-a.sock")
    try:
        _serve_ready(sock, trading=False)
        cfg = GatewayConfig(
            user="u",
            system_name="LucidTrading",
            url="wss://example",
            listen=f"unix://{sock}",
            attest_flock=False,
        )
        client = GatewayClient(cfg)
        client.connect()
        assert "md" in client.scopes
        assert client.trading_enabled is False
        with pytest.raises(GatewayError) as ei:
            client.place_order("NQ", "CME", "BUY", "MARKET", 1)
        assert ei.value.code == "trading_disabled"
        client.disconnect()
    finally:
        if sock.exists():
            sock.unlink()


def test_client_cancel_all_denied() -> None:
    sock = Path(f"/tmp/rgw-test-{os.getpid()}-b.sock")
    try:
        _serve_ready(sock, cancel_all=False)
        cfg = GatewayConfig(
            user="u",
            system_name="LucidTrading",
            url="wss://example",
            listen=f"unix://{sock}",
            attest_flock=False,
        )
        client = GatewayClient(cfg)
        client.connect()
        with pytest.raises(GatewayError) as ei:
            client.cancel_all_orders()
        assert ei.value.code == "cancel_all_denied"
    finally:
        if sock.exists():
            sock.unlink()


def _serve_event_then_ack(sock_path: Path) -> None:
    """Push an Event before the correlated Ack to exercise RPC demux."""
    if sock_path.exists():
        sock_path.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock_path))
    server.listen(1)
    server.settimeout(5.0)

    def _run() -> None:
        try:
            conn, _ = server.accept()
        except Exception:
            server.close()
            return
        with conn:
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            payload = conn.recv(n)
            frame = pb.Frame()
            frame.ParseFromString(payload)
            assert frame.WhichOneof("body") == "handshake"
            conn.sendall(
                encode_frame(
                    pb.Frame(
                        ready=pb.Ready(
                            scopes=["md"],
                            trading_enabled=False,
                            cancel_all_enabled=False,
                        )
                    ).SerializeToString()
                )
            )
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            payload = conn.recv(n)
            req = pb.Frame()
            req.ParseFromString(payload)
            assert req.WhichOneof("body") == "subscribe"
            # Intervening event (must not be treated as the RPC reply).
            evt = pb.Frame(
                request_id=0,
                event=pb.Event(
                    last_trade=pb.LastTrade(
                        symbol="NQ", exchange="CME", trade_price=1.0
                    )
                ),
            )
            conn.sendall(encode_frame(evt.SerializeToString()))
            ack = pb.Frame(request_id=req.request_id, ack=pb.Ack())
            conn.sendall(encode_frame(ack.SerializeToString()))
        server.close()

    threading.Thread(target=_run, daemon=True).start()


def test_default_unix_path_rust_parity() -> None:
    """Must match
    crates/rithmic-gateway listen::tests::default_unix_path_matches_python_fnv_fixture.
    """
    path = default_unix_path(
        "alice", "LucidTrading", "wss://rprotocol.rithmic.com:443", "Live"
    )
    assert path.endswith("rgw-13146466402466778522.sock")
    assert len(path.encode()) <= 103


def test_live_demo_paths_differ() -> None:
    live = default_unix_path("u", "LucidTrading", "wss://x", "Live")
    demo = default_unix_path("u", "LucidTrading", "wss://x", "Demo")
    assert live != demo


def test_auth_token_omitted_from_repr() -> None:
    cfg = GatewayConfig(
        user="u",
        system_name="LucidTrading",
        url="wss://example",
        auth_token="super-secret-token",
        attest_flock=False,
        listen="unix:///tmp/rgw-repr.sock",
    )
    text = repr(cfg)
    assert "super-secret-token" not in text
    assert "auth_token" not in text


def test_client_rpc_demuxes_intervening_events() -> None:
    sock = Path(f"/tmp/rgw-test-{os.getpid()}-demux.sock")
    try:
        _serve_event_then_ack(sock)
        cfg = GatewayConfig(
            user="u",
            system_name="LucidTrading",
            url="wss://example",
            listen=f"unix://{sock}",
            attest_flock=False,
        )
        client = GatewayClient(cfg, rpc_timeout_sec=5.0)
        client.connect()
        client.subscribe("NQ", "CME")
        evt = client.poll_event(timeout_ms=100)
        assert evt is not None
        assert evt["type"] == "last_trade"
        assert evt["symbol"] == "NQ"
        client.disconnect()
    finally:
        if sock.exists():
            sock.unlink()


def _serve_multi_event_wrong_error_then_ack(sock_path: Path) -> None:
    if sock_path.exists():
        sock_path.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock_path))
    server.listen(1)
    server.settimeout(5.0)

    def _run() -> None:
        try:
            conn, _ = server.accept()
        except Exception:
            server.close()
            return
        with conn:
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            payload = conn.recv(n)
            frame = pb.Frame()
            frame.ParseFromString(payload)
            assert frame.WhichOneof("body") == "handshake"
            conn.sendall(
                encode_frame(
                    pb.Frame(
                        ready=pb.Ready(
                            scopes=["md"],
                            trading_enabled=False,
                            cancel_all_enabled=False,
                        )
                    ).SerializeToString()
                )
            )
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            payload = conn.recv(n)
            req = pb.Frame()
            req.ParseFromString(payload)
            assert req.WhichOneof("body") == "subscribe"
            # Multiple intervening events + a stale error for another rid.
            for price in (1.0, 2.0):
                evt = pb.Frame(
                    request_id=0,
                    event=pb.Event(
                        last_trade=pb.LastTrade(
                            symbol="NQ", exchange="CME", trade_price=price
                        )
                    ),
                )
                conn.sendall(encode_frame(evt.SerializeToString()))
            stale_err = pb.Frame(
                request_id=999,
                error=pb.ErrorResponse(code="stale", message="other rpc"),
            )
            conn.sendall(encode_frame(stale_err.SerializeToString()))
            # Also push pnl/order/history events for poll routing.
            conn.sendall(
                encode_frame(
                    pb.Frame(
                        request_id=0,
                        event=pb.Event(account_pnl=pb.AccountPnl(account_id="A1")),
                    ).SerializeToString()
                )
            )
            conn.sendall(
                encode_frame(
                    pb.Frame(
                        request_id=0,
                        event=pb.Event(
                            order_notification=pb.OrderNotification(source="order")
                        ),
                    ).SerializeToString()
                )
            )
            conn.sendall(
                encode_frame(
                    pb.Frame(
                        request_id=0,
                        event=pb.Event(
                            time_bar=pb.HistoryBar(symbol="NQ", exchange="CME")
                        ),
                    ).SerializeToString()
                )
            )
            ack = pb.Frame(request_id=req.request_id, ack=pb.Ack())
            conn.sendall(encode_frame(ack.SerializeToString()))
        server.close()

    threading.Thread(target=_run, daemon=True).start()


def test_client_rpc_rejects_uncorrelated_error_and_routes_polls() -> None:
    sock = Path(f"/tmp/rgw-test-{os.getpid()}-demux2.sock")
    try:
        _serve_multi_event_wrong_error_then_ack(sock)
        cfg = GatewayConfig(
            user="u",
            system_name="LucidTrading",
            url="wss://example",
            listen=f"unix://{sock}",
            attest_flock=False,
        )
        client = GatewayClient(cfg, rpc_timeout_sec=5.0)
        client.connect()
        with pytest.raises(GatewayError) as ei:
            client.subscribe("NQ", "CME")
        assert ei.value.code == "protocol"
        assert "999" in str(ei.value)
        # Connection may still have buffered events from before the raise —
        # reconnect for clean poll routing check via a second mock.
        client.disconnect()
    finally:
        if sock.exists():
            sock.unlink()


def _serve_push_typed_events(sock_path: Path) -> None:
    if sock_path.exists():
        sock_path.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock_path))
    server.listen(1)
    server.settimeout(5.0)

    def _run() -> None:
        try:
            conn, _ = server.accept()
        except Exception:
            server.close()
            return
        with conn:
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            payload = conn.recv(n)
            frame = pb.Frame()
            frame.ParseFromString(payload)
            assert frame.WhichOneof("body") == "handshake"
            conn.sendall(
                encode_frame(
                    pb.Frame(
                        ready=pb.Ready(
                            scopes=["md"],
                            trading_enabled=False,
                            cancel_all_enabled=False,
                        )
                    ).SerializeToString()
                )
            )
            # After handshake, client will poll — push typed events.
            for frame_bytes in (
                pb.Frame(
                    event=pb.Event(last_trade=pb.LastTrade(symbol="NQ", exchange="CME"))
                ).SerializeToString(),
                pb.Frame(
                    event=pb.Event(account_pnl=pb.AccountPnl(account_id="A1"))
                ).SerializeToString(),
                pb.Frame(
                    event=pb.Event(order_notification=pb.OrderNotification(source="o"))
                ).SerializeToString(),
                pb.Frame(
                    event=pb.Event(time_bar=pb.HistoryBar(symbol="NQ", exchange="CME"))
                ).SerializeToString(),
            ):
                conn.sendall(encode_frame(frame_bytes))
            # Keep connection open briefly for polls.
            with contextlib.suppress(Exception):
                conn.recv(1)
        server.close()

    threading.Thread(target=_run, daemon=True).start()


def test_poll_routes_md_pnl_order_history() -> None:
    sock = Path(f"/tmp/rgw-test-{os.getpid()}-poll.sock")
    try:
        _serve_push_typed_events(sock)
        cfg = GatewayConfig(
            user="u",
            system_name="LucidTrading",
            url="wss://example",
            listen=f"unix://{sock}",
            attest_flock=False,
        )
        client = GatewayClient(cfg, rpc_timeout_sec=5.0)
        client.connect()
        md = client.poll_event(timeout_ms=500)
        pnl = client.poll_pnl_event(timeout_ms=500)
        order = client.poll_order_event(timeout_ms=500)
        hist = client.poll_history_event(timeout_ms=500)
        assert md is not None and md["type"] == "last_trade"
        assert pnl is not None and pnl["type"] == "account_pnl"
        assert order is not None and order["type"] == "order_notification"
        assert hist is not None and hist["type"] == "time_bar"
        client.disconnect()
    finally:
        if sock.exists():
            sock.unlink()


def test_order_book_event_always_has_side_keys() -> None:
    from rithmic_gateway.client import _event_to_dict

    # Single-sided book: asks only — ListFields would omit bid_* without the fix.
    evt = pb.Event(
        order_book=pb.OrderBook(
            symbol="NQ",
            exchange="CME",
            ask_price=[1.0],
            ask_size=[2],
        )
    )
    out = _event_to_dict(evt)
    assert out["type"] == "order_book"
    assert out["bid_price"] == []
    assert out["bid_size"] == []
    assert list(out["ask_price"]) == [1.0]
    assert list(out["ask_size"]) == [2]


def test_a_mid_payload_timeout_nulls_sock_for_next_rpc() -> None:
    """Partial-frame timeout must yield not_connected on the next call, not OSError."""
    sock = Path(f"/tmp/rgw-test-{os.getpid()}-desync.sock")
    if sock.exists():
        sock.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock))
    server.listen(1)
    server.settimeout(5.0)

    def _run() -> None:
        try:
            conn, _ = server.accept()
        except Exception:
            server.close()
            return
        with conn:
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            conn.recv(n)
            conn.sendall(
                encode_frame(
                    pb.Frame(
                        ready=pb.Ready(
                            scopes=["md"],
                            trading_enabled=False,
                            cancel_all_enabled=False,
                        )
                    ).SerializeToString()
                )
            )
            # Accept Subscribe request, then send a partial length-prefixed frame
            # so the client times out mid-payload (a timeout, not a desync).
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            conn.recv(n)
            conn.sendall(struct.pack("!I", 100))  # claim 100 bytes
            conn.sendall(b"\x00\x01")  # only 2 bytes — then stall until client closes
            with contextlib.suppress(Exception):
                conn.recv(1)
        server.close()

    threading.Thread(target=_run, daemon=True).start()
    try:
        cfg = GatewayConfig(
            user="u",
            system_name="LucidTrading",
            url="wss://example",
            listen=f"unix://{sock}",
            attest_flock=False,
        )
        client = GatewayClient(cfg, rpc_timeout_sec=0.3)
        client.connect()
        # A reply that stalls part-way is a timeout (outcome unknown); the RPC
        # path closes the socket so the next call cannot misread the stream.
        with pytest.raises(GatewayError) as ei:
            client.subscribe("NQ", "CME")
        assert ei.value.code == "timeout"
        with pytest.raises(GatewayError) as ei2:
            client.subscribe("ES", "CME")
        assert ei2.value.code == "not_connected"
    finally:
        if sock.exists():
            sock.unlink()


def test_timeout_sec_zero_nulls_sock_for_next_rpc() -> None:
    """``timeout_sec=0`` sets non-blocking mode; treat BlockingIOError as a
    timeout and close the socket so the next RPC is not_connected instead of
    consuming the queued response (request-ID protocol error)."""
    sock = Path(f"/tmp/rgw-test-{os.getpid()}-timeout0.sock")
    if sock.exists():
        sock.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock))
    server.listen(1)
    server.settimeout(5.0)

    def _run() -> None:
        try:
            conn, _ = server.accept()
        except Exception:
            server.close()
            return
        with conn:
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            conn.recv(n)
            conn.sendall(
                encode_frame(
                    pb.Frame(
                        ready=pb.Ready(
                            scopes=["md"],
                            trading_enabled=False,
                            cancel_all_enabled=False,
                        )
                    ).SerializeToString()
                )
            )
            # Accept the RPC request, then never answer — the client's
            # non-blocking read raises BlockingIOError immediately.
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            conn.recv(n)
            with contextlib.suppress(Exception):
                conn.recv(1)
        server.close()

    threading.Thread(target=_run, daemon=True).start()
    try:
        cfg = GatewayConfig(
            user="u",
            system_name="LucidTrading",
            url="wss://example",
            listen=f"unix://{sock}",
            attest_flock=False,
        )
        client = GatewayClient(cfg, rpc_timeout_sec=0.3)
        client.connect()
        with pytest.raises(GatewayError) as ei:
            client.load_time_bars("NQ", "CME", 1, 2, rpc_timeout_sec=0)
        assert ei.value.code == "timeout"
        with pytest.raises(GatewayError) as ei2:
            client.load_time_bars("ES", "CME", 1, 2)
        assert ei2.value.code == "not_connected"
    finally:
        if sock.exists():
            sock.unlink()


def test_runtime_base_dir_happy_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rithmic_gateway.config import runtime_base_dir

    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    path = runtime_base_dir()
    assert path.endswith(f"rgw-{os.getuid()}")
    assert (os.stat(path).st_mode & 0o777) == 0o700


def test_clamp_unix_path_uses_private_dir_not_sticky_tmp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rithmic_gateway.config import _clamp_unix_path

    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    long = "/" + "x" * 120 + "/rgw-1.sock"
    clamped = _clamp_unix_path(long, 0xABCDEF01)
    assert f"/tmp/rgw-{os.getuid()}/" in clamped
    assert clamped.endswith(".sock")
    assert f"/tmp/rgw-{os.getuid()}-" not in clamped


def _serve_resolved_account(sock_path: Path, account_id: str | None) -> None:
    """One-shot mock parent: Handshake → Ready, then answer resolved_account."""
    if sock_path.exists():
        sock_path.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock_path))
    server.listen(1)
    server.settimeout(5.0)

    def _run() -> None:
        try:
            conn, _ = server.accept()
        except Exception:
            server.close()
            return
        with conn:
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            conn.recv(n)  # handshake
            conn.sendall(
                encode_frame(
                    pb.Frame(ready=pb.Ready(scopes=["md", "pnl"])).SerializeToString()
                )
            )
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            req = pb.Frame()
            req.ParseFromString(conn.recv(n))
            assert req.WhichOneof("body") == "resolved_account"
            if account_id is None:
                resp = pb.ResolvedAccountResponse()
            else:
                resp = pb.ResolvedAccountResponse(
                    account_id=account_id, fcm_id="F1", ib_id="I1"
                )
            conn.sendall(
                encode_frame(
                    pb.Frame(
                        request_id=req.request_id, resolved_account_response=resp
                    ).SerializeToString()
                )
            )
        server.close()

    threading.Thread(target=_run, daemon=True).start()


def _resolved_account_client(sock: Path) -> GatewayClient:
    cfg = GatewayConfig(
        user="u",
        system_name="LucidTrading",
        url="wss://example",
        listen=f"unix://{sock}",
        attest_flock=False,
    )
    return GatewayClient(cfg, rpc_timeout_sec=2.0)


def test_client_resolved_account_roundtrip() -> None:
    sock = Path(f"/tmp/rgw-ra-{os.getpid()}.sock")
    try:
        _serve_resolved_account(sock, account_id="A1")
        client = _resolved_account_client(sock)
        client.connect()
        assert client.resolved_account() == {
            "account_id": "A1",
            "fcm_id": "F1",
            "ib_id": "I1",
        }
        client.disconnect()
    finally:
        if sock.exists():
            sock.unlink()


def test_client_resolved_account_none_when_unresolved() -> None:
    sock = Path(f"/tmp/rgw-ra-none-{os.getpid()}.sock")
    try:
        _serve_resolved_account(sock, account_id=None)
        client = _resolved_account_client(sock)
        client.connect()
        assert client.resolved_account() is None
        client.disconnect()
    finally:
        if sock.exists():
            sock.unlink()


def test_history_denied_live_md_maps_to_capability_denied() -> None:
    sock = Path(f"/tmp/rgw-hist-deny-{os.getpid()}.sock")
    if sock.exists():
        sock.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock))
    server.listen(1)

    def _run() -> None:
        try:
            conn, _ = server.accept()
        except Exception:
            server.close()
            return
        with conn:
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            conn.recv(n)
            conn.sendall(
                encode_frame(
                    pb.Frame(
                        ready=pb.Ready(
                            scopes=["md", "history"],
                            trading_enabled=False,
                            cancel_all_enabled=False,
                            gateway_instance_id=99,
                            transport_generation=3,
                        )
                    ).SerializeToString()
                )
            )
            header = conn.recv(4)
            (n,) = struct.unpack("!I", header)
            req = pb.Frame()
            req.ParseFromString(conn.recv(n))
            conn.sendall(
                encode_frame(
                    pb.Frame(
                        request_id=req.request_id,
                        error=pb.ErrorResponse(
                            code="history_denied_live_md",
                            message="live md active",
                        ),
                    ).SerializeToString()
                )
            )
        server.close()

    threading.Thread(target=_run, daemon=True).start()
    try:
        cfg = GatewayConfig(
            user="u",
            system_name="LucidTrading",
            url="wss://example",
            listen=f"unix://{sock}",
            attest_flock=False,
        )
        client = GatewayClient(cfg, rpc_timeout_sec=2.0)
        client.connect()
        assert client.gateway_instance_id == 99
        assert client.transport_generation == 3
        with pytest.raises(GatewayError) as ei:
            client.load_time_bars("NQ", "CME", 1, 2)
        assert ei.value.code == "capability_denied"
        client.disconnect()
    finally:
        if sock.exists():
            sock.unlink()


def _bare_client() -> tuple[GatewayClient, socket.socket]:
    """A client wired to one end of a socket pair, past the handshake."""
    from collections import deque

    ours, theirs = socket.socketpair()
    client = GatewayClient.__new__(GatewayClient)
    client._sock = ours
    client._rpc_timeout_sec = 2.0
    client._io_lock = threading.RLock()
    client._pending = deque()
    client._next_id = 1
    return client, theirs


def _event_frame(symbol: str) -> bytes:
    frame = pb.Frame(
        request_id=0,
        event=pb.Event(last_trade=pb.LastTrade(symbol=symbol, exchange="CME")),
    )
    return encode_frame(frame.SerializeToString())


def test_a_poll_that_reads_half_a_frame_keeps_it_for_the_next_poll() -> None:
    client, peer = _bare_client()
    wire = _event_frame("NQU6")
    peer.sendall(wire[:5])  # header plus one byte
    assert client.poll_event(timeout_ms=0) is None
    peer.sendall(wire[5:])
    evt = client.poll_event(timeout_ms=500)
    assert evt is not None and evt["symbol"] == "NQU6", evt
    peer.sendall(_event_frame("ESZ6"))
    after = client.poll_event(timeout_ms=500)
    assert after is not None and after["symbol"] == "ESZ6", "stream still aligned"


def test_a_request_that_could_not_be_written_is_not_sent() -> None:
    client, peer = _bare_client()
    peer.close()
    with pytest.raises(GatewayError) as err:
        client._rpc(pb.Frame(cancel_order=pb.CancelOrderRequest(basket_id="B")))
    assert err.value.code == "not_sent"


def test_unpolled_events_are_bounded_and_the_loss_is_a_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rithmic_gateway.client as client_mod

    monkeypatch.setattr(client_mod, "_MAX_PENDING_EVENTS", 3)
    client, _peer = _bare_client()
    for i in range(10):
        client._queue_event({"type": "last_trade", "symbol": f"T{i}"})
    pending = list(client._pending)
    assert len(pending) <= 3, pending
    gaps = [e for e in pending if e.get("type") == "gap"]
    kept = [e for e in pending if e.get("type") != "gap"]
    # Every event is either kept or counted in the gap: none vanish.
    assert gaps == [{"type": "gap", "stream": "ticker", "skipped": 10 - len(kept)}]
    assert kept[-1]["symbol"] == "T9", "the newest events are kept"
    assert pending[0]["type"] == "gap", "the loss is reported before what followed it"
