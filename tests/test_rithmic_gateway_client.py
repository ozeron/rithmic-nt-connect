"""Unit tests for pure-Python ``rithmic_gateway`` client + spawn helpers."""

from __future__ import annotations

import os
import socket
import struct
import threading
from pathlib import Path

import pytest

from rithmic_gateway.client import GatewayClient, GatewayError
from rithmic_gateway.config import (
    GatewayConfig,
    GatewayConfigError,
    default_unix_path,
    parse_listen_url,
)
from rithmic_gateway.framing import encode_frame
from rithmic_gateway.spawn import (
    assert_no_password_in_argv,
    curated_env,
    resolve_gateway_bin,
    spawn_argv,
    SpawnError,
)
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


def test_spawn_argv_has_no_password() -> None:
    argv = spawn_argv("/usr/bin/rithmic-gateway")
    assert argv == ["/usr/bin/rithmic-gateway"]
    assert_no_password_in_argv(argv, "s3cret")


def test_curated_env_keeps_password_in_env_only() -> None:
    env = curated_env(
        {
            "RITHMIC_USER": "u",
            "RITHMIC_PASSWORD": "s3cret",
            "OTHER": "nope",
        }
    )
    assert env["RITHMIC_PASSWORD"] == "s3cret"
    assert "OTHER" not in env


def test_resolve_bin_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RITHMIC_GATEWAY_BIN", raising=False)
    monkeypatch.setenv("PATH", str(tmp_path))
    with pytest.raises(SpawnError, match="not on PATH"):
        resolve_gateway_bin(None)


def test_resolve_bin_explicit_missing(tmp_path: Path) -> None:
    with pytest.raises(SpawnError, match="not found"):
        resolve_gateway_bin(str(tmp_path / "missing-bin"))


def _serve_ready(sock_path: Path, *, trading: bool = False, cancel_all: bool = False) -> None:
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
            auto_spawn=False,
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
            auto_spawn=False,
        )
        client = GatewayClient(cfg)
        client.connect()
        with pytest.raises(GatewayError) as ei:
            client.cancel_all_orders()
        assert ei.value.code == "cancel_all_denied"
    finally:
        if sock.exists():
            sock.unlink()

