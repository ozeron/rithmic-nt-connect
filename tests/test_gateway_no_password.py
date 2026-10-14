"""Gateway-mode clients hold no Rithmic password (openspec: simplify-gateway-modes).

Only the parent process holds ``RITHMIC_PASSWORD``; a client authenticates with
the auth token and is identified by fingerprint.
"""

from __future__ import annotations

import dataclasses
import os
import socket
import struct
import threading
from pathlib import Path

import pytest
from _gateway_parent import gateway_config_from_session
from rithmic_gateway.client import GatewayClient
from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.framing import encode_frame
from rithmic_gateway.v1 import session_pb2 as pb
from rithmic_nt_connect.config import ConfigError, ConnectMode, SessionConfig

SENTINEL = "sentinel-pw-7f3a91"


def _recv_frame_bytes(conn: socket.socket) -> bytes:
    header = conn.recv(4)
    (n,) = struct.unpack("!I", header)
    payload = b""
    while len(payload) < n:
        payload += conn.recv(n - len(payload))
    return payload


def _serve_capture(sock_path: Path, captured: list[bytes]) -> None:
    """Mock parent: record raw Handshake + one RPC frame, answer Ready / Ack."""
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
            captured.append(_recv_frame_bytes(conn))
            ready = pb.Frame(ready=pb.Ready(scopes=["md"]))
            conn.sendall(encode_frame(ready.SerializeToString()))
            raw = _recv_frame_bytes(conn)
            captured.append(raw)
            req = pb.Frame()
            req.ParseFromString(raw)
            conn.sendall(
                encode_frame(
                    pb.Frame(
                        request_id=req.request_id, ack=pb.Ack()
                    ).SerializeToString()
                )
            )
        server.close()

    threading.Thread(target=_run, daemon=True).start()


def _gateway_env(**extra: str) -> dict[str, str]:
    return {
        "RITHMIC_USER": "alice",
        "RITHMIC_CONNECT_MODE": "gateway",
        "RITHMIC_GATEWAY_LISTEN": "unix:///tmp/rgw-nopw.sock",
        **extra,
    }


# -- SessionConfig keyed on connect_mode ---------------------------------------


def test_gateway_session_config_builds_without_password() -> None:
    cfg = SessionConfig(user="alice", connect_mode=ConnectMode.GATEWAY)
    assert cfg.password is None


def test_gateway_session_config_from_env_without_password() -> None:
    cfg = SessionConfig.from_env(_gateway_env())
    assert cfg.connect_mode is ConnectMode.GATEWAY
    assert cfg.password is None


def test_gateway_session_config_discards_a_supplied_password() -> None:
    cfg = SessionConfig.from_env(_gateway_env(RITHMIC_PASSWORD=SENTINEL))
    assert cfg.password is None
    assert SENTINEL not in repr(cfg)
    assert SENTINEL not in str(cfg.to_dict(redact=False))
    direct = SessionConfig(
        user="alice", password=SENTINEL, connect_mode=ConnectMode.GATEWAY
    )
    assert direct.password is None


def test_direct_session_config_without_password_fails() -> None:
    with pytest.raises(ConfigError, match="password"):
        SessionConfig(user="alice", connect_mode=ConnectMode.DIRECT)


def test_direct_session_config_from_env_without_password_fails() -> None:
    env = _gateway_env(RITHMIC_CONNECT_MODE="direct")
    with pytest.raises(ConfigError, match="RITHMIC_PASSWORD"):
        SessionConfig.from_env(env)


def test_direct_session_config_still_redacts_password() -> None:
    cfg = SessionConfig(
        user="alice", password=SENTINEL, connect_mode=ConnectMode.DIRECT
    )
    assert SENTINEL not in repr(cfg)
    assert cfg.to_dict()["password"] == "***"


# -- gateway client surface ----------------------------------------------------


def test_gateway_config_has_no_password_field_and_repr_is_clean() -> None:
    names = {f.name for f in dataclasses.fields(GatewayConfig)}
    assert not {n for n in names if "pass" in n or n.endswith("pw")}
    cfg = GatewayConfig(
        user="alice",
        system_name="LucidTrading",
        url="wss://example",
        auth_token="tok-123",
        attest_flock=False,
    )
    assert "pass" not in repr(cfg).lower()
    assert "tok-123" not in repr(cfg)


def test_gateway_config_from_session_carries_no_password() -> None:
    session = SessionConfig.from_env(_gateway_env(RITHMIC_PASSWORD=SENTINEL))
    gcfg = gateway_config_from_session(session)
    assert SENTINEL not in repr(gcfg)
    assert SENTINEL not in str(dataclasses.asdict(gcfg))


def test_handshake_and_rpc_frames_contain_no_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RITHMIC_PASSWORD", SENTINEL)
    sock = Path(f"/tmp/rgw-nopw-{os.getpid()}.sock")
    captured: list[bytes] = []
    try:
        _serve_capture(sock, captured)
        session = SessionConfig.from_env(
            _gateway_env(
                RITHMIC_PASSWORD=SENTINEL,
                RITHMIC_GATEWAY_LISTEN=f"unix://{sock}",
            )
        )
        gcfg = gateway_config_from_session(session)
        gcfg.attest_flock = False
        client = GatewayClient(gcfg, rpc_timeout_sec=3.0)
        client.connect()
        client.subscribe("NQH6", "CME")
        client.disconnect()
    finally:
        if sock.exists():
            sock.unlink()

    assert len(captured) >= 2
    for raw in captured:
        assert SENTINEL.encode() not in raw
    handshake = pb.Frame()
    handshake.ParseFromString(captured[0])
    assert handshake.WhichOneof("body") == "handshake"
    assert "password" not in [f.name for f in handshake.handshake.DESCRIPTOR.fields]
