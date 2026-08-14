"""CI-safe tests: two gateway clients share one parent and get their own ticks."""

from __future__ import annotations

import os
import socket
import struct
import threading
import time
from pathlib import Path

import pytest

from rithmic_gateway.client import GatewayClient, GatewayError
from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.framing import encode_frame
from rithmic_gateway.v1 import session_pb2 as pb


def _read_frame(conn: socket.socket) -> pb.Frame:
    header = conn.recv(4)
    if not header or len(header) < 4:
        raise ConnectionError("eof header")
    (n,) = struct.unpack("!I", header)
    payload = b""
    while len(payload) < n:
        chunk = conn.recv(n - len(payload))
        if not chunk:
            raise ConnectionError("eof payload")
        payload += chunk
    frame = pb.Frame()
    frame.ParseFromString(payload)
    return frame


def _send(conn: socket.socket, frame: pb.Frame) -> None:
    conn.sendall(encode_frame(frame.SerializeToString()))


def _serve_shared_md_parent(sock_path: Path, *, clients: int = 2) -> threading.Thread:
    """Mock parent: N clients Handshake→Ready; Subscribe Ack; fan-out LastTrade by symbol."""
    if sock_path.exists():
        sock_path.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sock_path))
    server.listen(clients)
    server.settimeout(8.0)
    ready = threading.Event()

    def _client(conn: socket.socket) -> None:
        subscribed: set[tuple[str, str]] = set()
        try:
            hs = _read_frame(conn)
            assert hs.WhichOneof("body") == "handshake"
            _send(
                conn,
                pb.Frame(
                    ready=pb.Ready(
                        scopes=["md"], trading_enabled=False, cancel_all_enabled=False
                    )
                ),
            )
            # Drain RPCs; after each Subscribe, push a matching LastTrade.
            deadline = time.time() + 6.0
            while time.time() < deadline:
                conn.settimeout(0.5)
                try:
                    req = _read_frame(conn)
                except (TimeoutError, socket.timeout, ConnectionError, OSError):
                    # Also push any pending fan-out periodically.
                    continue
                which = req.WhichOneof("body")
                if which == "get_front_month":
                    root = req.get_front_month.symbol
                    _send(
                        conn,
                        pb.Frame(
                            request_id=req.request_id,
                            front_month_response=pb.FrontMonthResponse(
                                symbol=root,
                                exchange=req.get_front_month.exchange,
                                trading_symbol=f"{root}H6",
                            ),
                        ),
                    )
                elif which == "subscribe":
                    key = (req.subscribe.symbol, req.subscribe.exchange)
                    subscribed.add(key)
                    _send(conn, pb.Frame(request_id=req.request_id, ack=pb.Ack()))
                    for _ in range(3):
                        _send(
                            conn,
                            pb.Frame(
                                event=pb.Event(
                                    last_trade=pb.LastTrade(
                                        symbol=key[0],
                                        exchange=key[1],
                                        trade_price=1.0,
                                        trade_size=1,
                                    )
                                )
                            ),
                        )
                elif which == "disconnect":
                    _send(conn, pb.Frame(request_id=req.request_id, ack=pb.Ack()))
                    return
                else:
                    _send(conn, pb.Frame(request_id=req.request_id, ack=pb.Ack()))
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _run() -> None:
        ready.set()
        threads: list[threading.Thread] = []
        try:
            for _ in range(clients):
                try:
                    conn, _ = server.accept()
                except Exception:
                    break
                t = threading.Thread(target=_client, args=(conn,), daemon=True)
                t.start()
                threads.append(t)
            for t in threads:
                t.join(timeout=8.0)
        finally:
            server.close()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    assert ready.wait(1.0)
    # Brief pause so listen() is fully up for dialers.
    time.sleep(0.05)
    return thread


def test_two_clients_share_one_parent_and_receive_own_ticks() -> None:
    sock = Path(f"/tmp/rgw-shared-{os.getpid()}.sock")
    try:
        _serve_shared_md_parent(sock, clients=2)
        listen = f"unix://{sock}"
        cfg_a = GatewayConfig(
            user="u",
            system_name="LucidTrading",
            url="wss://example",
            listen=listen,
            auto_spawn=False,
            attest_flock=False,
        )
        cfg_b = GatewayConfig(
            user="u",
            system_name="LucidTrading",
            url="wss://example",
            listen=listen,
            auto_spawn=False,
            attest_flock=False,
        )
        a = GatewayClient(cfg_a, rpc_timeout_sec=5.0)
        b = GatewayClient(cfg_b, rpc_timeout_sec=5.0)
        a.connect()
        b.connect()

        front_a = a.get_front_month("NQ", "CME")
        front_b = b.get_front_month("MNQ", "CME")
        assert front_a.get("trading_symbol") == "NQH6"
        assert front_b.get("trading_symbol") == "MNQH6"

        a.subscribe("NQH6", "CME")
        b.subscribe("MNQH6", "CME")

        ticks_a: list[dict] = []
        ticks_b: list[dict] = []
        deadline = time.time() + 3.0
        while time.time() < deadline and (len(ticks_a) < 3 or len(ticks_b) < 3):
            if len(ticks_a) < 3:
                ev = a.poll_event(timeout_ms=100)
                if ev is not None:
                    ticks_a.append(ev)
            if len(ticks_b) < 3:
                ev = b.poll_event(timeout_ms=100)
                if ev is not None:
                    ticks_b.append(ev)

        assert len(ticks_a) >= 3
        assert len(ticks_b) >= 3
        assert all(t.get("symbol") == "NQH6" for t in ticks_a)
        assert all(t.get("symbol") == "MNQH6" for t in ticks_b)
        # Cross-check: A must not see MNQ ticks (mock only pushes subscribed symbol).
        assert not any(t.get("symbol") == "MNQH6" for t in ticks_a)
        assert not any(t.get("symbol") == "NQH6" for t in ticks_b)

        a.disconnect()
        b.disconnect()
    finally:
        if sock.exists():
            sock.unlink()


def test_second_connect_dials_existing_socket_without_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the listen path already accepts, connect must not call spawn_gateway."""
    sock = Path(f"/tmp/rgw-nosspawn-{os.getpid()}.sock")
    spawn_calls: list[object] = []

    def _boom(_cfg: GatewayConfig) -> object:
        spawn_calls.append(1)
        raise AssertionError("spawn_gateway should not run when socket exists")

    monkeypatch.setattr("rithmic_gateway.client.spawn_gateway", _boom)
    try:
        _serve_shared_md_parent(sock, clients=2)
        cfg = GatewayConfig(
            user="u",
            system_name="LucidTrading",
            url="wss://example",
            listen=f"unix://{sock}",
            auto_spawn=True,
            attest_flock=False,
        )
        first = GatewayClient(cfg, rpc_timeout_sec=5.0)
        second = GatewayClient(cfg, rpc_timeout_sec=5.0)
        first.connect()
        second.connect()
        assert spawn_calls == []
        first.disconnect()
        second.disconnect()
    finally:
        if sock.exists():
            sock.unlink()


def test_consumer_script_help_runs() -> None:
    """Sanity: consumer entrypoint is importable / --help exits 0."""
    import subprocess
    import sys

    root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, str(root / "scripts" / "gateway_tick_consumer.py"), "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert "tick consumer" in proc.stdout.lower() or "symbol" in proc.stdout.lower()
