"""Tests for gateway runtime registry and mux client facades."""

from __future__ import annotations

import importlib.util
import os
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
from rithmic_gateway.client import GatewayError
from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.runtime import (
    GatewayRuntimeRegistry,
    MuxGatewayClient,
    create_gateway_client,
)

_shared = importlib.util.spec_from_file_location(
    "test_gateway_shared_consumers",
    Path(__file__).with_name("test_gateway_shared_consumers.py"),
)
assert _shared and _shared.loader
_shared_mod = importlib.util.module_from_spec(_shared)
_shared.loader.exec_module(_shared_mod)
_serve_shared_md_parent = _shared_mod._serve_shared_md_parent


@pytest.fixture(autouse=True)
def _reset_registry() -> Iterator[None]:
    GatewayRuntimeRegistry.reset_for_tests()
    yield
    GatewayRuntimeRegistry.reset_for_tests()


def test_clients_share_one_runtime_by_default() -> None:
    cfg = GatewayConfig(
        user="u-runtime",
        system_name="LucidTrading",
        url="wss://example",
        attest_flock=False,
    )
    a = create_gateway_client(cfg)
    b = create_gateway_client(cfg)
    assert isinstance(a, MuxGatewayClient)
    assert isinstance(b, MuxGatewayClient)
    assert a is not b
    assert a._runtime is b._runtime


def test_acquire_rolls_back_holder_on_connect_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = GatewayConfig(
        user="u-acquire-fail",
        system_name="LucidTrading",
        url="wss://example",
        attest_flock=False,
    )
    runtime = GatewayRuntimeRegistry._get_or_create(cfg)

    def _boom() -> None:
        raise GatewayError("dial_failed", "mock connect failure")

    monkeypatch.setattr(runtime.mux, "connect", _boom)

    with pytest.raises(GatewayError, match="dial_failed"):
        GatewayRuntimeRegistry.attach(cfg)
    assert runtime._holders == 0
    assert runtime_registry_key_missing(cfg)


def runtime_registry_key_missing(cfg: GatewayConfig) -> bool:
    from rithmic_gateway.runtime import runtime_registry_key

    key = runtime_registry_key(cfg)
    return key not in GatewayRuntimeRegistry._runtimes


def test_reconnect_resolves_fresh_runtime_after_registry_drop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sock = Path(f"/tmp/rgw-runtime-reuse-{os.getpid()}.sock")
    dials: list[int] = []
    real_dial = None

    def _count_dial(self, path: str) -> None:
        dials.append(1)
        assert real_dial is not None
        real_dial(self, path)

    from rithmic_gateway import mux as mux_mod

    real_dial = mux_mod.GatewayMux._dial
    monkeypatch.setattr(mux_mod.GatewayMux, "_dial", _count_dial)

    try:
        _serve_shared_md_parent(sock, clients=2)
        cfg = GatewayConfig(
            user="u-runtime-reuse",
            system_name="LucidTrading",
            url="wss://example",
            listen=f"unix://{sock}",
            attest_flock=False,
            attach_timeout_sec=3.0,
        )
        client = create_gateway_client(cfg)
        assert isinstance(client, MuxGatewayClient)
        stale_runtime = client._runtime
        client.connect()
        client.disconnect()
        client.connect()
        assert client._runtime is not stale_runtime
        assert len(dials) == 2
        client.disconnect()
    finally:
        if sock.exists():
            sock.unlink()


def test_detached_mux_client_rejects_rpc() -> None:
    from rithmic_gateway.v1 import session_pb2 as pb

    cfg = GatewayConfig(
        user="u-detached-rpc",
        system_name="LucidTrading",
        url="wss://example",
        attest_flock=False,
    )
    client = create_gateway_client(cfg)
    assert isinstance(client, MuxGatewayClient)
    with pytest.raises(GatewayError, match=r"not_connected|connect"):
        client._rpc(pb.Frame())


def test_ten_concurrent_acquire_single_dial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sock = Path(f"/tmp/rgw-runtime10-{os.getpid()}.sock")
    dials: list[int] = []
    real_dial = None

    def _count_dial(self, path: str) -> None:
        dials.append(1)
        assert real_dial is not None
        real_dial(self, path)

    from rithmic_gateway import mux as mux_mod

    real_dial = mux_mod.GatewayMux._dial
    monkeypatch.setattr(mux_mod.GatewayMux, "_dial", _count_dial)

    try:
        _serve_shared_md_parent(sock, clients=4)
        cfg = GatewayConfig(
            user="u-runtime10",
            system_name="LucidTrading",
            url="wss://example",
            listen=f"unix://{sock}",
            attest_flock=False,
            attach_timeout_sec=3.0,
        )
        clients = [create_gateway_client(cfg) for _ in range(10)]
        errors: list[Exception] = []

        def _connect(client: MuxGatewayClient) -> None:
            try:
                client.connect()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_connect, args=(c,)) for c in clients]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)
        assert not errors
        assert len(dials) == 1
        for client in clients:
            client.disconnect()
    finally:
        if sock.exists():
            sock.unlink()
