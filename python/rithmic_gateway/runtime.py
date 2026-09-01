"""Process-local gateway runtime registry (mux mode)."""

from __future__ import annotations

import os
import threading
from collections.abc import Callable
from typing import Any, ClassVar, Literal

from rithmic_gateway.client import GatewayClient
from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.mux import GatewayMux

GatewayClientMode = Literal["dual", "mux"]


def runtime_registry_key(config: GatewayConfig) -> str:
    return (
        f"{config.user}|{config.system_name}|{config.url}|"
        f"{config.env}|{config.socket_path}"
    )


def parse_gateway_client_mode(
    env: dict[str, str] | None = None,
    *,
    default: GatewayClientMode = "mux",
) -> GatewayClientMode:
    source = env if env is not None else os.environ
    raw = source.get("RITHMIC_GATEWAY_CLIENT_MODE")
    if raw is None or not str(raw).strip():
        return default
    key = str(raw).strip().lower()
    if key == "dual":
        return "dual"
    if key == "mux":
        return "mux"
    raise ValueError(
        f"invalid RITHMIC_GATEWAY_CLIENT_MODE {raw!r}; expected dual or mux"
    )


class GatewayRuntime:
    """One mux + refcount for a credential fingerprint within a process."""

    def __init__(self, config: GatewayConfig) -> None:
        self.config = config
        self.mux = GatewayMux(config)
        self._holders = 0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            self._holders += 1
            if self._holders == 1 or not self.mux.is_connected():
                try:
                    self.mux.connect()
                except Exception:
                    self._holders -= 1
                    raise

    def release(self) -> None:
        with self._lock:
            if self._holders <= 0:
                return
            self._holders -= 1
            if self._holders == 0:
                self.mux.disconnect()
                GatewayRuntimeRegistry._drop(runtime_registry_key(self.config))


class MuxGatewayClient(GatewayClient):
    """``GatewayClient`` facade over a shared ``GatewayMux``."""

    shared_runtime = True

    def __init__(self, runtime: GatewayRuntime) -> None:
        super().__init__(runtime.config)
        self._runtime = runtime
        self._mux = runtime.mux
        self._attached = False

    def register_transport_generation_listener(
        self, callback: Callable[[int], None]
    ) -> None:
        self._mux.add_generation_listener(callback)

    def connect(self) -> None:
        with self._io_lock:
            runtime = GatewayRuntimeRegistry._get_or_create(self._config)
            self._runtime = runtime
            self._mux = runtime.mux
            if self._attached:
                if self._mux.is_connected():
                    self._sync_ready_state()
                    return
                self._mux.connect()
                self._sync_ready_state()
                return
            runtime.acquire()
            self._attached = True
            self._sync_ready_state()

    def disconnect(self) -> None:
        with self._io_lock:
            if not self._attached:
                return
            self._runtime.release()
            self._attached = False
            self._pending.clear()

    def reconnect_transport(self) -> None:
        with self._io_lock:
            self._mux.reconnect()
            self._sync_ready_state()

    def _sync_ready_state(self) -> None:
        self._scopes = list(self._mux.scopes)
        self._trading_enabled = self._mux.trading_enabled
        self._cancel_all_enabled = self._mux.cancel_all_enabled
        self._gateway_instance_id = self._mux.gateway_instance_id
        self._transport_generation = self._mux.transport_generation

    def _rpc(self, frame: Any, *, timeout_sec: float | None = None) -> Any:
        with self._io_lock:
            return self._mux.rpc_unlocked(frame, timeout_sec=timeout_sec)

    def _poll_filtered(self, timeout_ms: int, predicate: Any) -> dict[str, Any] | None:
        with self._io_lock:
            return self._mux.poll_filtered(timeout_ms, predicate)


class GatewayRuntimeRegistry:
    _runtimes: ClassVar[dict[str, GatewayRuntime]] = {}
    _guard: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def _get_or_create(cls, config: GatewayConfig) -> GatewayRuntime:
        key = runtime_registry_key(config)
        with cls._guard:
            runtime = cls._runtimes.get(key)
            if runtime is None:
                runtime = GatewayRuntime(config)
                cls._runtimes[key] = runtime
            return runtime

    @classmethod
    def create_client(cls, config: GatewayConfig) -> MuxGatewayClient:
        return MuxGatewayClient(cls._get_or_create(config))

    @classmethod
    def _drop(cls, key: str) -> None:
        with cls._guard:
            cls._runtimes.pop(key, None)

    @classmethod
    def reset_for_tests(cls) -> None:
        with cls._guard:
            for runtime in list(cls._runtimes.values()):
                runtime.mux.disconnect()
            cls._runtimes.clear()


def create_gateway_client(
    config: GatewayConfig,
    *,
    mode: GatewayClientMode | str | None = None,
) -> GatewayClient:
    resolved: GatewayClientMode
    if mode == "dual":
        resolved = "dual"
    elif mode == "mux":
        resolved = "mux"
    else:
        resolved = parse_gateway_client_mode()
    if resolved == "dual":
        return GatewayClient(config)
    return GatewayRuntimeRegistry.create_client(config)
