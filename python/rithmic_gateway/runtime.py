"""Process-local gateway runtime registry (mux mode)."""

from __future__ import annotations

import contextlib
import os
import threading
from collections.abc import Callable, Mapping
from typing import Any, ClassVar, Literal

from rithmic_gateway.client import GatewayClient, GatewayError
from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.mux import DEFAULT_RPC_TIMEOUT_SEC, GatewayMux

GatewayClientMode = Literal["dual", "mux"]


def runtime_registry_key(config: GatewayConfig) -> str:
    return (
        f"{config.user}|{config.system_name}|{config.url}|"
        f"{config.env}|{config.socket_path}"
    )


def parse_gateway_client_mode(
    env: Mapping[str, str] | None = None,
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

    def __init__(
        self,
        config: GatewayConfig,
        *,
        rpc_timeout_sec: float = DEFAULT_RPC_TIMEOUT_SEC,
    ) -> None:
        self.config = config
        self.mux = GatewayMux(config, rpc_timeout_sec=rpc_timeout_sec)
        self._holders = 0
        self._lock = threading.Lock()

    def _connect_if_needed(self) -> None:
        with self._lock:
            if not self.mux.is_connected():
                self.mux.connect()


class MuxGatewayClient(GatewayClient):
    """``GatewayClient`` facade over a shared ``GatewayMux``."""

    shared_runtime = True

    def __init__(self, runtime: GatewayRuntime) -> None:
        super().__init__(
            runtime.config,
            rpc_timeout_sec=runtime.mux._rpc_timeout_sec,
        )
        self._runtime = runtime
        self._mux = runtime.mux
        self._attached = False
        self._sub_id: int | None = None

    def register_transport_generation_listener(
        self, callback: Callable[[int], None]
    ) -> None:
        self._mux.add_generation_listener(callback)

    def connect(self) -> None:
        with self._io_lock:
            if self._attached:
                if self._mux.is_connected():
                    self._sync_ready_state()
                    return
                self._runtime._connect_if_needed()
                self._sync_ready_state()
                return
            runtime = GatewayRuntimeRegistry.attach(
                self._config, rpc_timeout_sec=self._rpc_timeout_sec
            )
            self._runtime = runtime
            self._mux = runtime.mux
            self._sub_id = self._mux.register_subscriber()
            self._attached = True
            self._sync_ready_state()

    def disconnect(self) -> None:
        with self._io_lock:
            if not self._attached:
                return
            if self._sub_id is not None:
                self._mux.unregister_subscriber(self._sub_id)
                self._sub_id = None
            GatewayRuntimeRegistry.detach(self._runtime)
            self._attached = False
            self._pending.clear()

    def reconnect_transport(self) -> None:
        with self._io_lock:
            if not self._attached:
                raise GatewayError("not_connected", "call connect() first")
            self._mux.reconnect()
            self._sync_ready_state()

    def _sync_ready_state(self) -> None:
        self._scopes = list(self._mux.scopes)
        self._trading_enabled = self._mux.trading_enabled
        self._cancel_all_enabled = self._mux.cancel_all_enabled
        self._gateway_instance_id = self._mux.gateway_instance_id
        self._transport_generation = self._mux.transport_generation

    def _require_attached(self) -> None:
        if not self._attached:
            raise GatewayError("not_connected", "call connect() first")

    def _rpc(self, frame: Any, *, timeout_sec: float | None = None) -> Any:
        with self._io_lock:
            self._require_attached()
            return self._mux.rpc_unlocked(frame, timeout_sec=timeout_sec)

    def _poll_filtered(self, timeout_ms: int, predicate: Any) -> dict[str, Any] | None:
        with self._io_lock:
            self._require_attached()
            if self._sub_id is None:
                return None
            return self._mux.poll_filtered(self._sub_id, timeout_ms, predicate)


class GatewayRuntimeRegistry:
    _runtimes: ClassVar[dict[str, GatewayRuntime]] = {}
    _guard: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def attach(
        cls,
        config: GatewayConfig,
        *,
        rpc_timeout_sec: float = DEFAULT_RPC_TIMEOUT_SEC,
    ) -> GatewayRuntime:
        """Get-or-create + hold under one lock so release cannot drop mid-attach."""
        key = runtime_registry_key(config)
        with cls._guard:
            runtime = cls._runtimes.get(key)
            if runtime is None:
                runtime = GatewayRuntime(config, rpc_timeout_sec=rpc_timeout_sec)
                cls._runtimes[key] = runtime
            runtime._holders += 1
            need_connect = runtime._holders == 1 or not runtime.mux.is_connected()
        if need_connect:
            try:
                runtime._connect_if_needed()
            except Exception:
                with cls._guard:
                    runtime._holders = max(0, runtime._holders - 1)
                    drop = runtime._holders == 0
                    if drop:
                        cls._runtimes.pop(key, None)
                if drop:
                    with contextlib.suppress(Exception):
                        runtime.mux.disconnect()
                raise
        return runtime

    @classmethod
    def detach(cls, runtime: GatewayRuntime) -> None:
        key = runtime_registry_key(runtime.config)
        disconnect = False
        with cls._guard:
            if runtime._holders <= 0:
                return
            runtime._holders -= 1
            if runtime._holders == 0:
                cls._runtimes.pop(key, None)
                disconnect = True
        if disconnect:
            runtime.mux.disconnect()

    @classmethod
    def _get_or_create(cls, config: GatewayConfig) -> GatewayRuntime:
        """Return registry entry without attaching a holder (tests / factory)."""
        key = runtime_registry_key(config)
        with cls._guard:
            runtime = cls._runtimes.get(key)
            if runtime is None:
                runtime = GatewayRuntime(config)
                cls._runtimes[key] = runtime
            return runtime

    @classmethod
    def create_client(
        cls,
        config: GatewayConfig,
        *,
        rpc_timeout_sec: float = DEFAULT_RPC_TIMEOUT_SEC,
    ) -> MuxGatewayClient:
        key = runtime_registry_key(config)
        with cls._guard:
            runtime = cls._runtimes.get(key)
            if runtime is None:
                runtime = GatewayRuntime(config, rpc_timeout_sec=rpc_timeout_sec)
                cls._runtimes[key] = runtime
            return MuxGatewayClient(runtime)

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
    rpc_timeout_sec: float = DEFAULT_RPC_TIMEOUT_SEC,
    history_rpc_timeout_sec: float | None = None,
) -> GatewayClient:
    resolved: GatewayClientMode
    if mode == "dual":
        resolved = "dual"
    elif mode == "mux":
        resolved = "mux"
    else:
        resolved = parse_gateway_client_mode()
    if resolved == "dual":
        kwargs: dict[str, Any] = {"rpc_timeout_sec": rpc_timeout_sec}
        if history_rpc_timeout_sec is not None:
            kwargs["history_rpc_timeout_sec"] = history_rpc_timeout_sec
        return GatewayClient(config, **kwargs)
    return GatewayRuntimeRegistry.create_client(config, rpc_timeout_sec=rpc_timeout_sec)
