"""Single-socket gateway mux: one drain thread, typed event fan-out."""

from __future__ import annotations

import contextlib
import queue
import socket
import struct
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from rithmic_gateway.client import (
    _CONNECT_FIRST,
    _NOT_CONNECTED,
    GatewayError,
    _dial_with_attach,
    _event_to_dict,
    _handshake_until_ready,
    _is_history_event,
    _is_md_event,
    _is_order_event,
    _is_pnl_event,
    _recv_exact,
    _wire_error,
)
from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.flock import session_flock_held
from rithmic_gateway.framing import MAX_FRAME_LEN, encode_frame
from rithmic_gateway.v1 import session_pb2 as pb

DEFAULT_RPC_TIMEOUT_SEC = 30.0
_MD_QUEUE_CAP = 4096


class GatewayMux:
    """Owns the unix fd and demuxes frames to RPC waiters and typed queues."""

    def __init__(self, config: GatewayConfig) -> None:
        self._config = config
        self._rpc_timeout_sec = DEFAULT_RPC_TIMEOUT_SEC
        self._sock: socket.socket | None = None
        self._spawned = None
        self._next_id = 1
        self._scopes: list[str] = []
        self._trading_enabled = False
        self._cancel_all_enabled = False
        self._gateway_instance_id = 0
        self._transport_generation = 0
        self._write_lock = threading.Lock()
        self._state_lock = threading.RLock()
        self._rpc_waiters: dict[int, queue.Queue[Any]] = {}
        self._md_events: deque[dict[str, Any]] = deque()
        self._order_events: deque[dict[str, Any]] = deque()
        self._pnl_events: deque[dict[str, Any]] = deque()
        self._history_events: deque[dict[str, Any]] = deque()
        self._generation_listeners: list[Callable[[int], None]] = []
        self._stop = threading.Event()
        self._drain_thread: threading.Thread | None = None

    @property
    def scopes(self) -> list[str]:
        return list(self._scopes)

    @property
    def trading_enabled(self) -> bool:
        return self._trading_enabled

    @property
    def cancel_all_enabled(self) -> bool:
        return self._cancel_all_enabled

    @property
    def gateway_instance_id(self) -> int:
        return self._gateway_instance_id

    @property
    def transport_generation(self) -> int:
        return self._transport_generation

    def is_connected(self) -> bool:
        thread = self._drain_thread
        return self._sock is not None and thread is not None and thread.is_alive()

    def add_generation_listener(self, callback: Callable[[int], None]) -> None:
        self._generation_listeners.append(callback)

    def connect(self) -> None:
        with self._state_lock:
            if self.is_connected():
                return
            if self._sock is not None:
                self._close_sock()
            cfg = self._config
            self._spawned = _dial_with_attach(cfg, self._dial)
            self._require_parent_flock()
            _handshake_until_ready(
                cfg,
                dial=self._dial,
                handshake=self._handshake,
                require_flock=self._require_parent_flock,
                close_sock=self._close_sock,
            )
            self._stop.clear()
            self._drain_thread = threading.Thread(
                target=self._drain_loop, name="gateway-mux-drain", daemon=True
            )
            self._drain_thread.start()

    def disconnect(self) -> None:
        with self._state_lock:
            if self._sock is None:
                return
            self._stop.set()
            with contextlib.suppress(Exception):
                self._write_frame(pb.Frame(disconnect=pb.DisconnectRequest()))
            if self._drain_thread is not None:
                self._drain_thread.join(timeout=2.0)
                self._drain_thread = None
            self._close_sock()
            self._clear_queues()
            self._fail_pending_rpcs(GatewayError("eof", "gateway disconnected"))

    def reconnect(self) -> None:
        """L3 transport recovery: DISARM peers, then re-dial + handshake."""
        with self._state_lock:
            self._stop.set()
            if self._drain_thread is not None:
                self._drain_thread.join(timeout=2.0)
                self._drain_thread = None
            self._close_sock()
            self._clear_queues()
            self._fail_pending_rpcs(
                GatewayError("transport_reset", "gateway transport reconnecting")
            )
            self.connect()
            self._notify_generation_listeners()

    def rpc_unlocked(
        self, frame: pb.Frame, *, timeout_sec: float | None = None
    ) -> pb.Frame:
        if self._sock is None:
            raise GatewayError(_NOT_CONNECTED, _CONNECT_FIRST)
        effective = self._rpc_timeout_sec if timeout_sec is None else float(timeout_sec)
        with self._write_lock:
            rid = self._next_id
            self._next_id += 1
            waiter: queue.Queue[Any] = queue.Queue(maxsize=1)
            self._rpc_waiters[rid] = waiter
            frame.request_id = rid
            self._write_frame(frame)
        try:
            resp = waiter.get(timeout=effective)
        except queue.Empty as exc:
            self._rpc_waiters.pop(rid, None)
            raise GatewayError(
                "timeout", f"RPC {rid} timed out after {effective}s"
            ) from exc
        if isinstance(resp, GatewayError):
            raise resp
        return resp

    def poll_filtered(self, timeout_ms: int, predicate: Any) -> dict[str, Any] | None:
        deadline = (
            time.monotonic() + max(timeout_ms, 0) / 1000.0 if timeout_ms else None
        )
        while True:
            with self._state_lock:
                for q in self._event_queues():
                    for idx, evt in enumerate(q):
                        if predicate(evt):
                            del q[idx]
                            return evt
            if timeout_ms == 0:
                return None
            if deadline is not None and time.monotonic() >= deadline:
                return None
            time.sleep(0.001)

    def _drain_loop(self) -> None:
        while not self._stop.is_set():
            try:
                frame = self._read_frame()
            except GatewayError as exc:
                if exc.code in {"desync", "eof", "frame_too_large"}:
                    self._close_sock()
                    self._fail_pending_rpcs(exc)
                    self._notify_generation_listeners()
                return
            except TimeoutError:
                # Idle read timeout must not kill the drain; connect clears timeout.
                continue
            except Exception:
                self._close_sock()
                self._fail_pending_rpcs(
                    GatewayError("eof", "gateway drain thread failed")
                )
                self._notify_generation_listeners()
                return
            self._dispatch_frame(frame)

    def _dispatch_frame(self, frame: pb.Frame) -> None:
        which = frame.WhichOneof("body")
        if which == "event":
            evt = _event_to_dict(frame.event)
            with self._state_lock:
                if _is_order_event(evt):
                    self._order_events.append(evt)
                elif _is_pnl_event(evt):
                    self._pnl_events.append(evt)
                elif _is_history_event(evt):
                    self._history_events.append(evt)
                elif _is_md_event(evt):
                    if len(self._md_events) >= _MD_QUEUE_CAP:
                        self._md_events.popleft()
                    self._md_events.append(evt)
            return
        rid = frame.request_id
        waiter = self._rpc_waiters.pop(rid, None)
        if waiter is None:
            return
        if which == "error":
            waiter.put(_wire_error(frame.error.code, frame.error.message))
        else:
            waiter.put(frame)

    def _notify_generation_listeners(self) -> None:
        gen = self._transport_generation
        for cb in list(self._generation_listeners):
            with contextlib.suppress(Exception):
                cb(gen)

    def _fail_pending_rpcs(self, exc: GatewayError) -> None:
        waiters = list(self._rpc_waiters.values())
        self._rpc_waiters.clear()
        for waiter in waiters:
            with contextlib.suppress(Exception):
                waiter.put(exc)

    def _clear_queues(self) -> None:
        for q in self._event_queues():
            q.clear()

    def _event_queues(self) -> tuple[deque[dict[str, Any]], ...]:
        return (
            self._order_events,
            self._pnl_events,
            self._history_events,
            self._md_events,
        )

    def _dial(self, path: str) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self._rpc_timeout_sec)
        sock.connect(path)
        sock.settimeout(None)
        self._sock = sock

    def _require_parent_flock(self) -> None:
        if not self._config.attest_flock:
            return
        cfg = self._config
        if not session_flock_held(cfg.user, cfg.system_name, cfg.url, cfg.env):
            self._close_sock()
            raise GatewayError(
                "parent_unattested",
                "listen path is up but credential flock is free — refusing "
                "impostor parent",
            )

    def _handshake(self) -> None:
        if self._sock is None:
            raise GatewayError(_NOT_CONNECTED, _CONNECT_FIRST)
        cfg = self._config
        hs = pb.Handshake(
            user=cfg.user,
            system_name=cfg.system_name,
            url=cfg.url,
            env=cfg.env,
            account_id=cfg.account_id or "",
            fcm_id=cfg.fcm_id or "",
            ib_id=cfg.ib_id or "",
            auth_token=cfg.auth_token or "",
        )
        self._write_frame(pb.Frame(handshake=hs))
        ready_frame = self._read_frame()
        which = ready_frame.WhichOneof("body")
        if which != "ready":
            if which == "error":
                raise _wire_error(ready_frame.error.code, ready_frame.error.message)
            raise GatewayError("protocol", f"expected Ready, got {which}")
        ready = ready_frame.ready
        prev_gen = self._transport_generation
        self._scopes = list(ready.scopes)
        self._trading_enabled = bool(ready.trading_enabled)
        self._cancel_all_enabled = bool(ready.cancel_all_enabled)
        self._gateway_instance_id = int(ready.gateway_instance_id)
        self._transport_generation = int(ready.transport_generation)
        if prev_gen and prev_gen != self._transport_generation:
            self._notify_generation_listeners()

    def _write_frame(self, frame: pb.Frame) -> None:
        if self._sock is None:
            raise GatewayError(_NOT_CONNECTED, _CONNECT_FIRST)
        payload = frame.SerializeToString()
        self._sock.sendall(encode_frame(payload))

    def _read_frame(self) -> pb.Frame:
        if self._sock is None:
            raise GatewayError(_NOT_CONNECTED, _CONNECT_FIRST)
        header = _recv_exact(self._sock, 4)
        (length,) = struct.unpack("!I", header)
        if length > MAX_FRAME_LEN:
            raise GatewayError("frame_too_large", f"{length} bytes")
        payload = _recv_exact(self._sock, length)
        frame = pb.Frame()
        frame.ParseFromString(payload)
        return frame

    def _close_sock(self) -> None:
        sock = self._sock
        self._sock = None
        if sock is not None:
            with contextlib.suppress(Exception):
                sock.close()
