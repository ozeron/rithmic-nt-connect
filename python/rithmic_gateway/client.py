"""Plant-level gateway client over unix + length-delimited protobuf."""

from __future__ import annotations

import socket
import struct
from typing import Any

from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.framing import MAX_FRAME_LEN, encode_frame
from rithmic_gateway.spawn import SpawnError, spawn_gateway
from rithmic_gateway.v1 import session_pb2 as pb


class GatewayError(RuntimeError):
    """Gateway RPC or protocol error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class GatewayClient:
    """Attach to a parent ``rithmic-gateway`` and issue plant-semantic RPCs.

    Events and responses are plain dicts / structured protobuf MessageToDict-style
    maps — never Nautilus types.
    """

    def __init__(self, config: GatewayConfig) -> None:
        self._config = config
        self._sock: socket.socket | None = None
        self._next_id = 1
        self._scopes: list[str] = []
        self._trading_enabled = False
        self._cancel_all_enabled = False
        self._spawned = None

    @property
    def scopes(self) -> list[str]:
        return list(self._scopes)

    @property
    def trading_enabled(self) -> bool:
        return self._trading_enabled

    @property
    def cancel_all_enabled(self) -> bool:
        return self._cancel_all_enabled

    def connect(self) -> None:
        if self._sock is not None:
            return
        path = self._config.socket_path
        try:
            self._dial(path)
        except (FileNotFoundError, ConnectionRefusedError, OSError):
            if not self._config.auto_spawn:
                raise
            try:
                self._spawned = spawn_gateway(self._config)
            except SpawnError:
                raise
            self._dial(path)
        self._handshake()

    def disconnect(self) -> None:
        """Detach this client only — does not tear down parent plants for peers."""
        if self._sock is None:
            return
        try:
            self._rpc(pb.Frame(disconnect=pb.DisconnectRequest()))
        except Exception:
            pass
        try:
            self._sock.close()
        finally:
            self._sock = None

    def subscribe(self, symbol: str, exchange: str) -> None:
        self._rpc(pb.Frame(subscribe=pb.SubscribeRequest(symbol=symbol, exchange=exchange)))

    def unsubscribe(self, symbol: str, exchange: str) -> None:
        self._rpc(
            pb.Frame(unsubscribe=pb.UnsubscribeRequest(symbol=symbol, exchange=exchange))
        )

    def subscribe_order_book_summary(self, symbol: str, exchange: str) -> None:
        self._rpc(
            pb.Frame(
                subscribe_book=pb.SubscribeBookRequest(symbol=symbol, exchange=exchange)
            )
        )

    def request_plants(self, plants: str) -> None:
        self._rpc(pb.Frame(request_plants=pb.RequestPlantsRequest(plants=plants)))

    def place_order(
        self,
        symbol: str,
        exchange: str,
        side: str,
        price_type: str,
        quantity: int,
        user_tag: str = "",
        price: float | None = None,
        trigger_price: float | None = None,
        duration: str = "DAY",
        trail_by_ticks: int | None = None,
        trail_by_price_id: int | None = None,
    ) -> None:
        req = pb.PlaceOrderRequest(
            symbol=symbol,
            exchange=exchange,
            side=side,
            price_type=price_type,
            quantity=quantity,
            user_tag=user_tag,
            duration=duration,
        )
        if price is not None:
            req.price = price
        if trigger_price is not None:
            req.trigger_price = trigger_price
        if trail_by_ticks is not None:
            req.trail_by_ticks = trail_by_ticks
        if trail_by_price_id is not None:
            req.trail_by_price_id = trail_by_price_id
        self._rpc(pb.Frame(place_order=req))

    def cancel_all_orders(self) -> None:
        self._rpc(pb.Frame(cancel_all_orders=pb.CancelAllOrdersRequest()))

    def poll_event(self, timeout_ms: int = 0) -> dict[str, Any] | None:
        """Non-blocking-ish read of the next Event frame (or None on timeout)."""
        if self._sock is None:
            raise GatewayError("not_connected", "call connect() first")
        self._sock.settimeout(max(timeout_ms, 0) / 1000.0 if timeout_ms else 0.0)
        try:
            frame = self._read_frame()
        except (TimeoutError, socket.timeout):
            return None
        except BlockingIOError:
            return None
        finally:
            self._sock.settimeout(None)
        which = frame.WhichOneof("body")
        if which == "event":
            return _event_to_dict(frame.event)
        if which == "error":
            raise GatewayError(frame.error.code, frame.error.message)
        return {"type": which or "unknown"}

    # --- internals ---------------------------------------------------------

    def _dial(self, path: str) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(path)
        self._sock = sock

    def _handshake(self) -> None:
        assert self._sock is not None
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
            raise GatewayError("protocol", f"expected Ready, got {which}")
        ready = ready_frame.ready
        self._scopes = list(ready.scopes)
        self._trading_enabled = bool(ready.trading_enabled)
        self._cancel_all_enabled = bool(ready.cancel_all_enabled)

    def _rpc(self, frame: pb.Frame) -> pb.Frame:
        if self._sock is None:
            raise GatewayError("not_connected", "call connect() first")
        rid = self._next_id
        self._next_id += 1
        frame.request_id = rid
        self._write_frame(frame)
        resp = self._read_frame()
        which = resp.WhichOneof("body")
        if which == "error":
            raise GatewayError(resp.error.code, resp.error.message)
        return resp

    def _write_frame(self, frame: pb.Frame) -> None:
        assert self._sock is not None
        payload = frame.SerializeToString()
        self._sock.sendall(encode_frame(payload))

    def _read_frame(self) -> pb.Frame:
        assert self._sock is not None
        header = _recv_exact(self._sock, 4)
        (length,) = struct.unpack("!I", header)
        if length > MAX_FRAME_LEN:
            raise GatewayError("frame_too_large", f"{length} bytes")
        payload = _recv_exact(self._sock, length)
        frame = pb.Frame()
        frame.ParseFromString(payload)
        return frame


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise GatewayError("eof", "gateway closed connection")
        buf.extend(chunk)
    return bytes(buf)


def _event_to_dict(event: pb.Event) -> dict[str, Any]:
    which = event.WhichOneof("body")
    if which is None:
        return {"type": "empty"}
    msg = getattr(event, which)
    out: dict[str, Any] = {"type": which}
    for field, value in msg.ListFields():
        out[field.name] = value
    return out
