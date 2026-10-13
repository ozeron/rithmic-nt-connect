"""Length-delimited protobuf framing over one unix socket (shared by client and mux)."""

from __future__ import annotations

import contextlib
import socket
import struct

from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.flock import session_flock_held
from rithmic_gateway.framing import MAX_FRAME_LEN, encode_frame
from rithmic_gateway.v1 import session_pb2 as pb

# Default dial + RPC socket timeout (seconds). Stuck parent must not hang forever.
DEFAULT_RPC_TIMEOUT_SEC = 30.0


class GatewayError(RuntimeError):
    """Gateway RPC or protocol error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


# Constants used in the ``not_connected`` raise paths. Kept module-level so the
# ``Sonar S1192`` "duplicate literal" rule sees a single source of truth; copy
# only changes when the protocol does, never scattered across private methods.
_CONNECT_FIRST = "call connect() first"
_NOT_CONNECTED = "not_connected"


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    try:
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise GatewayError("eof", "gateway closed connection")
            buf.extend(chunk)
    except TimeoutError as exc:
        if buf:
            with contextlib.suppress(Exception):
                sock.close()
            raise GatewayError(
                "desync",
                f"timed out after {len(buf)}/{n} bytes; connection closed",
            ) from exc
        raise
    return bytes(buf)


class FramedConnection:
    """Socket lifecycle + frame I/O; subclasses own handshake, RPC and polling."""

    _sock: socket.socket | None
    _config: GatewayConfig
    _rpc_timeout_sec: float

    def _dial(self, path: str) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self._rpc_timeout_sec)
        sock.connect(path)
        # Timeout stays set through the handshake; callers adjust it afterwards.
        self._sock = sock

    def _require_parent_flock(self) -> None:
        """Refuse Ready from a dialable impostor that does not hold the flock."""
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

    def _close_sock(self) -> None:
        sock = self._sock
        self._sock = None
        if sock is not None:
            with contextlib.suppress(Exception):
                sock.close()

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
