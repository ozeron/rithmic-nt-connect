"""Harness: the real ``rithmic-gateway`` server over a scripted fake plant.

Spawns the test-only ``rithmic-gateway-fake`` binary (built with
``--features testing``; never part of the wheel) in a short temp runtime dir and
drives it over its control socket. The same scenario list runs against every
client implementation, so behavior is pinned once and checked per client.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import socket
import subprocess  # nosec B404 - spawns the fake gateway binary under test
import tempfile
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

from rithmic_gateway.attach import socket_listening
from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.flock import session_flock_held

ROOT = Path(__file__).resolve().parents[1]
FAKE_BIN_ENV = "RITHMIC_GATEWAY_FAKE_BIN"
SYSTEM = "LucidTrading"
URL = "wss://fake.example"


def build_fake_gateway() -> Path | None:
    """The fake gateway binary: ``$RITHMIC_GATEWAY_FAKE_BIN`` or a cargo build."""
    explicit = os.environ.get(FAKE_BIN_ENV)
    if explicit:
        path = Path(explicit)
        return path if path.is_file() else None
    cargo = shutil.which("cargo")
    if cargo is None:
        return None
    built = subprocess.run(  # nosec B603 - fixed argv
        [
            cargo,
            "build",
            "-p",
            "rithmic-gateway",
            "--features",
            "testing",
            "--bin",
            "rithmic-gateway-fake",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if built.returncode != 0:
        return None
    target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    path = target / "debug" / "rithmic-gateway-fake"
    return path if path.is_file() else None


class FakeParent:
    """One running fake-plant gateway parent and its control channel."""

    def __init__(
        self,
        binary: Path,
        *,
        trading: bool = False,
        cancel_all: bool = False,
        idle_exit_sec: str | None = None,
    ) -> None:
        self.binary = binary
        self.trading = trading
        self.cancel_all = cancel_all
        self.idle_exit_sec = idle_exit_sec
        self.user = f"fake-{uuid.uuid4().hex[:10]}"
        # Short path: macOS sockaddr_un is ~104 bytes.
        self.dir = Path(tempfile.mkdtemp(prefix="rgf-", dir="/tmp"))  # nosec B108
        self.listen = self.dir / "gw.sock"
        self.control_path = self.dir / "ctl.sock"
        self.proc: subprocess.Popen[bytes] | None = None
        self._stderr = tempfile.TemporaryFile()  # noqa: SIM115 - closed in stop()

    # -- lifecycle ---------------------------------------------------------

    def config(self, *, attach_timeout_sec: float = 30.0) -> GatewayConfig:
        """A client config whose fingerprint matches this parent."""
        return GatewayConfig(
            user=self.user,
            system_name=SYSTEM,
            url=URL,
            env="Live",
            listen=f"unix://{self.listen}",
            attach_timeout_sec=attach_timeout_sec,
        )

    def _env(self) -> dict[str, str]:
        # TMPDIR / XDG_RUNTIME_DIR must match the client's: the credential flock
        # lives under the runtime dir both sides derive from them.
        env = {
            key: os.environ[key]
            for key in ("PATH", "HOME", "TMPDIR", "XDG_RUNTIME_DIR")
            if key in os.environ
        }
        env |= {
            "RITHMIC_USER": self.user,
            "RITHMIC_SYSTEM_NAME": SYSTEM,
            "RITHMIC_URL": URL,
            "RITHMIC_ENV": "Live",
            "RITHMIC_GATEWAY_LISTEN": f"unix://{self.listen}",
            "RITHMIC_FAKE_CONTROL": str(self.control_path),
        }
        if self.trading:
            env["RITHMIC_ENABLE_TRADING"] = "1"
        if self.cancel_all:
            env["RITHMIC_GATEWAY_CANCEL_ALL"] = "1"
        if self.idle_exit_sec is not None:
            env["RITHMIC_GATEWAY_IDLE_EXIT_SEC"] = self.idle_exit_sec
        return env

    def start(self, timeout_sec: float = 20.0) -> FakeParent:
        self.proc = subprocess.Popen(  # nosec B603 - fixed argv, test-controlled env
            [str(self.binary)],
            env=self._env(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=self._stderr,
            start_new_session=True,
        )
        deadline = time.monotonic() + timeout_sec
        cfg = self.config()
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"fake gateway exited early: {self.stderr_text()}")
            listening = socket_listening(self.listen)
            held = session_flock_held(cfg.user, cfg.system_name, cfg.url, cfg.env)
            if listening and held and self.control_path.exists():
                return self
            time.sleep(0.02)
        self.stop()
        raise RuntimeError(f"fake gateway did not come up: {self.stderr_text()}")

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)

    def cleanup(self) -> None:
        self.stop()
        with contextlib.suppress(Exception):
            self._stderr.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def restart(self) -> None:
        """Kill the parent and start a fresh one on the same paths / fingerprint."""
        self.stop()
        deadline = time.monotonic() + 10
        cfg = self.config()
        while session_flock_held(cfg.user, cfg.system_name, cfg.url, cfg.env):
            if time.monotonic() > deadline:
                raise RuntimeError("flock not released after stopping the parent")
            time.sleep(0.02)
        self.listen.unlink(missing_ok=True)
        self.start()

    def stderr_text(self) -> str:
        self._stderr.seek(0)
        return self._stderr.read().decode(errors="replace")[-2000:]

    # -- control channel ---------------------------------------------------

    def control(self, *commands: str, batch: int = 200) -> list[list[str]]:
        """Send commands (in batches, so replies are drained); one reply each."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(30)
        sock.connect(str(self.control_path))
        replies: list[list[str]] = []
        try:
            for start in range(0, len(commands), batch):
                chunk = commands[start : start + batch]
                sock.sendall("".join(f"{c}\n" for c in chunk).encode())
                current: list[str] = []
                buf = b""
                want = len(replies) + len(chunk)
                while len(replies) < want:
                    data = sock.recv(65536)
                    if not data:
                        raise RuntimeError("control channel closed early")
                    buf += data
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        text = line.decode()
                        if text == "END":
                            replies.append(current)
                            current = []
                        else:
                            current.append(text)
            return replies
        finally:
            sock.close()

    def command(self, cmd: str) -> list[str]:
        reply = self.control(cmd)[0]
        if reply and reply[0].startswith("err"):
            raise RuntimeError(f"{cmd!r}: {reply[0]}")
        return reply

    def calls(self) -> list[str]:
        return self.command("calls")

    def count(self, prefix: str) -> int:
        return sum(1 for c in self.calls() if c.startswith(prefix))

    def clear_calls(self) -> None:
        self.command("clear_calls")

    def drop_plants(self) -> None:
        self.command("drop_plants")

    def push_last_trades(self, symbol: str, n: int, *, exchange: str = "CME") -> None:
        cmds = [f"push last_trade {symbol} {exchange} {100 + i}.25 1" for i in range(n)]
        for reply in self.control(*cmds):
            assert reply == ["ok"], reply

    def fail_next(self, method: str, kind: str, message: str = "injected") -> None:
        """Fail the next call of ``method``: kind is rithmic, closed, config,
        rejected, not_sent or unknown."""
        self.push_command(f"fail_next {method} {kind} {message}")

    def push_command(self, command: str) -> None:
        assert self.command(command) == ["ok"]

    def push_gap(self, stream: str, skipped: int) -> None:
        """Script a plant lag: ``stream`` is ticker, history, pnl or order."""
        self.push("gap", stream, str(skipped))

    def push(self, *parts: str) -> None:
        assert self.command("push " + " ".join(parts)) == ["ok"]

    def wait_for_call(self, prefix: str, timeout_sec: float = 15.0) -> bool:
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            if self.count(prefix):
                return True
            time.sleep(0.05)
        return False


@contextlib.contextmanager
def running_fake(
    binary: Path,
    *,
    trading: bool = False,
    cancel_all: bool = False,
    idle_exit_sec: str | None = None,
) -> Iterator[FakeParent]:
    parent = FakeParent(
        binary, trading=trading, cancel_all=cancel_all, idle_exit_sec=idle_exit_sec
    )
    try:
        yield parent.start()
    finally:
        parent.cleanup()
