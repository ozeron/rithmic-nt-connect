"""Auto-spawn ``rithmic-gateway`` for local unix listeners."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Mapping, Sequence

from rithmic_gateway.config import GatewayConfig, GatewayConfigError, parse_listen_url

# Env keys forwarded to the parent process (password stays in env, never argv).
_CURATED_ENV_KEYS = (
    "RITHMIC_USER",
    "RITHMIC_PASSWORD",
    "RITHMIC_SYSTEM_NAME",
    "RITHMIC_URL",
    "RITHMIC_ENV",
    "RITHMIC_ACCOUNT_ID",
    "RITHMIC_FCM_ID",
    "RITHMIC_IB_ID",
    "RITHMIC_ENABLE_TRADING",
    "RITHMIC_GATEWAY_CANCEL_ALL",
    "RITHMIC_GATEWAY_LISTEN",
    "RITHMIC_GATEWAY_AUTH_TOKEN",
    "RITHMIC_GATEWAY_IDLE_EXIT_SEC",
    "XDG_RUNTIME_DIR",
    "PATH",
    "HOME",
    "TMPDIR",
)


class SpawnError(RuntimeError):
    """Failed to locate or start the gateway binary."""


def _bin_search_starts() -> list[Path]:
    """Roots to walk for cargo ``target/`` binaries (cwd first, then package)."""
    return [Path.cwd(), Path(__file__).resolve().parent]


def _cargo_target_candidates(start: Path) -> list[Path]:
    """Look for ``target/{release,debug}/rithmic-gateway`` under ancestors of ``start``."""
    out: list[Path] = []
    cargo_target = os.environ.get("CARGO_TARGET_DIR")
    if cargo_target:
        base = Path(cargo_target).expanduser()
        out.append(base / "release" / "rithmic-gateway")
        out.append(base / "debug" / "rithmic-gateway")
    cur = start.resolve()
    for root in [cur, *cur.parents]:
        out.append(root / "target" / "release" / "rithmic-gateway")
        out.append(root / "target" / "debug" / "rithmic-gateway")
        if (root / "Cargo.toml").is_file() and (root / "crates").is_dir():
            break
    return out


def resolve_gateway_bin(explicit: str | None = None) -> str:
    """Resolve ``rithmic-gateway`` from explicit path, env, ``PATH``, or cargo targets.

    Does not run ``cargo build`` — set ``RITHMIC_GATEWAY_BIN`` or build the binary first.
    Search order: explicit → ``RITHMIC_GATEWAY_BIN`` → ``PATH`` → cwd then package
    ``target/{release,debug}`` (and ``CARGO_TARGET_DIR`` when set).
    """
    if explicit:
        path = Path(explicit).expanduser()
        if not path.is_file():
            raise SpawnError(f"gateway bin not found: {path}")
        return str(path.resolve())
    env_bin = os.environ.get("RITHMIC_GATEWAY_BIN")
    if env_bin:
        path = Path(env_bin).expanduser()
        if not path.is_file():
            raise SpawnError(f"RITHMIC_GATEWAY_BIN not found: {path}")
        return str(path.resolve())
    found = shutil.which("rithmic-gateway")
    if found:
        return found
    seen: set[str] = set()
    for start in _bin_search_starts():
        for candidate in _cargo_target_candidates(start):
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            if candidate.is_file():
                return str(candidate.resolve())
    raise SpawnError(
        "rithmic-gateway not on PATH or under target/{release,debug}; "
        "set RITHMIC_GATEWAY_BIN or build with "
        "`cargo build -p rithmic-gateway --release`"
    )


def curated_env(source: Mapping[str, str] | None = None) -> dict[str, str]:
    """Build a minimal env for the parent — never puts password on argv."""
    src = source if source is not None else os.environ
    out: dict[str, str] = {}
    for key in _CURATED_ENV_KEYS:
        val = src.get(key)
        if val is not None and str(val) != "":
            out[key] = str(val)
    return out


def spawn_argv(bin_path: str) -> list[str]:
    """Argv allowlist: binary path only (no secrets)."""
    return [bin_path]


def spawn_gateway(
    config: GatewayConfig,
    *,
    environ: Mapping[str, str] | None = None,
    wait_socket: bool = True,
) -> subprocess.Popen[bytes]:
    """Start ``rithmic-gateway`` detached enough for the client to dial.

    Password is only forwarded via curated env, never argv.
    """
    listen = config.listen or ""
    try:
        parse_listen_url(listen)
    except GatewayConfigError as exc:
        raise SpawnError(str(exc)) from exc
    if listen.strip().startswith("tcp://") or listen.strip().startswith("tls://"):
        raise SpawnError("auto-spawn only supports local unix:// listeners")

    bin_path = resolve_gateway_bin(config.gateway_bin)
    argv = spawn_argv(bin_path)
    # Defense in depth: argv must never contain the password value.
    candidate_secrets = [
        (environ or os.environ).get("RITHMIC_PASSWORD", ""),
        (config.spawn_environ or {}).get("RITHMIC_PASSWORD", ""),
    ]
    for secret in candidate_secrets:
        if secret and any(secret in arg for arg in argv):
            raise SpawnError("refusing to spawn: password must not appear on argv")

    env = curated_env(environ)
    # Overlay spawn_environ (credentials) before fingerprint overwrite so password
    # is present even when the process env is empty or stale.
    if config.spawn_environ:
        for key, val in config.spawn_environ.items():
            if val is not None and str(val) != "":
                env[str(key)] = str(val)
    # Overwrite fingerprint keys from GatewayConfig (aliases must not win).
    env["RITHMIC_USER"] = config.user
    env["RITHMIC_SYSTEM_NAME"] = config.system_name
    env["RITHMIC_URL"] = config.url
    env["RITHMIC_ENV"] = config.env
    # Password: prefer spawn_environ, else already curated from environ.
    if config.spawn_environ and config.spawn_environ.get("RITHMIC_PASSWORD"):
        env["RITHMIC_PASSWORD"] = config.spawn_environ["RITHMIC_PASSWORD"]
    env["RITHMIC_GATEWAY_LISTEN"] = listen
    # Auto-spawned parents exit after last client (grace); manual bin defaults to never.
    env.setdefault("RITHMIC_GATEWAY_IDLE_EXIT_SEC", "5")
    if "RITHMIC_PASSWORD" not in env or not env["RITHMIC_PASSWORD"]:
        raise SpawnError(
            "missing RITHMIC_PASSWORD for auto-spawn "
            "(set spawn_environ or process env; never put password on argv)"
        )

    proc = subprocess.Popen(
        argv,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )

    def _drain_stderr() -> None:
        """Prevent a long-lived child from blocking on a full stderr PIPE."""
        if proc.stderr is None:
            return
        try:
            while proc.stderr.read(65536):
                pass
        except Exception:
            pass

    if wait_socket:
        deadline = time.monotonic() + float(config.spawn_timeout_sec)
        sock = Path(config.socket_path)

        def _listening() -> bool:
            try:
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.settimeout(0.2)
                s.connect(str(sock))
                s.close()
                return True
            except OSError:
                return False

        def _session_flock_held() -> bool:
            """True when another live process holds the credential flock."""
            from rithmic_gateway.flock import session_flock_held

            return session_flock_held(
                config.user, config.system_name, config.url, config.env
            )

        while time.monotonic() < deadline:
            # Socket alone is not proof — require the credential flock too.
            if _listening() and _session_flock_held():
                threading.Thread(target=_drain_stderr, name="gw-stderr", daemon=True).start()
                return proc
            if proc.poll() is not None:
                if _listening() and _session_flock_held():
                    return proc
                err = b""
                if proc.stderr is not None:
                    err = proc.stderr.read() or b""
                raise SpawnError(
                    f"gateway exited early (code={proc.returncode}): {err.decode(errors='replace')}"
                )
            time.sleep(0.05)
        # Reap the orphan so it cannot keep the flock / bind later.
        threading.Thread(target=_drain_stderr, name="gw-stderr", daemon=True).start()
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                pass
        raise SpawnError(f"timed out waiting for gateway socket {sock}")
    threading.Thread(target=_drain_stderr, name="gw-stderr", daemon=True).start()
    return proc


def assert_no_password_in_argv(argv: Sequence[str], password: str) -> None:
    """Test helper: ensure password never appears in argv."""
    if not password:
        return
    for arg in argv:
        if password in arg:
            raise AssertionError("password leaked onto argv")
