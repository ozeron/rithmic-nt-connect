"""Auto-spawn ``rithmic-gateway`` for local unix listeners."""

from __future__ import annotations

import contextlib
import os
import shutil
import socket
import subprocess  # nosec B404 - auto-spawn requires launching the gateway binary
import threading
import time
from collections.abc import Mapping, Sequence
from pathlib import Path

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


# Cargo emits ``rithmic-gateway.exe`` on Windows and ``rithmic-gateway`` elsewhere.
_GATEWAY_BIN_NAME = "rithmic-gateway"
_GATEWAY_BIN_NAMES = (_GATEWAY_BIN_NAME, _GATEWAY_BIN_NAME + ".exe")


def _bin_search_starts() -> list[Path]:
    """Roots to walk for cargo ``target/`` binaries.

    Include the editable ``rithmic_nt_connect`` tree. The installed
    ``rithmic_gateway`` wheel carries its own ``bin/`` (resolved separately via
    :func:`_bundled_bin`) and has no ``target/``.
    """
    starts = [Path.cwd(), Path(__file__).resolve().parent]
    try:
        import rithmic_nt_connect

        starts.append(Path(rithmic_nt_connect.__file__).resolve().parent)
    except ImportError:
        pass
    return starts


def _cargo_target_candidates(start: Path) -> list[Path]:
    """Look for ``target/{release,debug}/rithmic-gateway[.exe]`` under
    ``start`` ancestors.
    """
    out: list[Path] = []
    cargo_target = os.environ.get("CARGO_TARGET_DIR")
    if cargo_target:
        base = Path(cargo_target).expanduser()
        for name in _GATEWAY_BIN_NAMES:
            out.append(base / "release" / name)
            out.append(base / "debug" / name)
    cur = start.resolve()
    for root in [cur, *cur.parents]:
        for name in _GATEWAY_BIN_NAMES:
            out.append(root / "target" / "release" / name)
            out.append(root / "target" / "debug" / name)
        if (root / "Cargo.toml").is_file() and (root / "crates").is_dir():
            break
    return out


def _bundled_bin() -> str | None:
    """Return the gateway binary bundled inside the installed package, if present.

    Wheels built via ``scripts/build_wheel.sh`` carry ``rithmic_gateway/bin/``
    alongside the Python client, so an installed wheel self-contains the native
    binary with no ``cargo build`` and no ``target/`` on the consumer's disk.
    """
    base = Path(__file__).resolve().parent / "bin"
    for name in _GATEWAY_BIN_NAMES:
        path = base / name
        if path.is_file() and os.access(path, os.X_OK):
            return str(path.resolve())
    return None


def _resolve_user_bin(raw: str | None, label: str) -> str | None:
    """Resolve a user-supplied binary path or ``None`` when unset;
    ``SpawnError`` if missing.
    """
    if not raw:
        return None
    path = Path(raw).expanduser()
    if not path.is_file():
        raise SpawnError(f"{label} not found: {path}")
    return str(path.resolve())


def resolve_gateway_bin(explicit: str | None = None) -> str:
    """Resolve ``rithmic-gateway`` from explicit path, env, bundled, PATH, or
    cargo targets.

    Does not run ``cargo build`` — set ``RITHMIC_GATEWAY_BIN`` or build the
    binary first.
    Search order: explicit → ``RITHMIC_GATEWAY_BIN`` → bundled ``rithmic_gateway/bin`` →
    ``PATH`` → cwd then package ``target/{release,debug}`` (and ``CARGO_TARGET_DIR``).
    """
    for label, raw in (
        ("gateway bin", explicit),
        ("RITHMIC_GATEWAY_BIN", os.environ.get("RITHMIC_GATEWAY_BIN")),
    ):
        resolved = _resolve_user_bin(raw, label)
        if resolved is not None:
            return resolved
    bundled = _bundled_bin()
    if bundled is not None:
        return bundled
    found = shutil.which(_GATEWAY_BIN_NAME)
    if found:
        return found
    seen: set[str] = set()
    for start in _bin_search_starts():
        for candidate in _cargo_target_candidates(start):
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            if candidate.is_file() and os.access(candidate, os.X_OK):
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


def _validate_listen(listen: str) -> None:
    """Auto-spawn only supports local unix:// listeners."""
    try:
        parse_listen_url(listen)
    except GatewayConfigError as exc:
        raise SpawnError(str(exc)) from exc
    if listen.strip().startswith("tcp://") or listen.strip().startswith("tls://"):
        raise SpawnError("auto-spawn only supports local unix:// listeners")


def _build_child_env(
    config: GatewayConfig, environ: Mapping[str, str] | None
) -> dict[str, str]:
    """Assemble the child environment; precedence order is load-bearing.

    Layers, lowest to highest: curated process env → spawn_environ overlay
    (credentials survive an empty/stale process env) → fingerprint overwrite
    from GatewayConfig (aliases must not win) → idle-exit default. Invariant:
    a password must exist by the end, sourced from env only — never argv.
    """
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
    env["RITHMIC_GATEWAY_LISTEN"] = config.listen or ""
    # Auto-spawned parents exit after last client (grace); manual bin defaults to never.
    env.setdefault("RITHMIC_GATEWAY_IDLE_EXIT_SEC", "5")
    if "RITHMIC_PASSWORD" not in env or not env["RITHMIC_PASSWORD"]:
        raise SpawnError(
            "missing RITHMIC_PASSWORD for auto-spawn "
            "(set spawn_environ or process env; never put password on argv)"
        )
    return env


def _wait_for_socket(
    proc: subprocess.Popen[bytes],
    config: GatewayConfig,
) -> None:
    """Block until the gateway socket accepts and holds the credential flock.

    Raises ``SpawnError`` on early exit or timeout; reaps the orphan child so
    it cannot keep the flock / bind later.
    """
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
            return
        if proc.poll() is not None:
            if _listening() and _session_flock_held():
                return
            if _session_flock_held():
                # Lost the flock race: another client's gateway won and is
                # still binding its socket (plants connect after bind). Keep
                # waiting for that socket instead of failing the caller on a
                # transient concurrent-spawn race.
                time.sleep(0.05)
                continue
            err = b""
            if proc.stderr is not None:
                err = proc.stderr.read() or b""
            raise SpawnError(
                f"gateway exited early (code={proc.returncode}): "
                f"{err.decode(errors='replace')}"
            )
        time.sleep(0.05)
    # Reap the orphan so it cannot keep the flock / bind later.
    _start_stderr_drain(proc)
    proc.terminate()
    try:
        proc.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=2.0)
    raise SpawnError(f"timed out waiting for gateway socket {sock}")


def _start_stderr_drain(proc: subprocess.Popen[bytes]) -> None:
    """Prevent a long-lived child from blocking on a full stderr PIPE."""

    def _drain_stderr() -> None:
        if proc.stderr is None:
            return
        try:
            while proc.stderr.read(65536):
                pass
        except Exception:
            pass

    threading.Thread(target=_drain_stderr, name="gw-stderr", daemon=True).start()


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
    _validate_listen(listen)

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

    env = _build_child_env(config, environ)

    proc = subprocess.Popen(  # nosec B603 - argv allowlisted; env curated
        argv,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )

    if wait_socket:
        _wait_for_socket(proc, config)
    _start_stderr_drain(proc)
    return proc


def assert_no_password_in_argv(argv: Sequence[str], password: str) -> None:
    """Test helper: ensure password never appears in argv."""
    if not password:
        return
    for arg in argv:
        if password in arg:
            raise AssertionError("password leaked onto argv")
