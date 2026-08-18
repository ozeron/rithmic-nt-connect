"""Gateway client configuration (fingerprint + listen URL; never puts
password on IPC).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path


class GatewayConfigError(ValueError):
    """Invalid gateway client / listen configuration."""


def _require(name: str, value: str | None) -> str:
    if value is None or not str(value).strip():
        raise GatewayConfigError(f"missing or empty required field: {name}")
    return str(value).strip()


def _env_first(env: Mapping[str, str], *names: str) -> str | None:
    """First non-empty env value among ``names`` (SessionConfig / lake precedence)."""
    for name in names:
        value = env.get(name)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return None


def parse_listen_url(raw: str) -> str:
    """Return absolute unix socket path from ``unix://…`` or bare absolute path.

    ``tcp://`` / ``tls://`` are reserved until v2
    (see docs/references/gateway-remote.md).
    """
    raw = raw.strip()
    if not raw:
        raise GatewayConfigError("empty gateway listen URL")
    if raw.startswith("tcp://") or raw.startswith("tls://"):
        raise GatewayConfigError(
            f"tcp/tls listen is not implemented yet (see "
            f"docs/references/gateway-remote.md): {raw}"
        )
    if raw.startswith("unix://"):
        path = raw[len("unix://") :]
        if path.startswith("//"):
            path = "/" + path.lstrip("/")
        if not path.startswith("/"):
            path = str((Path.cwd() / path).resolve())
        return path
    if raw.startswith("/"):
        return raw
    raise GatewayConfigError(
        f"unsupported listen URL scheme (v1 supports unix:// only): {raw}"
    )


def canon_env(env: str) -> str:
    """Match Rust ``runtime_dir::canon_env`` for path hashing."""
    key = env.strip().lower()
    if key in {"live", "production"}:
        return "live"
    if key in {"demo", "development"}:
        return "demo"
    if key == "test":
        return "test"
    return "live"


def runtime_base_dir() -> str:
    """Prefer XDG_RUNTIME_DIR; else private ``$TMPDIR/rgw-$UID`` (0700).

    Matches Rust ``runtime_dir::ensure_private_dir``: refuse dirs not owned by
    this uid and fail closed if chmod 0700 cannot be applied.
    """
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg and xdg.strip():
        return xdg
    tmp = os.environ.get("TMPDIR") or "/tmp"
    uid = os.getuid()
    path = Path(tmp) / f"rgw-{uid}"
    path.mkdir(parents=True, exist_ok=True)
    st = path.stat()
    if st.st_uid != uid:
        raise GatewayConfigError(
            f"runtime dir {path} not owned by uid {uid} (st_uid={st.st_uid})"
        )
    try:
        os.chmod(path, 0o700)
    except OSError as exc:
        raise GatewayConfigError(
            f"cannot chmod 0700 runtime dir {path}: {exc}"
        ) from exc
    return str(path)


# macOS sockaddr_un.sun_path is 104 bytes incl. NUL.
_UNIX_PATH_MAX = 103


def _clamp_unix_path(path: str, hash_u64: int) -> str:
    if len(path.encode()) <= _UNIX_PATH_MAX:
        return path
    # Never place default socks directly in sticky /tmp — use private 0700 dir.
    uid = os.getuid()
    base = Path("/tmp") / f"rgw-{uid}"
    base.mkdir(parents=True, exist_ok=True)
    st = base.stat()
    if st.st_uid != uid:
        raise GatewayConfigError(
            f"clamp runtime dir {base} not owned by uid {uid} (st_uid={st.st_uid})"
        )
    try:
        os.chmod(base, 0o700)
    except OSError as exc:
        raise GatewayConfigError(f"cannot chmod 0700 clamp dir {base}: {exc}") from exc
    short = base / f"{(hash_u64 & 0xFFFFFFFF):08x}.sock"
    return str(short)


def default_unix_path(user: str, system_name: str, url: str, env: str = "Live") -> str:
    """Match Rust ``default_unix_path`` FNV-1a under the private runtime dir."""
    base = runtime_base_dir()
    key = f"{user}|{system_name}|{url}|{canon_env(env)}"
    h = 0xCBF29CE484222325
    for b in key.encode():
        h ^= b
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    path = str(Path(base) / f"rgw-{h}.sock")
    return _clamp_unix_path(path, h)


@dataclass
class GatewayConfig:
    """Fingerprint + listen settings for attaching to a parent gateway."""

    user: str
    system_name: str
    url: str
    env: str = "Live"
    account_id: str = ""
    fcm_id: str = ""
    ib_id: str = ""
    auth_token: str = field(default="", repr=False)
    listen: str | None = None
    auto_spawn: bool = True
    gateway_bin: str | None = None
    spawn_timeout_sec: float = 30.0
    #: Extra env merged into auto-spawn child (e.g. RITHMIC_PASSWORD). Never logged.
    spawn_environ: dict[str, str] | None = field(default=None, repr=False)
    #: Require credential flock held before accepting Ready (disable only for
    # mock-parent unit tests).
    attest_flock: bool = True

    def __post_init__(self) -> None:
        self.user = _require("user", self.user)
        self.system_name = _require("system_name", self.system_name)
        self.url = _require("url", self.url)
        self.env = _require("env", self.env)
        if self.listen is None or not str(self.listen).strip():
            listen = default_unix_path(self.user, self.system_name, self.url, self.env)
            self.listen = f"unix://{listen}"
        else:
            # validate early
            parse_listen_url(str(self.listen))

    @property
    def socket_path(self) -> str:
        assert self.listen is not None
        return parse_listen_url(self.listen)

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> GatewayConfig:
        # Precedence matches SessionConfig / lake: GATEWAY before URL, SYSTEM before
        # SYSTEM_NAME.
        env = environ if environ is not None else os.environ
        url = _env_first(env, "RITHMIC_GATEWAY", "RITHMIC_URL") or (
            "wss://rprotocol.rithmic.com:443"
        )
        system_name = (
            _env_first(env, "RITHMIC_SYSTEM", "RITHMIC_SYSTEM_NAME") or "LucidTrading"
        )
        user = _env_first(env, "RITHMIC_USER", "RITHMIC_USERNAME")
        return cls(
            user=_require("user", user),
            system_name=system_name,
            url=url,
            env=_env_first(env, "RITHMIC_ENV") or "Live",
            account_id=_env_first(env, "RITHMIC_ACCOUNT_ID") or "",
            fcm_id=_env_first(env, "RITHMIC_FCM_ID") or "",
            ib_id=_env_first(env, "RITHMIC_IB_ID") or "",
            auth_token=_env_first(env, "RITHMIC_GATEWAY_AUTH_TOKEN") or "",
            listen=_env_first(env, "RITHMIC_GATEWAY_LISTEN"),
            auto_spawn=(env.get("RITHMIC_GATEWAY_AUTO_SPAWN") or "1").strip().lower()
            not in {"0", "false", "no", "off"},
            gateway_bin=_env_first(env, "RITHMIC_GATEWAY_BIN"),
        )
