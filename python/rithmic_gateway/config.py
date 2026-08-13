"""Gateway client configuration (fingerprint + listen URL; never puts password on IPC)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


class GatewayConfigError(ValueError):
    """Invalid gateway client / listen configuration."""


def _require(name: str, value: str | None) -> str:
    if value is None or not str(value).strip():
        raise GatewayConfigError(f"missing or empty required field: {name}")
    return str(value).strip()


def parse_listen_url(raw: str) -> str:
    """Return absolute unix socket path from ``unix://…`` or bare absolute path.

    ``tcp://`` / ``tls://`` are reserved until v2 (see docs/references/gateway-remote.md).
    """
    raw = raw.strip()
    if not raw:
        raise GatewayConfigError("empty gateway listen URL")
    if raw.startswith("tcp://") or raw.startswith("tls://"):
        raise GatewayConfigError(
            f"tcp/tls listen is not implemented yet (see docs/references/gateway-remote.md): {raw}"
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
    raise GatewayConfigError(f"unsupported listen URL scheme (v1 supports unix:// only): {raw}")


def default_unix_path(user: str, system_name: str, url: str) -> str:
    """Match Rust ``default_unix_path`` FNV-1a style hash under XDG_RUNTIME_DIR or /tmp."""
    base = os.environ.get("XDG_RUNTIME_DIR") or "/tmp"
    key = f"{user}|{system_name}|{url}"
    h = 0xCBF29CE484222325
    for b in key.encode():
        h ^= b
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return str(Path(base) / f"rithmic-gateway-{h}.sock")


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
    auth_token: str = ""
    listen: str | None = None
    auto_spawn: bool = True
    gateway_bin: str | None = None
    spawn_timeout_sec: float = 30.0

    def __post_init__(self) -> None:
        self.user = _require("user", self.user)
        self.system_name = _require("system_name", self.system_name)
        self.url = _require("url", self.url)
        self.env = _require("env", self.env)
        if self.listen is None or not str(self.listen).strip():
            self.listen = f"unix://{default_unix_path(self.user, self.system_name, self.url)}"
        else:
            # validate early
            parse_listen_url(str(self.listen))

    @property
    def socket_path(self) -> str:
        assert self.listen is not None
        return parse_listen_url(self.listen)

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> GatewayConfig:
        env = environ if environ is not None else os.environ
        return cls(
            user=_require("user", env.get("RITHMIC_USER")),
            system_name=env.get("RITHMIC_SYSTEM_NAME") or "LucidTrading",
            url=env.get("RITHMIC_URL") or "wss://rprotocol.rithmic.com:443",
            env=env.get("RITHMIC_ENV") or "Live",
            account_id=env.get("RITHMIC_ACCOUNT_ID") or "",
            fcm_id=env.get("RITHMIC_FCM_ID") or "",
            ib_id=env.get("RITHMIC_IB_ID") or "",
            auth_token=env.get("RITHMIC_GATEWAY_AUTH_TOKEN") or "",
            listen=env.get("RITHMIC_GATEWAY_LISTEN"),
            auto_spawn=(env.get("RITHMIC_GATEWAY_AUTO_SPAWN") or "1").strip().lower()
            not in {"0", "false", "no", "off"},
            gateway_bin=env.get("RITHMIC_GATEWAY_BIN"),
        )
