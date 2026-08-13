"""Configuration dataclasses for rithmic-nt-connect.

Supports MY046-style env names and rithmic-rs ``RITHMIC_LIVE_*`` / ``RITHMIC_DEMO_*``
prefixes. LucidTrading acceptance maps to Live env + system ``LucidTrading`` + the
production R|Protocol URL. Passwords are never included in ``repr`` or error text.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, MutableMapping

from rithmic_nt_connect.constants import (
    DEFAULT_APP_NAME,
    DEFAULT_APP_VERSION,
    DEFAULT_GATEWAY_URL,
    DEFAULT_SYSTEM_NAME,
    VENUE,
)


class ConfigError(ValueError):
    """Raised when session / client configuration is incomplete or invalid."""


def _require_nonempty(name: str, value: str | None) -> str:
    if value is None or not str(value).strip():
        raise ConfigError(f"missing or empty required config field: {name}")
    return str(value).strip()


def _env_first(mapping: Mapping[str, str], *keys: str) -> str | None:
    for key in keys:
        raw = mapping.get(key)
        if raw is not None and str(raw).strip():
            return str(raw).strip()
    return None


_ENV_TRUTHY = frozenset({"1", "true", "yes", "on"})


def env_truthy(value: str | None, *, default: bool = False) -> bool:
    """Parse a common env flag (``1`` / ``true`` / ``yes`` / ``on``)."""
    if value is None:
        return default
    return value.strip().lower() in _ENV_TRUTHY


def _session_mode_fields(env: Mapping[str, str]) -> dict[str, Any]:
    mode = (_env_first(env, "RITHMIC_SESSION_MODE") or "direct").strip().lower()
    auto_raw = env.get("RITHMIC_GATEWAY_AUTO_SPAWN")
    auto = True if auto_raw is None else env_truthy(auto_raw, default=True)
    return {
        "session_mode": mode,
        "gateway_listen": _env_first(env, "RITHMIC_GATEWAY_LISTEN"),
        "gateway_auto_spawn": auto,
        "gateway_auth_token": _env_first(env, "RITHMIC_GATEWAY_AUTH_TOKEN") or "",
        "gateway_bin": _env_first(env, "RITHMIC_GATEWAY_BIN"),
    }


def load_dotenv(path: str | Path) -> bool:
    """Load ``KEY=VALUE`` lines into ``os.environ`` via ``setdefault``.

    Missing files are ignored. Returns whether ``path`` existed as a file.
    """
    file_path = Path(path)
    if not file_path.is_file():
        return False
    for raw in file_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))
    return True


def load_dotenv_files(
    *paths: str | Path,
    extra_env_var: str = "RITHMIC_CONNECT_DOTENV",
) -> None:
    """Load each path, then optional extra paths from ``extra_env_var`` (``os.pathsep``)."""
    for path in paths:
        load_dotenv(path)
    extra = os.environ.get(extra_env_var, "")
    for part in extra.split(os.pathsep):
        part = part.strip()
        if part:
            load_dotenv(Path(part))


def _redact_secrets(data: MutableMapping[str, Any]) -> dict[str, Any]:
    out = dict(data)
    if "password" in out and out["password"] is not None:
        out["password"] = "***"
    return out


@dataclass
class SessionConfig:
    """Wire-level Rithmic session settings (maps onto ``rithmic-rs`` config)."""

    user: str
    password: str
    system_name: str = DEFAULT_SYSTEM_NAME
    url: str = DEFAULT_GATEWAY_URL
    app_name: str = DEFAULT_APP_NAME
    app_version: str = DEFAULT_APP_VERSION
    env: str = "Live"
    beta_url: str | None = None
    account_id: str | None = None
    fcm_id: str | None = None
    ib_id: str | None = None
    symbol: str | None = None
    exchange: str | None = None
    # Dual-mode session broker (direct = in-process PyO3; gateway = rithmic_gateway client).
    session_mode: str = "direct"
    gateway_listen: str | None = None
    gateway_auto_spawn: bool = True
    gateway_auth_token: str = ""
    gateway_bin: str | None = None

    def __post_init__(self) -> None:
        self.user = _require_nonempty("user", self.user)
        self.password = _require_nonempty("password", self.password)
        self.system_name = _require_nonempty("system_name", self.system_name)
        self.url = _require_nonempty("url", self.url)
        self.app_name = _require_nonempty("app_name", self.app_name)
        self.app_version = _require_nonempty("app_version", self.app_version)
        env = _require_nonempty("env", self.env)
        if env not in {"Live", "Demo", "Test"}:
            raise ConfigError(f"invalid env {env!r}; expected Live, Demo, or Test")
        self.env = env
        mode = (self.session_mode or "direct").strip().lower()
        if mode not in {"direct", "gateway"}:
            raise ConfigError(f"invalid session_mode {self.session_mode!r}; expected direct or gateway")
        self.session_mode = mode
        if self.beta_url is None or not str(self.beta_url).strip():
            self.beta_url = self.url

    def __repr__(self) -> str:
        return (
            "SessionConfig("
            f"user={self.user!r}, password='***', system_name={self.system_name!r}, "
            f"url={self.url!r}, env={self.env!r}, app_name={self.app_name!r}, "
            f"app_version={self.app_version!r}, account_id={self.account_id!r})"
        )

    def has_account(self) -> bool:
        return bool(self.account_id and self.fcm_id and self.ib_id)

    def to_dict(self, *, redact: bool = True) -> dict[str, Any]:
        data = {
            "user": self.user,
            "password": self.password,
            "system_name": self.system_name,
            "url": self.url,
            "beta_url": self.beta_url,
            "app_name": self.app_name,
            "app_version": self.app_version,
            "env": self.env,
            "account_id": self.account_id,
            "fcm_id": self.fcm_id,
            "ib_id": self.ib_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "session_mode": self.session_mode,
            "gateway_listen": self.gateway_listen,
            "gateway_auto_spawn": self.gateway_auto_spawn,
        }
        return _redact_secrets(data) if redact else data

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        prefer_lucid: bool = True,
    ) -> SessionConfig:
        """Load credentials from MY046 or rithmic-rs-style environment variables.

        Priority:
        1. ``RITHMIC_USER`` / ``RITHMIC_PASSWORD`` (MY046)
        2. ``RITHMIC_LIVE_*``
        3. ``RITHMIC_DEMO_*``

        When ``prefer_lucid`` is true (default), MY046 and Live paths use
        ``LucidTrading`` system + production gateway defaults unless overridden.
        """
        env = environ if environ is not None else os.environ

        app_name = _env_first(env, "RITHMIC_APP_NAME") or DEFAULT_APP_NAME
        app_version = _env_first(env, "RITHMIC_APP_VERSION") or DEFAULT_APP_VERSION

        if _env_first(env, "RITHMIC_USER", "RHITMIC_USERNAME", "RITHMIC_USERNAME"):
            user = _require_nonempty(
                "RITHMIC_USER",
                _env_first(env, "RITHMIC_USER", "RHITMIC_USERNAME", "RITHMIC_USERNAME"),
            )
            password = _require_nonempty(
                "RITHMIC_PASSWORD",
                _env_first(env, "RITHMIC_PASSWORD", "RHITMIC_PASSWORD"),
            )
            system_name = (
                _env_first(
                    env,
                    "RITHMIC_SYSTEM",
                    "RITHMIC_SYSTEM_NAME",
                    "RITHMIC_LIVE_SYSTEM_NAME",
                )
                or (DEFAULT_SYSTEM_NAME if prefer_lucid else "Rithmic 01")
            )
            # Prefer production LucidTrading gateway over legacy test URL when prefer_lucid
            url = _env_first(env, "RITHMIC_GATEWAY", "RITHMIC_LIVE_URL")
            if url is None:
                legacy = _env_first(env, "RITHMIC_URL", "RHITMIC_URL")
                if prefer_lucid and legacy and "rituz00100" in legacy:
                    url = DEFAULT_GATEWAY_URL
                    system_name = DEFAULT_SYSTEM_NAME
                else:
                    url = legacy
            if url is None:
                url = DEFAULT_GATEWAY_URL if prefer_lucid else None
            if url is None:
                raise ConfigError("missing RITHMIC_GATEWAY / RITHMIC_LIVE_URL")
            beta_url = _env_first(env, "RITHMIC_LIVE_ALT_URL") or url
            account_id = _env_first(
                env, "RITHMIC_ACCOUNT_ID", "ACCOUNT_ID", "RITHMIC_LIVE_ACCOUNT_ID", "RHITMIC_ACCOUNT_ID"
            )
            fcm_id = _env_first(env, "RITHMIC_FCM_ID", "FCM_ID", "RITHMIC_LIVE_FCM_ID")
            ib_id = _env_first(env, "RITHMIC_IB_ID", "IB_ID", "RITHMIC_LIVE_IB_ID")
            symbol = _env_first(env, "RITHMIC_SYMBOL", "SYMBOL", "TEST_SYMBOL")
            exchange = _env_first(env, "RITHMIC_EXCHANGE", "EXCHANGE", "TEST_EXCHANGE")
            return cls(
                user=user,
                password=password,
                system_name=system_name,
                url=url,
                beta_url=beta_url,
                app_name=app_name,
                app_version=app_version,
                env="Live",
                account_id=account_id,
                fcm_id=fcm_id,
                ib_id=ib_id,
                symbol=symbol,
                exchange=exchange,
                **_session_mode_fields(env),
            )

        if _env_first(env, "RITHMIC_LIVE_USER"):
            user = _require_nonempty("RITHMIC_LIVE_USER", _env_first(env, "RITHMIC_LIVE_USER"))
            password = _require_nonempty("RITHMIC_LIVE_PW", _env_first(env, "RITHMIC_LIVE_PW"))
            system_name = (
                _env_first(env, "RITHMIC_LIVE_SYSTEM_NAME", "RITHMIC_SYSTEM")
                or DEFAULT_SYSTEM_NAME
            )
            url = _require_nonempty(
                "RITHMIC_LIVE_URL",
                _env_first(env, "RITHMIC_LIVE_URL", "RITHMIC_GATEWAY")
                or DEFAULT_GATEWAY_URL,
            )
            beta_url = _env_first(env, "RITHMIC_LIVE_ALT_URL") or url
            return cls(
                user=user,
                password=password,
                system_name=system_name,
                url=url,
                beta_url=beta_url,
                app_name=app_name,
                app_version=app_version,
                env="Live",
                account_id=_env_first(env, "RITHMIC_LIVE_ACCOUNT_ID", "RITHMIC_ACCOUNT_ID"),
                fcm_id=_env_first(env, "RITHMIC_LIVE_FCM_ID", "RITHMIC_FCM_ID"),
                ib_id=_env_first(env, "RITHMIC_LIVE_IB_ID", "RITHMIC_IB_ID"),
                symbol=_env_first(env, "RITHMIC_SYMBOL", "SYMBOL"),
                exchange=_env_first(env, "RITHMIC_EXCHANGE", "EXCHANGE"),
                **_session_mode_fields(env),
            )

        if _env_first(env, "RITHMIC_DEMO_USER"):
            user = _require_nonempty("RITHMIC_DEMO_USER", _env_first(env, "RITHMIC_DEMO_USER"))
            password = _require_nonempty("RITHMIC_DEMO_PW", _env_first(env, "RITHMIC_DEMO_PW"))
            system_name = (
                _env_first(env, "RITHMIC_DEMO_SYSTEM_NAME", "RITHMIC_SYSTEM")
                or "Rithmic Paper Trading"
            )
            url = _require_nonempty(
                "RITHMIC_DEMO_URL", _env_first(env, "RITHMIC_DEMO_URL")
            )
            beta_url = _env_first(env, "RITHMIC_DEMO_ALT_URL") or url
            return cls(
                user=user,
                password=password,
                system_name=system_name,
                url=url,
                beta_url=beta_url,
                app_name=app_name,
                app_version=app_version,
                env="Demo",
                account_id=_env_first(env, "RITHMIC_DEMO_ACCOUNT_ID", "RITHMIC_ACCOUNT_ID"),
                fcm_id=_env_first(env, "RITHMIC_DEMO_FCM_ID", "RITHMIC_FCM_ID"),
                ib_id=_env_first(env, "RITHMIC_DEMO_IB_ID", "RITHMIC_IB_ID"),
                symbol=_env_first(env, "RITHMIC_SYMBOL", "SYMBOL"),
                exchange=_env_first(env, "RITHMIC_EXCHANGE", "EXCHANGE"),
                **_session_mode_fields(env),
            )

        raise ConfigError(
            "missing credentials: set RITHMIC_USER/RITHMIC_PASSWORD "
            "or RITHMIC_LIVE_USER/RITHMIC_LIVE_PW or RITHMIC_DEMO_USER/RITHMIC_DEMO_PW"
        )


@dataclass
class RithmicDataClientConfig:
    """Config for the Phase 1 live market-data client (factories land in U4)."""

    session: SessionConfig
    instrument_ids: list[str] = field(default_factory=list)
    venue: str = VENUE

    def __repr__(self) -> str:
        return (
            f"RithmicDataClientConfig(session={self.session!r}, "
            f"instrument_ids={self.instrument_ids!r}, venue={self.venue!r})"
        )

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> RithmicDataClientConfig:
        session = SessionConfig.from_env(environ)
        instrument_ids: list[str] = []
        if session.symbol and session.exchange:
            instrument_ids.append(f"{session.symbol}.{VENUE}")
        return cls(session=session, instrument_ids=instrument_ids)


try:
    from nautilus_trader.config import LiveDataClientConfig
except ImportError:  # pragma: no cover - nautilus not installed
    RithmicLiveDataClientConfig = None  # type: ignore[misc, assignment]
else:

    class RithmicLiveDataClientConfig(LiveDataClientConfig, frozen=True, kw_only=True):
        """TradingNode-facing data config. Factory loads ``SessionConfig.from_env()``."""


@dataclass
class RithmicExecClientConfig:
    """Config for the Rithmic execution client.

    ``enable_trading=False`` (default) keeps Phase 1 read-only behavior: account/PnL
    only; order actions are rejected. Set ``enable_trading=True`` for Phase 2 order
    routing (requires account_id/fcm_id/ib_id and order-plant authorization).
    """

    session: SessionConfig
    venue: str = VENUE
    soft_fail_pnl: bool = True
    enable_trading: bool = False

    def __repr__(self) -> str:
        return (
            f"RithmicExecClientConfig(session={self.session!r}, "
            f"venue={self.venue!r}, soft_fail_pnl={self.soft_fail_pnl!r}, "
            f"enable_trading={self.enable_trading!r})"
        )

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> RithmicExecClientConfig:
        env = environ if environ is not None else os.environ
        raw = _env_first(env, "RITHMIC_ENABLE_TRADING", "ENABLE_TRADING")
        enable = env_truthy(raw)
        return cls(session=SessionConfig.from_env(environ), enable_trading=enable)
