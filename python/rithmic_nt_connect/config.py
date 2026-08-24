"""Configuration dataclasses for rithmic-nt-connect.

Supports MY046-style env names and rithmic-rs ``RITHMIC_LIVE_*`` / ``RITHMIC_DEMO_*``
prefixes. LucidTrading acceptance maps to Live env + system ``LucidTrading`` + the
production R|Protocol URL. Passwords are never included in ``repr`` or error text.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, NamedTuple

from rithmic_nt_connect.constants import (
    DEFAULT_APP_NAME,
    DEFAULT_APP_VERSION,
    DEFAULT_GATEWAY_URL,
    DEFAULT_SYSTEM_NAME,
    VENUE,
)


class ConfigError(ValueError):
    """Raised when session / client configuration is incomplete or invalid."""


class ConnectMode(StrEnum):
    """How this process opens Rithmic (required; no silent default)."""

    DIRECT = "direct"
    GATEWAY = "gateway"


def parse_connect_mode(value: ConnectMode | str) -> ConnectMode:
    """Coerce env / constructor input to ``ConnectMode``."""
    if isinstance(value, ConnectMode):
        return value
    raw = str(value).strip().lower()
    try:
        return ConnectMode(raw)
    except ValueError as exc:
        raise ConfigError(
            f"invalid connect_mode {value!r}; expected 'direct' or 'gateway'"
        ) from exc


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


def _connect_mode_fields(env: Mapping[str, str]) -> dict[str, Any]:
    raw = _env_first(env, "RITHMIC_CONNECT_MODE")
    if raw is None or not str(raw).strip():
        raise ConfigError(
            "missing RITHMIC_CONNECT_MODE; set to 'direct' (in-process plants) "
            "or 'gateway' (shared rithmic-gateway)"
        )
    auto_raw = env.get("RITHMIC_GATEWAY_AUTO_SPAWN")
    auto = True if auto_raw is None else env_truthy(auto_raw, default=True)
    try:
        mode = parse_connect_mode(raw)
    except ConfigError as exc:
        raise ConfigError(
            f"invalid RITHMIC_CONNECT_MODE {raw!r}; expected direct or gateway"
        ) from exc
    return {
        "connect_mode": mode,
        "gateway_listen": _env_first(env, "RITHMIC_GATEWAY_LISTEN"),
        "gateway_auto_spawn": auto,
        "gateway_auth_token": _env_first(env, "RITHMIC_GATEWAY_AUTH_TOKEN") or "",
        "gateway_bin": _env_first(env, "RITHMIC_GATEWAY_BIN"),
    }


def _parse_dotenv(path: str | Path) -> dict[str, str]:
    """Parse ``KEY=VALUE`` lines from ``path`` into a dict (empty when missing)."""
    file_path = Path(path)
    if not file_path.is_file():
        return {}
    values: dict[str, str] = {}
    for raw in file_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'").strip('"')
    return values


def load_dotenv(path: str | Path) -> bool:
    """Load ``KEY=VALUE`` lines into ``os.environ`` via ``setdefault``.

    Missing files are ignored. Returns whether ``path`` existed as a file.
    """
    file_path = Path(path)
    if not file_path.is_file():
        return False
    for key, value in _parse_dotenv(path).items():
        os.environ.setdefault(key, value)
    return True


def load_dotenv_files(
    *paths: str | Path,
    extra_env_var: str = "RITHMIC_CONNECT_DOTENV",
) -> None:
    """Load each path, then optional extra paths from ``extra_env_var``
    (``os.pathsep``).
    """
    for path in paths:
        load_dotenv(path)
    extra = os.environ.get(extra_env_var, "")
    for part in extra.split(os.pathsep):
        part = part.strip()
        if part:
            load_dotenv(Path(part))


# Shared classifier for system names (``explicit_test_env`` + conftest guard).
TEST_SYSTEM_MARKERS = ("TEST", "DEMO", "SANDBOX", "SIM", "PAPER")
PRODUCTION_SYSTEM_MARKERS = ("LUCID", "PRODUCTION", "RITHMIC 01", "RITHMIC 02")


def system_kind(system_name: str) -> str:
    """Classify a system name: ``'production'``, ``'test'``, or ``'unknown'``."""
    upper = system_name.upper()
    if any(marker in upper for marker in PRODUCTION_SYSTEM_MARKERS):
        return "production"
    if any(marker in upper for marker in TEST_SYSTEM_MARKERS):
        return "test"
    return "unknown"


def _dotenv_values(path: str | Path) -> dict[str, str]:
    """Read a dotenv file without mutating the process environment."""
    file_path = Path(path).expanduser()
    if not file_path.is_file():
        raise ConfigError(f"explicit test env file does not exist: {file_path}")
    return _parse_dotenv(file_path)


def explicit_test_env(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Return an isolated test environment from ``RITHMIC_TEST_DOTENV``.

    Live tests must opt into this source explicitly; the repository-root
    ``.env`` is deliberately never consulted here.
    """
    source_env = environ if environ is not None else os.environ
    source = _env_first(source_env, "RITHMIC_TEST_DOTENV")
    if source is None:
        raise ConfigError(
            "missing RITHMIC_TEST_DOTENV; live tests require an explicit test env file"
        )
    values = _dotenv_values(source)
    for key in (
        "RITHMIC_USER",
        "RITHMIC_PASSWORD",
        "RITHMIC_SYSTEM_NAME",
        "RITHMIC_CONNECT_MODE",
    ):
        if not _env_first(values, key):
            raise ConfigError(f"explicit test env file is missing {key}")
    # RITHMIC_CONNECT_MODE is validated non-empty above, so direct indexing is safe.
    mode = parse_connect_mode(values["RITHMIC_CONNECT_MODE"])
    if mode == ConnectMode.GATEWAY and not _env_first(values, "RITHMIC_GATEWAY"):
        # A gateway test env must name its endpoint; a direct env never uses one.
        raise ConfigError("explicit test env file is missing RITHMIC_GATEWAY")
    kind = system_kind(values["RITHMIC_SYSTEM_NAME"])
    # Lucid/production: only when RITHMIC_ALLOW_LUCID_E2E=1 is in process env
    # (never the dotenv file). See ops-runbook LucidTrading override.
    allow_production = kind == "production" and env_truthy(
        _env_first(source_env, "RITHMIC_ALLOW_LUCID_E2E")
    )
    if allow_production:
        print(
            f"WARNING: RITHMIC_ALLOW_LUCID_E2E=1 — running live tests against "
            f"production system {values['RITHMIC_SYSTEM_NAME']!r}; close "
            f"MotiveWave / R|Trader or the credential login will conflict",
            file=sys.stderr,
        )
    elif kind == "production":
        raise ConfigError("explicit test env resolves to a production Rithmic system")
    elif kind != "test":
        raise ConfigError("explicit test env system is not recognized as test/demo")
    trading = _env_first(source_env, "RITHMIC_ENABLE_TRADING")
    if trading is not None:
        values["RITHMIC_ENABLE_TRADING"] = trading
    values["RITHMIC_TEST_DOTENV"] = str(Path(source).expanduser())
    return values


def session_config_from_explicit_test_env(
    environ: Mapping[str, str] | None = None,
) -> SessionConfig:
    """Build a session only from the explicit test env source."""
    return SessionConfig.from_env(explicit_test_env(environ), prefer_lucid=False)


def _redact_secrets(data: MutableMapping[str, Any]) -> dict[str, Any]:
    out = dict(data)
    if "password" in out and out["password"] is not None:
        out["password"] = "***"
    return out


class _EnvSource(NamedTuple):
    """One credential source: env-prefix key sets + defaults for ``from_env``."""

    env: str
    user_keys: tuple[str, ...]
    password_keys: tuple[str, ...]
    system_keys: tuple[str, ...]
    url_keys: tuple[str, ...]
    account_keys: tuple[str, ...]
    fcm_keys: tuple[str, ...]
    ib_keys: tuple[str, ...]
    symbol_keys: tuple[str, ...]
    exchange_keys: tuple[str, ...]
    system_default: str
    url_default: str | None = None
    alt_url_keys: tuple[str, ...] = ()
    legacy_url_keys: tuple[str, ...] = ()
    # MY046-style source defaults to LucidTrading production when ``prefer_lucid``.
    lucid_defaults: bool = False


_ENV_SOURCES = (
    _EnvSource(
        env="Live",
        user_keys=("RITHMIC_USER", "RHITMIC_USERNAME", "RITHMIC_USERNAME"),
        password_keys=("RITHMIC_PASSWORD", "RHITMIC_PASSWORD"),
        system_keys=(
            "RITHMIC_SYSTEM",
            "RITHMIC_SYSTEM_NAME",
            "RITHMIC_LIVE_SYSTEM_NAME",
        ),
        url_keys=("RITHMIC_GATEWAY", "RITHMIC_LIVE_URL"),
        account_keys=(
            "RITHMIC_ACCOUNT_ID",
            "ACCOUNT_ID",
            "RITHMIC_LIVE_ACCOUNT_ID",
            "RHITMIC_ACCOUNT_ID",
        ),
        fcm_keys=("RITHMIC_FCM_ID", "FCM_ID", "RITHMIC_LIVE_FCM_ID"),
        ib_keys=("RITHMIC_IB_ID", "IB_ID", "RITHMIC_LIVE_IB_ID"),
        symbol_keys=("RITHMIC_SYMBOL", "SYMBOL", "TEST_SYMBOL"),
        exchange_keys=("RITHMIC_EXCHANGE", "EXCHANGE", "TEST_EXCHANGE"),
        system_default="Rithmic 01",
        alt_url_keys=("RITHMIC_LIVE_ALT_URL",),
        legacy_url_keys=("RITHMIC_URL", "RHITMIC_URL"),
        lucid_defaults=True,
    ),
    _EnvSource(
        env="Live",
        user_keys=("RITHMIC_LIVE_USER",),
        password_keys=("RITHMIC_LIVE_PW",),
        system_keys=("RITHMIC_LIVE_SYSTEM_NAME", "RITHMIC_SYSTEM"),
        url_keys=("RITHMIC_LIVE_URL", "RITHMIC_GATEWAY"),
        account_keys=("RITHMIC_LIVE_ACCOUNT_ID", "RITHMIC_ACCOUNT_ID"),
        fcm_keys=("RITHMIC_LIVE_FCM_ID", "RITHMIC_FCM_ID"),
        ib_keys=("RITHMIC_LIVE_IB_ID", "RITHMIC_IB_ID"),
        symbol_keys=("RITHMIC_SYMBOL", "SYMBOL"),
        exchange_keys=("RITHMIC_EXCHANGE", "EXCHANGE"),
        system_default=DEFAULT_SYSTEM_NAME,
        url_default=DEFAULT_GATEWAY_URL,
        alt_url_keys=("RITHMIC_LIVE_ALT_URL",),
    ),
    _EnvSource(
        env="Demo",
        user_keys=("RITHMIC_DEMO_USER",),
        password_keys=("RITHMIC_DEMO_PW",),
        system_keys=("RITHMIC_DEMO_SYSTEM_NAME", "RITHMIC_SYSTEM"),
        url_keys=("RITHMIC_DEMO_URL",),
        account_keys=("RITHMIC_DEMO_ACCOUNT_ID", "RITHMIC_ACCOUNT_ID"),
        fcm_keys=("RITHMIC_DEMO_FCM_ID", "RITHMIC_FCM_ID"),
        ib_keys=("RITHMIC_DEMO_IB_ID", "RITHMIC_IB_ID"),
        symbol_keys=("RITHMIC_SYMBOL", "SYMBOL"),
        exchange_keys=("RITHMIC_EXCHANGE", "EXCHANGE"),
        system_default="Rithmic Paper Trading",
        alt_url_keys=("RITHMIC_DEMO_ALT_URL",),
    ),
)


@dataclass
class SessionConfig:
    """Wire-level Rithmic session settings (maps onto ``rithmic-rs`` config)."""

    user: str
    password: str
    # Required: how this process opens Rithmic (no silent default).
    connect_mode: ConnectMode
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
        self.connect_mode = parse_connect_mode(self.connect_mode)
        if self.beta_url is None or not str(self.beta_url).strip():
            self.beta_url = self.url

    def __repr__(self) -> str:
        return (
            "SessionConfig("
            f"user={self.user!r}, password='***', system_name={self.system_name!r}, "
            f"url={self.url!r}, env={self.env!r}, "
            f"connect_mode={self.connect_mode.value!r}, "
            f"app_name={self.app_name!r}, app_version={self.app_version!r}, "
            f"account_id={self.account_id!r})"
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
            "connect_mode": self.connect_mode.value,
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

        When ``prefer_lucid`` is true (default), the MY046 path uses
        ``LucidTrading`` system + production gateway defaults unless overridden.
        """
        env = environ if environ is not None else os.environ
        app_name = _env_first(env, "RITHMIC_APP_NAME") or DEFAULT_APP_NAME
        app_version = _env_first(env, "RITHMIC_APP_VERSION") or DEFAULT_APP_VERSION

        for source in _ENV_SOURCES:
            user = _env_first(env, *source.user_keys)
            if user is None:
                continue
            lucid = prefer_lucid and source.lucid_defaults
            user = _require_nonempty(source.user_keys[0], user)
            password = _require_nonempty(
                source.password_keys[0], _env_first(env, *source.password_keys)
            )
            system_name = _env_first(env, *source.system_keys) or (
                DEFAULT_SYSTEM_NAME if lucid else source.system_default
            )
            url = _env_first(env, *source.url_keys)
            if url is None and source.legacy_url_keys:
                legacy = _env_first(env, *source.legacy_url_keys)
                if lucid and legacy and "rituz00100" in legacy:
                    url = DEFAULT_GATEWAY_URL
                    system_name = DEFAULT_SYSTEM_NAME
                else:
                    url = legacy
            if url is None:
                url = DEFAULT_GATEWAY_URL if lucid else source.url_default
            if url is None:
                raise ConfigError(f"missing {' / '.join(source.url_keys)}")
            return cls(
                user=user,
                password=password,
                system_name=system_name,
                url=url,
                beta_url=_env_first(env, *source.alt_url_keys) or url,
                app_name=app_name,
                app_version=app_version,
                env=source.env,
                account_id=_env_first(env, *source.account_keys),
                fcm_id=_env_first(env, *source.fcm_keys),
                ib_id=_env_first(env, *source.ib_keys),
                symbol=_env_first(env, *source.symbol_keys),
                exchange=_env_first(env, *source.exchange_keys),
                **_connect_mode_fields(env),
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
    def from_env(
        cls, environ: Mapping[str, str] | None = None
    ) -> RithmicDataClientConfig:
        session = SessionConfig.from_env(environ)
        instrument_ids: list[str] = []
        if session.symbol and session.exchange:
            instrument_ids.append(f"{session.symbol}.{VENUE}")
        return cls(session=session, instrument_ids=instrument_ids)


try:
    from nautilus_trader.config import LiveDataClientConfig

    _LIVE_DATA_CONFIG_BASE: Any = LiveDataClientConfig
except ImportError:  # pragma: no cover - nautilus not installed
    _LIVE_DATA_CONFIG_BASE = None

if _LIVE_DATA_CONFIG_BASE is not None:

    class RithmicLiveDataClientConfig(
        _LIVE_DATA_CONFIG_BASE, frozen=True, kw_only=True
    ):
        """TradingNode-facing data config. Factory loads
        ``SessionConfig.from_env()``.
        """

        session: SessionConfig | None = None


try:
    from nautilus_trader.config import LiveExecClientConfig

    _LIVE_EXEC_CONFIG_BASE: Any = LiveExecClientConfig
except ImportError:  # pragma: no cover - nautilus not installed
    _LIVE_EXEC_CONFIG_BASE = None

if _LIVE_EXEC_CONFIG_BASE is not None:
    # msgspec ``frozen=True`` (inherited from ``NautilusConfig``); stated here so
    # the class is self-describing and checkers don't flag the subclass.
    class RithmicLiveExecClientConfig(
        _LIVE_EXEC_CONFIG_BASE, kw_only=True, frozen=True
    ):
        """TradingNode-facing exec config. Factory loads ``SessionConfig.from_env()``.

        Adds the Rithmic ``session`` + ``enable_trading`` knobs on top of the base
        ``LiveExecClientConfig`` so the client can be registered on a ``TradingNode``.
        """

        session: SessionConfig | None = None
        enable_trading: bool = False
        soft_fail_pnl: bool = True


def __getattr__(name: str) -> Any:
    """Return ``None`` for the live config classes when Nautilus is absent.

    The classes are only defined when ``nautilus_trader`` imports. A ``= None``
    fallback branch beside the class would make type checkers union ``None`` into
    the class name's type, so the fallback lives in this module ``__getattr__``.
    """
    if name in ("RithmicLiveDataClientConfig", "RithmicLiveExecClientConfig"):
        return None
    raise AttributeError(name)


@dataclass
class RithmicExecClientConfig:
    """Config for the Rithmic execution client.

    ``enable_trading=False`` (default) keeps Phase 1 read-only behavior: account/PnL
    only; order actions are denied. Set ``enable_trading=True`` for Phase 2 order
    routing (account auto-discovered when unset; live place still needs authorization).
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
    def from_env(
        cls, environ: Mapping[str, str] | None = None
    ) -> RithmicExecClientConfig:
        env = environ if environ is not None else os.environ
        raw = _env_first(env, "RITHMIC_ENABLE_TRADING", "ENABLE_TRADING")
        enable = env_truthy(raw)
        return cls(session=SessionConfig.from_env(environ), enable_trading=enable)
