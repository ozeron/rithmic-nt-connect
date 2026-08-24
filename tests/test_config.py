"""Unit tests for SessionConfig / client config env mapping."""

from __future__ import annotations

import importlib
import os

import pytest
from rithmic_nt_connect import VENUE, ConfigError, ConnectMode, SessionConfig
from rithmic_nt_connect.config import RithmicDataClientConfig, RithmicExecClientConfig
from rithmic_nt_connect.constants import (
    DEFAULT_APP_NAME,
    DEFAULT_GATEWAY_URL,
    DEFAULT_SYSTEM_NAME,
)


def test_my046_env_maps_to_lucid_live() -> None:
    cfg = SessionConfig.from_env(
        {
            "RITHMIC_USER": "alice",
            "RITHMIC_PASSWORD": "s3cret-value",
            "RITHMIC_SYSTEM": "LucidTrading",
            "RITHMIC_GATEWAY": "wss://rprotocol.rithmic.com:443",
            "RITHMIC_APP_NAME": "rithmic-nt-connect",
            "RITHMIC_APP_VERSION": "0.1.0",
            "RITHMIC_SYMBOL": "NQ",
            "RITHMIC_EXCHANGE": "CME",
            "RITHMIC_CONNECT_MODE": "direct",
        }
    )
    assert cfg.user == "alice"
    assert cfg.password == "s3cret-value"
    assert cfg.system_name == DEFAULT_SYSTEM_NAME
    assert cfg.url == DEFAULT_GATEWAY_URL
    assert cfg.env == "Live"
    assert cfg.symbol == "NQ"
    assert cfg.exchange == "CME"


def test_my046_defaults_system_and_gateway() -> None:
    cfg = SessionConfig.from_env(
        {
            "RITHMIC_USER": "alice",
            "RITHMIC_PASSWORD": "pw",
            "RITHMIC_CONNECT_MODE": "direct",
        }
    )
    assert cfg.system_name == "LucidTrading"
    assert cfg.url == "wss://rprotocol.rithmic.com:443"
    assert cfg.env == "Live"
    assert cfg.app_name == DEFAULT_APP_NAME == "rithmic-nt-connect"


def test_live_rs_style_env() -> None:
    cfg = SessionConfig.from_env(
        {
            "RITHMIC_LIVE_USER": "live_user",
            "RITHMIC_LIVE_PW": "live_pw",
            "RITHMIC_LIVE_URL": "wss://rprotocol.rithmic.com:443",
            "RITHMIC_LIVE_ALT_URL": "wss://alt.example:443",
            "RITHMIC_LIVE_SYSTEM_NAME": "LucidTrading",
            "RITHMIC_APP_NAME": "app",
            "RITHMIC_APP_VERSION": "1",
            "RITHMIC_CONNECT_MODE": "direct",
        }
    )
    assert cfg.env == "Live"
    assert cfg.user == "live_user"
    assert cfg.system_name == "LucidTrading"
    assert cfg.beta_url == "wss://alt.example:443"


def test_demo_rs_style_env() -> None:
    cfg = SessionConfig.from_env(
        {
            "RITHMIC_DEMO_USER": "demo_user",
            "RITHMIC_DEMO_PW": "demo_pw",
            "RITHMIC_DEMO_URL": "wss://demo.example:443",
            "RITHMIC_APP_NAME": "app",
            "RITHMIC_APP_VERSION": "1",
            "RITHMIC_CONNECT_MODE": "direct",
        }
    )
    assert cfg.env == "Demo"
    assert cfg.user == "demo_user"
    assert cfg.system_name == "Rithmic Paper Trading"


def test_missing_credentials_clear_error_no_password_echo() -> None:
    with pytest.raises(ConfigError) as exc:
        SessionConfig.from_env({})
    msg = str(exc.value)
    assert "missing credentials" in msg.lower() or "RITHMIC_USER" in msg
    assert "s3cret" not in msg


def test_empty_user_rejected_without_echoing_password() -> None:
    with pytest.raises(ConfigError) as exc:
        SessionConfig(
            user="", password="top-secret-password", connect_mode=ConnectMode.DIRECT
        )
    msg = str(exc.value)
    assert "user" in msg
    assert "top-secret-password" not in msg


def test_repr_redacts_password() -> None:
    cfg = SessionConfig(
        user="u", password="top-secret-password", connect_mode=ConnectMode.DIRECT
    )
    text = repr(cfg)
    assert "top-secret-password" not in text
    assert "***" in text
    assert "connect_mode='direct'" in text


def test_to_dict_redacts_by_default() -> None:
    cfg = SessionConfig(
        user="u", password="top-secret-password", connect_mode=ConnectMode.DIRECT
    )
    assert cfg.to_dict()["password"] == "***"
    assert cfg.to_dict(redact=False)["password"] == "top-secret-password"
    assert cfg.to_dict()["connect_mode"] == "direct"
    assert cfg.connect_mode is ConnectMode.DIRECT


def test_data_and_exec_config_from_env() -> None:
    env = {
        "RITHMIC_USER": "alice",
        "RITHMIC_PASSWORD": "pw",
        "RITHMIC_SYMBOL": "NQ",
        "RITHMIC_EXCHANGE": "CME",
        "RITHMIC_CONNECT_MODE": "direct",
    }
    data = RithmicDataClientConfig.from_env(env)
    exec_cfg = RithmicExecClientConfig.from_env(env)
    assert data.venue == VENUE
    assert data.instrument_ids == [f"NQ-CME.{VENUE}"]
    assert exec_cfg.session.user == "alice"
    assert "pw" not in repr(data)
    assert "pw" not in repr(exec_cfg)


def test_env_truthy() -> None:
    from rithmic_nt_connect.config import env_truthy

    assert env_truthy("1")
    assert env_truthy("TRUE")
    assert not env_truthy(None)
    assert not env_truthy("0")
    assert env_truthy(None, default=True)


def test_load_dotenv_setdefault(tmp_path, monkeypatch) -> None:
    from rithmic_nt_connect.config import load_dotenv, load_dotenv_files

    env_file = tmp_path / ".env"
    env_file.write_text("RITHMIC_USER=alice\n# comment\nRITHMIC_PASSWORD='s3cret'\n")
    monkeypatch.delenv("RITHMIC_USER", raising=False)
    monkeypatch.delenv("RITHMIC_PASSWORD", raising=False)
    monkeypatch.setenv("RITHMIC_USER", "keep-me")
    assert load_dotenv(env_file) is True
    assert os.environ["RITHMIC_USER"] == "keep-me"
    assert os.environ["RITHMIC_PASSWORD"] == "s3cret"
    assert load_dotenv(tmp_path / "missing.env") is False

    extra = tmp_path / "extra.env"
    extra.write_text("RITHMIC_SYMBOL=NQ\n")
    monkeypatch.delenv("RITHMIC_SYMBOL", raising=False)
    monkeypatch.setenv("RITHMIC_CONNECT_DOTENV", str(extra))
    load_dotenv_files(tmp_path / "missing.env")
    assert os.environ["RITHMIC_SYMBOL"] == "NQ"


def test_explicit_test_env_requires_source_and_never_uses_root_env(
    tmp_path, monkeypatch
) -> None:
    from rithmic_nt_connect.config import ConfigError, explicit_test_env

    monkeypatch.delenv("RITHMIC_TEST_DOTENV", raising=False)
    monkeypatch.setenv("RITHMIC_USER", "production-user")
    with pytest.raises(ConfigError, match="RITHMIC_TEST_DOTENV"):
        explicit_test_env()

    env_file = tmp_path / "rithmic-test.env"
    env_file.write_text(
        "RITHMIC_USER=test-user\n"
        "RITHMIC_PASSWORD=test-password\n"
        "RITHMIC_SYSTEM_NAME=Rithmic Test\n"
        "RITHMIC_GATEWAY=wss://test.example\n"
        "RITHMIC_CONNECT_MODE=direct\n"
    )
    monkeypatch.setenv("RITHMIC_TEST_DOTENV", str(env_file))
    values = explicit_test_env()
    assert values["RITHMIC_USER"] == "test-user"
    assert values["RITHMIC_SYSTEM_NAME"] == "Rithmic Test"
    assert values["RITHMIC_USER"] != os.environ["RITHMIC_USER"]
    from rithmic_nt_connect.config import SessionConfig

    assert "test-password" not in repr(
        SessionConfig.from_env(values, prefer_lucid=False)
    )


def test_explicit_test_env_gateway_required_only_for_gateway_mode(
    tmp_path, monkeypatch
) -> None:
    from rithmic_nt_connect.config import ConfigError, explicit_test_env

    env_file = tmp_path / "direct.env"
    env_file.write_text(
        "RITHMIC_USER=u\nRITHMIC_PASSWORD=p\n"
        "RITHMIC_SYSTEM_NAME=Rithmic Test\n"
        "RITHMIC_CONNECT_MODE=direct\n"
    )
    monkeypatch.setenv("RITHMIC_TEST_DOTENV", str(env_file))
    values = explicit_test_env()
    assert values["RITHMIC_CONNECT_MODE"] == "direct"
    assert "RITHMIC_GATEWAY" not in values

    gateway_file = tmp_path / "gateway.env"
    gateway_file.write_text(
        "RITHMIC_USER=u\nRITHMIC_PASSWORD=p\n"
        "RITHMIC_SYSTEM_NAME=Rithmic Test\n"
        "RITHMIC_CONNECT_MODE=gateway\n"
    )
    monkeypatch.setenv("RITHMIC_TEST_DOTENV", str(gateway_file))
    with pytest.raises(ConfigError, match="RITHMIC_GATEWAY"):
        explicit_test_env()


def test_explicit_test_env_rejects_production_system(tmp_path, monkeypatch) -> None:
    from rithmic_nt_connect.config import ConfigError, explicit_test_env

    env_file = tmp_path / "production.env"
    env_file.write_text(
        "RITHMIC_USER=u\nRITHMIC_PASSWORD=p\n"
        "RITHMIC_SYSTEM_NAME=LucidTrading\n"
        "RITHMIC_GATEWAY=wss://prod.example\nRITHMIC_CONNECT_MODE=direct\n"
    )
    monkeypatch.setenv("RITHMIC_TEST_DOTENV", str(env_file))
    with pytest.raises(ConfigError, match="production"):
        explicit_test_env()


def test_explicit_test_env_lucid_override_requires_process_env_flag(
    tmp_path, monkeypatch, capsys
) -> None:
    """Lucid override requires RITHMIC_ALLOW_LUCID_E2E=1 in process env."""
    from rithmic_nt_connect.config import ConfigError, explicit_test_env

    env_file = tmp_path / "lucid.env"
    env_file.write_text(
        "RITHMIC_USER=u\nRITHMIC_PASSWORD=p\n"
        "RITHMIC_SYSTEM_NAME=LucidTrading\n"
        "RITHMIC_GATEWAY=wss://prod.example\nRITHMIC_CONNECT_MODE=direct\n"
        "RITHMIC_ALLOW_LUCID_E2E=1\n"  # inside the file: deliberately ignored
    )
    monkeypatch.setenv("RITHMIC_TEST_DOTENV", str(env_file))

    with pytest.raises(ConfigError, match="production"):
        explicit_test_env()

    monkeypatch.setenv("RITHMIC_ALLOW_LUCID_E2E", "1")
    values = explicit_test_env()
    assert values["RITHMIC_SYSTEM_NAME"] == "LucidTrading"
    captured = capsys.readouterr()
    assert "RITHMIC_ALLOW_LUCID_E2E=1" in captured.err
    assert "MotiveWave" in captured.err


def test_explicit_test_env_unknown_system_still_refused(tmp_path, monkeypatch) -> None:
    """Unknown system names stay refused even with the Lucid override."""
    from rithmic_nt_connect.config import ConfigError, explicit_test_env

    env_file = tmp_path / "weird.env"
    env_file.write_text(
        "RITHMIC_USER=u\nRITHMIC_PASSWORD=p\n"
        "RITHMIC_SYSTEM_NAME=MysterySystem\n"
        "RITHMIC_CONNECT_MODE=direct\n"
    )
    monkeypatch.setenv("RITHMIC_TEST_DOTENV", str(env_file))
    monkeypatch.setenv("RITHMIC_ALLOW_LUCID_E2E", "1")
    with pytest.raises(ConfigError, match="not recognized"):
        explicit_test_env()


def test_package_imports_without_network() -> None:
    import rithmic_nt_connect

    assert rithmic_nt_connect.VENUE == "RITHMIC"
    assert rithmic_nt_connect.__version__
    # The legacy package name is gone; assert it raises at runtime without a
    # static import (which a checker would flag as unresolvable).
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("rithmic_connect")


def test_live_data_client_config_is_nautilus_config() -> None:
    from nautilus_trader.config import LiveDataClientConfig
    from rithmic_nt_connect.config import RithmicLiveDataClientConfig

    cfg = RithmicLiveDataClientConfig()
    assert isinstance(cfg, LiveDataClientConfig)
