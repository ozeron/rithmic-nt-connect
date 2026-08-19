"""Live e2e: RMS commission rates round-trip on the explicit test account.

Mechanism assertions only — never specific venue rates. The test plant may
publish ``0.0`` / unset rates (that is fine); the point is that the fetch path
works and returns the typed row shape on both connect modes:

- **direct session**: ``load_product_rms_info`` / ``load_account_rms_info``
  through the PyO3 plants session (order-plant login, read-only query).
- **gateway wire**: ``GatewayClient`` over a spawned parent. The RPC rides the
  order-plant trading gate, so with ``RITHMIC_ENABLE_TRADING=1`` the fetch
  round-trips and without it the parent must refuse with ``trading_disabled``.

Run (test plant only; refuses production systems)::

    RITHMIC_TEST_DOTENV=.env RITHMIC_ENABLE_TRADING=1 \\
        RITHMIC_GATEWAY_BIN=target/release/rithmic-gateway \\
        uv run pytest tests/e2e/test_rms_commission_live.py -v

``RITHMIC_ENABLE_TRADING=1`` is optional — without it the gateway test asserts
the trading gate instead of the fetch. The gateway binary must include the RMS
RPC (rebuild ``cargo build --release -p rithmic-gateway``); a stale binary
fails the gateway test.
"""

from __future__ import annotations

import contextlib
import os

import pytest
from rithmic_gateway import GatewayClient, GatewayConfig, GatewayError
from rithmic_gateway.spawn import resolve_gateway_bin
from rithmic_nt_connect.config import env_truthy, session_config_from_explicit_test_env


def _assert_product_rows(rows: list[dict[str, object]]) -> None:
    """Typed row shape — never specific rates (0.0 / unset are valid)."""
    assert isinstance(rows, list)
    for row in rows:
        code = row.get("product_code")
        assert code is None or isinstance(code, str)
        rate = row.get("commission_fill_rate")
        if rate is not None:
            assert isinstance(rate, float) and rate >= 0.0


def _assert_account_rows(rows: list[dict[str, object]]) -> None:
    assert isinstance(rows, list)
    for row in rows:
        acct = row.get("account_id")
        assert acct is None or isinstance(acct, str)
        rate = row.get("default_commission")
        if rate is not None:
            assert isinstance(rate, float) and rate >= 0.0


def test_direct_rms_round_trip(live_session, require_test_plant) -> None:
    """The direct PyO3 session fetch round-trips typed rows (any rates)."""
    product = live_session.load_product_rms_info()
    _assert_product_rows(product)
    assert product, "expected at least one product RMS row on the test account"

    account = live_session.load_account_rms_info()
    _assert_account_rows(account)


def test_gateway_rms_round_trip(
    test_env, require_test_plant, credentials_available
) -> None:
    """The gateway wire fetch round-trips (trading on) or the gate holds."""
    if not credentials_available:
        pytest.skip(
            "RITHMIC_TEST_DOTENV with test credentials is not set (safe skip for CI)"
        )

    session_cfg = session_config_from_explicit_test_env()
    # The parent needs the password + trading flag in its spawn env; honor an
    # explicit process-env override so the command line can enable the fetch
    # without editing the test env file.
    trading = env_truthy(
        os.environ.get("RITHMIC_ENABLE_TRADING")
        or (test_env or {}).get("RITHMIC_ENABLE_TRADING")
    )
    spawn_env: dict[str, str] = {}
    if (test_env or {}).get("RITHMIC_PASSWORD"):
        spawn_env["RITHMIC_PASSWORD"] = str(test_env["RITHMIC_PASSWORD"])
    if trading:
        spawn_env["RITHMIC_ENABLE_TRADING"] = "1"

    gcfg = GatewayConfig(
        user=session_cfg.user,
        system_name=session_cfg.system_name,
        url=session_cfg.url,
        env=str(session_cfg.env),
        account_id=session_cfg.account_id or "",
        fcm_id=session_cfg.fcm_id or "",
        ib_id=session_cfg.ib_id or "",
        auth_token=getattr(session_cfg, "gateway_auth_token", None) or "",
        listen=getattr(session_cfg, "gateway_listen", None),
        auto_spawn=True,
        gateway_bin=resolve_gateway_bin(),
        spawn_environ=spawn_env,
    )

    client = GatewayClient(gcfg)
    try:
        client.connect()
    except GatewayError as exc:
        pytest.fail(f"gateway connect failed: {exc.code}: {exc.message}")

    try:
        if trading:
            assert client.trading_enabled
            product = client.load_product_rms_info()
            _assert_product_rows(product)
            assert product, "expected at least one product RMS row on the test account"
            account = client.load_account_rms_info()
            _assert_account_rows(account)
        else:
            # Gate honesty: without parent trading the order-plant RPC is
            # refused with a clean typed error, never a hang or crash.
            with pytest.raises(GatewayError) as excinfo:
                client.load_product_rms_info()
            assert excinfo.value.code == "trading_disabled"
    finally:
        with contextlib.suppress(Exception):
            client.disconnect()
