"""Live e2e: RMS commission rates round-trip on the explicit test account.

Mechanism assertions only — never specific venue rates. The test plant may
publish ``0.0`` / unset rates (that is fine); the point is that the fetch path
works and returns the typed row shape on both connect modes:

- **direct session**: ``load_product_rms_info`` / ``load_account_rms_info``
  through the PyO3 plants session (order-plant login, read-only query).
- **gateway wire**: ``GatewayClient`` over a parent the harness starts
  (``scripts/_gateway_parent.py``; the client never spawns). The RPC rides the
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
from _gateway_parent import running_parent
from rithmic_gateway import GatewayClient, GatewayError
from rithmic_nt_connect.config import env_truthy, session_config_from_explicit_test_env
from rithmic_nt_connect.gateway_wire import gateway_config_from_session


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
    # Only the parent holds the password; it also needs the trading flag in its
    # env. Honor an explicit process-env override so the command line can enable
    # the fetch without editing the test env file.
    trading = env_truthy(
        os.environ.get("RITHMIC_ENABLE_TRADING")
        or (test_env or {}).get("RITHMIC_ENABLE_TRADING")
    )
    parent_extra_env = {"RITHMIC_ENABLE_TRADING": "1"} if trading else {}
    password = str((test_env or {}).get("RITHMIC_PASSWORD") or "")
    if not password:
        pytest.skip("test env has no RITHMIC_PASSWORD for the gateway parent")

    gcfg = gateway_config_from_session(session_cfg)

    with running_parent(gcfg, password=password, extra_env=parent_extra_env):
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
                assert product, (
                    "expected at least one product RMS row on the test account"
                )
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
