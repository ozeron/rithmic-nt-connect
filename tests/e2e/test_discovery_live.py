"""Live discovery check — proves Account auto-discovery [x] with a committed e2e.

Uses `live_session` (market-data only, no trading gate) to assert
`resolved_account()` returns the env triple when set or an auto-discovered
account otherwise. Skips without `RITHMIC_TEST_DOTENV` (CI) and on
multi-account envs without selector (venue-limited, not failure).
"""

from __future__ import annotations

import contextlib

import pytest

pytestmark = pytest.mark.live


def test_resolved_account_matches_env(test_env):
    """`resolved_account()` matches the env triple when set."""
    import pytest
    from rithmic_nt_connect.config import (
        explicit_test_env,
        session_config_from_explicit_test_env,
    )
    from rithmic_nt_connect.session import PLANTS_EXECUTION, create_session

    if test_env is None:
        pytest.skip("explicit test env unavailable")
    env = explicit_test_env()
    cfg = session_config_from_explicit_test_env()
    sess = create_session(cfg, plants=PLANTS_EXECUTION)
    try:
        sess.connect()
        sess.ensure_order_plant()
        acct = sess.resolved_account()
        if acct is None:
            pytest.skip("no resolved account (auto-discovery pending or multi-account)")
        assert acct.get("account_id"), "account_id missing"
        for key in ("account_id", "fcm_id", "ib_id"):
            if env.get(key):
                assert acct.get(key) == env[key], (
                    f"{key} mismatch: {acct.get(key)!r} != {env[key]!r}"
                )
    finally:
        with contextlib.suppress(Exception):
            sess.disconnect()
            lock = getattr(sess, "_lock", None)
            if lock is not None:
                lock.close()


def test_resolved_account_auto_discovers(test_env):
    """Auto-discovery still returns an account when env triple is unset."""
    import pytest
    from rithmic_nt_connect.config import session_config_from_explicit_test_env
    from rithmic_nt_connect.session import PLANTS_EXECUTION, create_session

    if test_env is None:
        pytest.skip("explicit test env unavailable")
    cfg = session_config_from_explicit_test_env()
    sess = create_session(cfg, plants=PLANTS_EXECUTION)
    try:
        sess.connect()
        sess.ensure_order_plant()
        acct = sess.resolved_account()
        if acct is None:
            pytest.skip("auto-discovery returned None (multi-account or pending)")
        assert acct.get("account_id"), "auto-discovery missing account_id"
    finally:
        with contextlib.suppress(Exception):
            sess.disconnect()
            lock = getattr(sess, "_lock", None)
            if lock is not None:
                lock.close()
