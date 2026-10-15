"""Characterize venue book refreshes (task 4.2c); no orders or outcome policy.

Run on a Test account with CME L2 entitlement and MotiveWave/R|Trader closed:
  RITHMIC_TEST_DOTENV=.env uv run pytest \\
      tests/e2e/test_book_resubscribe_snapshot_live.py -v -s

Use a direct, pre-wave-5 extension: its Session calls RithmicVenue directly.
A SessionHost may turn repeat subscribe into unsubscribe + subscribe, which
would measure host policy rather than the venue's repeat-subscribe behavior.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from rithmic_nt_connect.config import ConnectMode, parse_connect_mode
from rithmic_nt_connect.session import WireSession

pytestmark = [pytest.mark.live, pytest.mark.slow]

_SNAPSHOT_TIMEOUT_SEC = 20.0


@pytest.fixture
def venue_session(test_env, require_test_plant, request) -> WireSession:
    """Check guards before asking the existing fixture to open its one login."""
    if parse_connect_mode(test_env["RITHMIC_CONNECT_MODE"]) != ConnectMode.DIRECT:
        pytest.skip("venue characterization needs RITHMIC_CONNECT_MODE=direct")
    from rithmic_nt_connect._lib import Session

    if hasattr(Session, "subscribe_session_events"):
        pytest.skip(
            "needs the pre-wave-5 venue path: SessionHost refreshes repeat book "
            "intents; a raw venue hook is required after the host switch"
        )
    return request.getfixturevalue("live_session")


def _observe_snapshots(session: WireSession, symbol: str, exchange: str) -> int:
    """Observe a full bounded window, consuming every row of any snapshot.

    At this wire boundary F_SNAPSHOT is represented by venue update_type=3;
    Nautilus flags are added later by the conversion boundary.
    """
    snapshots = 0
    deadline = time.monotonic() + _SNAPSHOT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        event = session.poll_event(timeout_ms=100)
        if (
            event
            and event.get("type") == "order_book"
            and event.get("symbol") == symbol
            and event.get("exchange") == exchange
            and event.get("update_type") == 3
        ):
            snapshots += 1
    return snapshots


def test_book_resubscribe_snapshot(venue_session: WireSession, request) -> None:
    """Record both outcomes; require an initial snapshot to make the probe valid."""
    _, symbol, exchange = request.getfixturevalue("live_front_month")
    live_session = venue_session
    subscribed = False
    try:
        try:
            live_session.subscribe_order_book_summary(symbol, exchange)
            subscribed = True
        except RuntimeError as exc:
            if "permission denied" in str(exc) or "not entitled" in str(exc):
                pytest.skip(
                    "CME L2 book entitlement is unavailable on this Test account"
                )
            raise

        initial = _observe_snapshots(live_session, symbol, exchange)
        assert initial, (
            "no initial update_type=3 snapshot within 20s; probe is inconclusive"
        )

        # Deliberately no unsubscribe here: this is the venue behavior in question.
        live_session.subscribe_order_book_summary(symbol, exchange)
        repeated = _observe_snapshots(live_session, symbol, exchange)

        live_session.unsubscribe_order_book_summary(symbol, exchange)
        live_session.subscribe_order_book_summary(symbol, exchange)
        resubscribed = _observe_snapshots(live_session, symbol, exchange)

        verdict = (
            f"repeat subscribe resends snapshot: {'yes' if repeated else 'no'}; "
            f"unsub+sub resends: {'yes' if resubscribed else 'no'}"
        )
        print(verdict)
        # artifacts/ is ignored at every depth by the repository .gitignore.
        artifact = (
            Path(__file__).parent / "artifacts" / "book_resubscribe_snapshot.json"
        )
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text(
            json.dumps(
                {
                    "recorded_at": datetime.now(UTC).isoformat(),
                    "symbol": symbol,
                    "exchange": exchange,
                    "connect_mode": "direct",
                    "layer": "RithmicVenue (pre-wave-5)",
                    "window_seconds": _SNAPSHOT_TIMEOUT_SEC,
                    "initial_snapshot_rows": initial,
                    "repeat_snapshot_rows": repeated,
                    "unsub_sub_snapshot_rows": resubscribed,
                    "repeat_subscribe_resends_snapshot": bool(repeated),
                    "unsub_sub_resends_snapshot": bool(resubscribed),
                    "verdict": verdict,
                },
                indent=2,
            )
            + "\n"
        )
        print(f"snapshot probe saved: {artifact}")
    finally:
        if subscribed:
            live_session.unsubscribe_order_book_summary(symbol, exchange)
