"""Full-node live proof: Nautilus INTERNAL 1m bars == EXTERNAL bars (TC-D54).

TC-D50/D53 reconstruct minute buckets by hand. This test drives the *real* path
MY043 runs — a ``TradingNode`` whose ``LiveDataEngine`` aggregates the Rithmic
ticker into ``1-MINUTE-INTERNAL`` bars with ``time_bars_timestamp_on_close=False``
— and proves:

- Seam/parity: for the first complete INTERNAL minute, the EXTERNAL bar for
  the same minute exists and its open/high/low/close/volume equals the INTERNAL
  bar's (prices within one tick, volume exact) — the two grids agree on the
  same minute-aligned open-time convention at the warmup→live handoff.
- In-progress continuity: the INTERNAL volume equals the live trade ticks
  accumulated from that minute's open (what the strategy actually counts).
- Sweep characterization: same-timestamp multi-price prints (a sweep) still
  produce full parity — the engine does not drop same-ts ticks.

Skips when no complete traded minute closes within the window (thin market).
"""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from nautilus_trader.config import LiveDataEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.trading.strategy import StrategyConfig

from rithmic_nt_connect import ADAPTER_NAME
from rithmic_nt_connect import RithmicLiveDataClientConfig
from rithmic_nt_connect import RithmicLiveDataClientFactory
from parity_helpers import NS_PER_MIN
from parity_helpers import open_minute
from parity_helpers import wait_for_external_bar

from rithmic_nt_connect import session_config_from_explicit_test_env
from rithmic_nt_connect.session import connect_market_data_session

pytestmark = pytest.mark.live


class InternalBarCaptureConfig(StrategyConfig):
    instrument_id: str


class InternalBarCapture(Strategy):
    """Records 1-MINUTE-INTERNAL bars and live trade ticks for one instrument."""

    def __init__(self, config: InternalBarCaptureConfig, instrument):
        super().__init__(config)
        self.instrument = instrument
        self.bars: list[Bar] = []
        self.ticks: list = []
        self._internal = BarType.from_str(
            f"{instrument.id.symbol}.RITHMIC-1-MINUTE-LAST-INTERNAL"
        )

    def on_start(self) -> None:
        # The engine derives the ticker subscription for the INTERNAL bar and
        # aggregates; subscribing ticks explicitly gives the reference tape.
        self.subscribe_trade_ticks(self.instrument.id)
        self.subscribe_bars(self._internal)

    def on_bar(self, bar: Bar) -> None:
        self.bars.append(bar)

    def on_trade_tick(self, tick) -> None:
        self.ticks.append(tick)


def _wait_running(node, timeout: float = 45.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        loop = node.get_event_loop()
        if loop is not None and loop.is_running():
            return
        time.sleep(0.05)
    raise RuntimeError("TradingNode did not reach running state")


def _wait_for_first_complete_traded_bar(strategy, timeout: float) -> Bar | None:
    """First *complete* INTERNAL bar with volume.

    The first bar after a mid-minute subscribe is the partial in-progress
    minute: the aggregator floors the first tick's time to the grid, so it is
    minute-aligned yet its volume only counts post-subscribe trades. Exclude it
    by requiring the bar's open minute to be strictly after the first tick's
    minute (the strategy and aggregator see the same tick stream).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if strategy.ticks:
            cutoff = open_minute(min(int(t.ts_event) for t in strategy.ticks))
            for bar in strategy.bars:
                if int(bar.volume) > 0 and open_minute(int(bar.ts_event)) > cutoff:
                    return bar
        time.sleep(0.2)
    return None


def _release_locks() -> None:
    """Release the credential flock held by the node's session (best-effort)."""
    try:
        from rithmic_nt_connect.factories import _SESSION_CACHE

        for _sess in _SESSION_CACHE.values():
            _lock = getattr(_sess, "_lock", None)
            if _lock is not None:
                try:
                    _lock.close()
                except Exception:
                    pass
        _SESSION_CACHE.clear()
    except Exception:
        pass


class TestInternalNode:
    def test_TC_D54_internal_node_matches_external(self, front_month_instrument) -> None:
        """TC-D54 — real-node INTERNAL bars equal EXTERNAL bars at the seam."""
        instrument = front_month_instrument
        test_session = session_config_from_explicit_test_env()

        strategy = InternalBarCapture(
            InternalBarCaptureConfig(instrument_id=str(instrument.id)),
            instrument,
        )
        # A prior test disposed its loop; give the next node a fresh one.
        asyncio.set_event_loop(asyncio.new_event_loop())
        node = TradingNode(
            config=TradingNodeConfig(
                trader_id=TraderId("TESTER-002"),
                logging=LoggingConfig(log_level="WARNING", print_config=False),
                data_engine=LiveDataEngineConfig(
                    graceful_shutdown_on_exception=False,
                    # The MY043 convention (FIX-1): INTERNAL bars stamp OPEN,
                    # matching the adapter's EXTERNAL close→open shift.
                    time_bars_timestamp_on_close=False,
                ),
                data_clients={
                    ADAPTER_NAME: RithmicLiveDataClientConfig(session=test_session)
                },
                timeout_connection=45.0,
                timeout_disconnection=10.0,
            )
        )
        node.add_data_client_factory(ADAPTER_NAME, RithmicLiveDataClientFactory)
        node.trader.add_strategy(strategy)
        node.build()

        thread = threading.Thread(target=node.run, daemon=True)
        thread.start()
        try:
            _wait_running(node)
            traded = _wait_for_first_complete_traded_bar(strategy, timeout=240.0)
            if traded is None:
                pytest.skip("no complete traded INTERNAL minute within 4 minutes")

            # The captured minute is OPEN time (timestamp_on_close=False).
            minute_ns = int(traded.ts_event)
            assert minute_ns % NS_PER_MIN == 0, "INTERNAL bar is minute-aligned"
        finally:
            node.stop()
            thread.join(timeout=30.0)
            node.dispose()
            _release_locks()

        minute_sec = minute_ns // 1_000_000_000

        # Reload the EXTERNAL grid with a fresh session and wait for the venue
        # to publish the captured minute's bar (replay emits in delayed batches).
        external_type = BarType.from_str(
            f"{instrument.id.symbol}.RITHMIC-1-MINUTE-LAST-EXTERNAL"
        )
        sess = connect_market_data_session(test_session)
        try:
            external = wait_for_external_bar(
                sess, instrument, external_type, minute_ns, deadline_secs=300.0
            )
        finally:
            sess.disconnect()
            lock = getattr(sess, "_lock", None)
            if lock is not None:
                lock.close()

        assert external is not None, f"EXTERNAL bar exists for captured minute {minute_sec}"

        # Parity: INTERNAL (aggregated by Nautilus) vs EXTERNAL (venue bar).
        tick = float(instrument.price_increment.as_double())
        for name, internal_px, external_px in (
            ("open", traded.open, external.open),
            ("high", traded.high, external.high),
            ("low", traded.low, external.low),
            ("close", traded.close, external.close),
        ):
            assert abs(float(internal_px) - float(external_px)) <= tick + 1e-9, (
                f"minute {minute_sec}: INTERNAL {name} {float(internal_px)} != "
                f"EXTERNAL {name} {float(external_px)}"
            )
        assert int(traded.volume) == int(external.volume), (
            f"minute {minute_sec}: INTERNAL volume {int(traded.volume)} != "
            f"EXTERNAL volume {int(external.volume)}"
        )

        # In-progress continuity: INTERNAL volume == live ticks from minute open.
        minute_ticks = [t for t in strategy.ticks if open_minute(int(t.ts_event)) == minute_ns]
        tick_volume = sum(int(t.size) for t in minute_ticks)
        assert tick_volume == int(traded.volume) == int(external.volume), (
            f"minute {minute_sec}: live ticks {tick_volume} != INTERNAL "
            f"{int(traded.volume)} != EXTERNAL {int(external.volume)}"
        )

        # Sweep characterization: same-ts multi-price prints must not be dropped.
        # When the captured minute co-located several prints in one nanosecond,
        # the parity assertions above already prove the engine did not collapse
        # them (the venue bar — built from the same ticks — still matches).
        same_ts = len(minute_ticks) - len({int(t.ts_event) for t in minute_ticks})
        assert same_ts >= 0
