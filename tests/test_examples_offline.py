"""The examples run on NautilusTrader 2 (no credentials).

* ``backtest_nq_today._backtest_fingerprint`` — the example's own engine setup —
  on synthetic trade ticks shaped like the history plant's, twice: the rule
  trades and replays identically (``--check``).
* ``live_nq_intraday_sandbox.build_paper_node`` — the paper node — with its
  Rithmic data client over the FakeVenue and the simulated sandbox venue.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path
from typing import Any

import pytest
from _fake_venue import (
    HAS_FAKE,
    INSTRUMENT_ID,
    Venues,
    factories,
    session_config,
    until,
)
from rithmic_nt_connect.config import RithmicLiveDataClientConfig
from rithmic_nt_connect.data import payloads_to_trade_ticks
from rithmic_nt_connect.providers import future_from_reference

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "examples"))

from backtest_nq_today import _backtest_fingerprint
from live_nq_intraday_sandbox import build_paper_node, run_for
from nq_four_bar import NqFourBarConfig, NqFourBarStrategy

# 2026-08-19 16:00:00Z.
_START_S = 1787155200
_REF = {
    "trading_symbol": "NQU6",
    "trading_exchange": "CME",
    "underlying": "NQ",
    "product_code": "NQ",
    "currency": "USD",
    "tick_size": 0.25,
    "point_value": 20.0,
    "price_precision": 2,
    "expiration_date": "20260918",
    "is_tradable": True,
}


def _ticks(seconds: int) -> list[Any]:
    """One trade per second: six lower closes, then six higher, repeating."""
    ticks: list[Any] = []
    price = 21000.0
    for i in range(seconds):
        price += -0.25 if (i // 6) % 2 == 0 else 0.25
        ts_event = (_START_S + i) * 1_000_000_000
        ticks.extend(
            payloads_to_trade_ticks(
                [
                    {
                        "trade_price": price,
                        "trade_size": 1,
                        "ssboe": _START_S + i,
                        "usecs": 0,
                    }
                ],
                symbol="NQU6",
                exchange="CME",
                price_precision=2,
                ts_init=ts_event,
            )
        )
    return ticks


def test_four_bar_example_trades_and_replays_identically() -> None:
    instrument = future_from_reference(_REF)
    ticks = _ticks(120)

    first = _backtest_fingerprint(instrument, ticks, [], log_strategy=False)
    again = _backtest_fingerprint(instrument, ticks, [], log_strategy=False)

    assert first["ticks"] == 120
    # Each six-bar run gives one four-close signal; the IOC market order fills
    # on the next trade.
    assert first["fills"] >= 10, first
    sides = {side for _, side, _, _ in first["fill_sig"]}
    assert sides == {"BUY", "SELL"}
    assert first["cash"] is not None
    assert again["fill_sig"] == first["fill_sig"]
    assert again["cash"] == first["cash"]


@pytest.mark.skipif(not HAS_FAKE, reason="extension built without testing")
def test_sandbox_paper_node_trades_on_fake_rithmic_data(tmp_path: Any) -> None:
    """The paper example's node: Rithmic data client (over the FakeVenue) +
    NautilusTrader's simulated sandbox execution. Falling trades build 1s
    INTERNAL bars, the rule buys, and the sandbox fills it."""
    venues = Venues(tmp_path)
    data_factory, _ = factories(venues)
    node = build_paper_node(
        data_factory,
        RithmicLiveDataClientConfig(session=session_config()),
        log_level="WARNING",
        timeout_connection_secs=10,
    )
    strategy = NqFourBarStrategy(
        NqFourBarConfig(instrument_id=str(INSTRUMENT_ID), request_lookback=False)
    )
    node.add_strategy(strategy)

    async def falling_tape() -> None:
        await until(
            lambda: any(
                c.startswith("subscribe NQH6") for c in venues.data.script.calls()
            ),
            "trade subscription",
        )
        price = 21000.0
        while True:
            price -= 0.25
            venues.data.script.emit(
                "ticker",
                {
                    "type": "last_trade",
                    "symbol": "NQH6",
                    "exchange": "CME",
                    "trade_price": price,
                    "trade_size": 1,
                    "aggressor": 2,
                    "ts_event_ns": time.time_ns(),
                },
            )
            await asyncio.sleep(0.2)

    async def drive() -> None:
        tape = asyncio.create_task(falling_tape())
        try:
            await run_for(node, 7.0)
        finally:
            tape.cancel()

    asyncio.run(drive())
    node.dispose()

    assert strategy.bars >= 4, f"1s INTERNAL bars={strategy.bars}"
    assert strategy.fill_events, "sandbox filled no order"
    assert {f.order_side.name for f in strategy.fill_events} >= {"BUY"}
    # The example flattens on stop: the sandbox account ends flat.
    assert strategy.portfolio.net_position(INSTRUMENT_ID) == 0
