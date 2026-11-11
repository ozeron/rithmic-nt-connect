#!/usr/bin/env python3
"""Live NQ ticks + INTERNAL 1-second bars; paper trades on Nautilus Sandbox.

Rithmic is **data only**. Orders go to NautilusTrader's simulated
``SandboxExecutionClientFactory`` (``venue=RITHMIC``), not the Rithmic order
plant.

Rule: 4 consecutive lower closes → buy 1; 4 consecutive higher closes → sell 1
(can stack). Daily SMA20 (EXTERNAL lookback) + 1m INTERNAL VWAP from ticks.
After ``--seconds``, stop
(``close_all_positions``).

Close MotiveWave / R|Trader first (one Rithmic session per login).

Usage::

    maturin develop
    cp .env.example .env   # user / password (system + gateway default LucidTrading)
    python examples/live_nq_intraday_sandbox.py --seconds 90
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
sys.path.insert(0, str(ROOT / "examples"))


def build_paper_node(
    data_factory: Any,
    data_config: Any,
    *,
    log_level: str = "INFO",
    timeout_connection_secs: int = 45,
) -> Any:
    """Rithmic data + NautilusTrader Sandbox execution on ``venue=RITHMIC``."""
    from nautilus_trader.adapters.sandbox import (
        SandboxExecutionClientConfig,
        SandboxExecutionClientFactory,
    )
    from nautilus_trader.common import Environment, LoggerConfig, LogLevel
    from nautilus_trader.config import LiveExecutionEngineConfig
    from nautilus_trader.execution import FixedFeeModel
    from nautilus_trader.live import LiveNode
    from nautilus_trader.model import (
        AccountType,
        Currency,
        Money,
        OmsType,
        TraderId,
        Venue,
    )
    from rithmic_nt_connect import ADAPTER_NAME, VENUE

    usd = Currency.from_str("USD")
    return (
        LiveNode.builder("PAPER", TraderId("PAPER-001"), Environment.SANDBOX)
        .with_logging(
            LoggerConfig(
                # Default INFO so SMA/VWAP lookback and BAR lines show.
                stdout_level=getattr(LogLevel, log_level.upper()),
                print_config=False,
                component_levels={"NqFourBar-001": "INFO"},
            )
        )
        .with_exec_engine_config(
            LiveExecutionEngineConfig(reconciliation_startup_delay_secs=0.0)
        )
        .with_timeout_connection(timeout_connection_secs)
        .with_timeout_reconciliation(5)
        .with_timeout_portfolio(5)
        .with_timeout_disconnection_secs(5)
        .with_delay_post_stop_secs(5)
        .add_data_client(ADAPTER_NAME, data_factory, data_config)
        .add_simulated_exec_client(
            "SANDBOX",
            SandboxExecutionClientFactory(),
            SandboxExecutionClientConfig(
                venue=Venue(VENUE),
                starting_balances=[Money(100_000, usd)],
                # Commission-free paper fills: the rule alone.
                fee_model=FixedFeeModel(Money(0, usd)),
                oms_type=OmsType.NETTING,
                account_type=AccountType.MARGIN,
                trade_execution=True,
                bar_execution=True,
            ),
        )
        .build()
    )


async def run_for(node: Any, seconds: float) -> None:
    """Run ``node`` for ``seconds``, then stop it (the strategy flattens)."""
    import asyncio

    handle = node.handle()
    runner = asyncio.create_task(node.run_async())
    try:
        await asyncio.sleep(max(seconds, 1.0))
    finally:
        if not runner.done():
            handle.stop()
            await runner
        else:
            exc = runner.exception()
            if exc is not None:
                raise exc
            print(
                "warning: LiveNode.run_async() returned before --seconds elapsed",
                file=sys.stderr,
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=60.0)
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="global stdout level (default INFO; use WARNING to hide engine chatter)",
    )
    args = parser.parse_args(argv)

    from rithmic_nt_connect import env_truthy, load_dotenv_files

    load_dotenv_files(ROOT / ".env")
    if env_truthy(os.environ.get("RITHMIC_ENABLE_TRADING")):
        print(
            "refusing to run: unset RITHMIC_ENABLE_TRADING (this example is "
            "sandbox-only)"
        )
        return 2

    import asyncio

    from nq_four_bar import NqFourBarConfig, NqFourBarStrategy
    from rithmic_nt_connect import (
        RithmicLiveDataClientConfig,
        RithmicLiveDataClientFactory,
    )

    node = build_paper_node(
        RithmicLiveDataClientFactory,
        RithmicLiveDataClientConfig(),
        log_level=args.log_level,
    )
    strategy = NqFourBarStrategy(NqFourBarConfig(request_lookback=True))
    node.add_strategy(strategy)
    print(f"paper node up  data=Rithmic exec=Sandbox  {args.seconds:.0f}s", flush=True)

    try:
        asyncio.run(run_for(node, args.seconds))
        print(
            f"PNL  bars={strategy.bars} fills={strategy.fills}",
            flush=True,
        )
        if strategy.bars == 0:
            print(
                "no 1s bars arrived — ticker was silent. Close MotiveWave/R|Trader "
                "(one Rithmic session) and retry.",
                file=sys.stderr,
            )
            return 2
    except Exception as exc:
        print(f"ERROR: node run failed: {exc}", file=sys.stderr)
        traceback.print_exception(exc, file=sys.stderr)
        return 1
    finally:
        try:
            node.dispose()
        except Exception as exc:
            print(f"ERROR: node.dispose() failed: {exc}", file=sys.stderr)
            traceback.print_exception(exc, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
