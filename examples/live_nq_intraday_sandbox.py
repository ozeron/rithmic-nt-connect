#!/usr/bin/env python3
"""Live NQ ticks + INTERNAL 1-second bars; paper trades on Nautilus Sandbox.

Rithmic is **data only**. Orders go to ``SandboxLiveExecClientFactory``
(``venue=RITHMIC``), not the Rithmic order plant.

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

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=60.0)
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="global stdout level (default INFO; use WARNING to hide engine chatter)",
    )
    args = parser.parse_args(argv)

    from rithmic_nt_connect import env_truthy
    from rithmic_nt_connect import load_dotenv_files

    load_dotenv_files(ROOT / ".env")
    if env_truthy(os.environ.get("RITHMIC_ENABLE_TRADING")):
        print("refusing to run: unset RITHMIC_ENABLE_TRADING (this example is sandbox-only)")
        return 2

    import asyncio

    from nautilus_trader.adapters.sandbox.config import SandboxExecutionClientConfig
    from nautilus_trader.adapters.sandbox.factory import SandboxLiveExecClientFactory
    from nautilus_trader.config import LiveDataEngineConfig
    from nautilus_trader.config import LiveExecEngineConfig
    from nautilus_trader.config import LiveRiskEngineConfig
    from nautilus_trader.config import LoggingConfig
    from nautilus_trader.config import TradingNodeConfig
    from nautilus_trader.live.node import TradingNode
    from nautilus_trader.model.identifiers import TraderId

    from nq_four_bar import NqFourBarConfig
    from nq_four_bar import NqFourBarStrategy
    from rithmic_nt_connect import ADAPTER_NAME
    from rithmic_nt_connect import RithmicLiveDataClientConfig
    from rithmic_nt_connect import RithmicLiveDataClientFactory
    from rithmic_nt_connect import VENUE

    node = TradingNode(
        config=TradingNodeConfig(
            trader_id=TraderId("PAPER-001"),
            logging=LoggingConfig(
                # Default INFO so SMA/VWAP lookback and BAR lines show.
                # https://nautilustrader.io/docs/latest/concepts/logging/
                log_level=args.log_level.upper(),
                print_config=False,
                log_component_levels={
                    "NqFourBar-001": "INFO",
                    "PAPER-001.NqFourBar-001": "INFO",
                },
            ),
            data_engine=LiveDataEngineConfig(graceful_shutdown_on_exception=True),
            exec_engine=LiveExecEngineConfig(
                graceful_shutdown_on_exception=True,
                reconciliation_startup_delay_secs=0.0,
            ),
            risk_engine=LiveRiskEngineConfig(graceful_shutdown_on_exception=True),
            data_clients={ADAPTER_NAME: RithmicLiveDataClientConfig()},
            exec_clients={
                "SANDBOX": SandboxExecutionClientConfig(
                    venue=VENUE,
                    starting_balances=["100000 USD"],
                    oms_type="NETTING",
                    account_type="MARGIN",
                    trade_execution=True,
                    bar_execution=True,
                ),
            },
            timeout_connection=45.0,
            timeout_reconciliation=5.0,
            timeout_portfolio=5.0,
            timeout_disconnection=5.0,
            timeout_post_stop=5.0,
        )
    )
    node.add_data_client_factory(ADAPTER_NAME, RithmicLiveDataClientFactory)
    node.add_exec_client_factory("SANDBOX", SandboxLiveExecClientFactory)
    strategy = NqFourBarStrategy(NqFourBarConfig(request_lookback=True))
    node.trader.add_strategy(strategy)
    node.build()
    print(f"paper node up  data=Rithmic exec=Sandbox  {args.seconds:.0f}s", flush=True)

    loop = node.get_event_loop()
    if loop is None:
        print("no event loop after TradingNode build")
        node.dispose()
        return 1

    async def _run() -> None:
        runner = asyncio.create_task(node.run_async())
        try:
            await asyncio.sleep(max(args.seconds, 1.0))
        finally:
            if not runner.done():
                await node.stop_async()
                await runner
            else:
                exc = runner.exception()
                if exc is not None:
                    print(f"ERROR: TradingNode.run_async() failed: {exc}", file=sys.stderr)
                    traceback.print_exception(exc, file=sys.stderr)
                    raise exc
                print(
                    "warning: TradingNode.run_async() returned before --seconds elapsed",
                    file=sys.stderr,
                )

    try:
        loop.run_until_complete(_run())
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
