from dataclasses import dataclass

@dataclass
class BacktestResult:
    trader_id: str
    machine_id: str
    run_config_id: str | None
    instance_id: str
    run_id: str
    run_started: int | None
    run_finished: int | None
    backtest_start: int | None
    backtest_end: int | None
    elapsed_time: float
    iterations: int
    total_events: int
    total_orders: int
    total_positions: int
    summary: dict[str, str]
    stats_pnls: dict[str, dict[str, float]]
    stats_returns: dict[str, float]

def ensure_plotting(func): ...
