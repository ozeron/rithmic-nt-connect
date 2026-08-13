# rithmic-nt-connect

**Unofficial community Rithmic R|Protocol adapter for NautilusTrader** — Rust client core, Python strategy surface.

> ⚠️ **Disclaimer:** This is an independent community project. It is **not** affiliated with, endorsed by, or supported by [Nautech Systems Pty Ltd](https://nautilustrader.io) or the official [NautilusTrader](https://nautilustrader.io) project.
>
> It is also **not** affiliated with, endorsed by, or supported by Rithmic, LLC.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Unofficial](https://img.shields.io/badge/NautilusTrader-unofficial%20community%20adapter-orange.svg)](https://nautilustrader.io)

## Status

Out-of-tree adapter for NautilusTrader **1.231.x**: live ticks/quotes, history requests, futures instruments, read-only PnL, and **gated** order-plant submit/cancel/modify.

Trading is **off by default** (`enable_trading=False` / unset `RITHMIC_ENABLE_TRADING`). Live place is gated; use the dry-run harness first. Scope vs done: [`docs/STATUS.md`](docs/STATUS.md).

Paper-trade a simple NQ strategy with **live Rithmic data** and Nautilus **Sandbox** execution (no Rithmic orders):

```bash
python examples/live_nq_intraday_sandbox.py --seconds 90
python examples/backtest_nq_today.py --rth
python examples/backtest_nq_today.py --rth --until 16:15:00   # same window → same tape
python examples/backtest_nq_today.py --rth --check            # engine twice on that tape
```

```bash
python scripts/verify_order_dry_run.py --seconds 5
# Do NOT pass --live-place until conformance / app_name is confirmed.
```

## Quick start

```bash
# Rust extension + editable install
maturin develop

# Unit tests (no live credentials)
cargo test -p rithmic-nt-connect
pytest -q

# Live LucidTrading smoke (close MotiveWave first)
cp .env.example .env   # fill credentials
python scripts/smoke_lucid_nq.py
```

Register on a `TradingNode`:

```python
from nautilus_trader.live.node import TradingNode
from rithmic_nt_connect import (
    RithmicDataClientConfig,
    RithmicExecClientConfig,
    RithmicLiveDataClientFactory,
    RithmicLiveExecClientFactory,
    SessionConfig,
    ADAPTER_NAME,
)

session = SessionConfig.from_env()
# node = TradingNode(config=...)
# node.add_data_client_factory(ADAPTER_NAME, RithmicLiveDataClientFactory)
# node.add_exec_client_factory(f"{ADAPTER_NAME}-EXEC", RithmicLiveExecClientFactory)
```

Wire smoke (no strategy): `python scripts/smoke_lucid_nq.py`.

Same 4-bar rule (SMA20 on 1-DAY EXTERNAL / VWAP on 1-MINUTE INTERNAL + 1s INTERNAL)
in [`examples/nq_four_bar.py`](examples/nq_four_bar.py), run live or backtest.

History **requests** (`load_time_bars` / `request_bars`) hydrate EXTERNAL lookback. Live EXTERNAL
1m / 15m / 1h / 1d bars subscribe on the **history plant** (`subscribe_time_bars`). 1-second
bars stay INTERNAL from ticks.

History for backtest / research (IB / Databento-style helper; windowing lives in Rust):

```python
from nautilus_trader.model.data import BarType
from rithmic_nt_connect import (
    connect_market_data_session,
    load_front_month_instrument,
    load_time_bars,
    load_trade_ticks,
)

session = connect_market_data_session()
instrument = load_front_month_instrument(session, "NQ", "CME")
ticks = load_trade_ticks(session, instrument, start, end)
daily = load_time_bars(
    session, instrument, start, end,
    BarType.from_str(f"{instrument.id}-1-DAY-LAST-EXTERNAL"),
)
```

Same pattern as [async-rithmic historical time bars](https://async-rithmic.readthedocs.io/en/latest/historical_data.html#fetch-historical-time-bars):
`python examples/load_nq_time_bars.py` (1-DAY / 1-HOUR / 15-MINUTE).

The live data client `_request_trade_ticks` / `_request_bars` uses the same session loaders.

Front-month + live↔history verify (writes JSON for a frontend/API):

```bash
python scripts/verify_live_vs_history.py --root NQ --seconds 12 --out artifacts/verify.json
```

```python
from rithmic_nt_connect import resolve_front_month, run_front_month_verify
# front = resolve_front_month(session, "NQ", "CME")
# report = run_front_month_verify(session, root="NQ", exchange="CME")
# report.to_dict()  # small JSON-friendly payload
```

## Ops

- **One session per login** — close MotiveWave / R|Trader before connecting.
- LucidTrading defaults: system `LucidTrading`, gateway `wss://rprotocol.rithmic.com:443`.
- Discover systems (no login): `python scripts/list_systems.py`
- See [`docs/references/ops-runbook.md`](docs/references/ops-runbook.md).

## License

Apache-2.0 — see [LICENSE](LICENSE) for details.

---

*This project is not affiliated with, endorsed by, or supported by Nautech Systems Pty Ltd or the NautilusTrader project.*
