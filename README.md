# rithmic-nt-connect

**Unofficial community Rithmic R|Protocol adapter for NautilusTrader** — Rust client core, Python strategy surface.

> ⚠️ **Disclaimer:** This is an independent community project. It is **not** affiliated with, endorsed by, or supported by [Nautech Systems Pty Ltd](https://nautilustrader.io) or the official [NautilusTrader](https://nautilustrader.io) project.
>
> It is also **not** affiliated with, endorsed by, or supported by Rithmic, LLC.

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Unofficial](https://img.shields.io/badge/NautilusTrader-unofficial%20community%20adapter-orange.svg)](https://nautilustrader.io)
[![Progress](https://img.shields.io/badge/implemented-79%25-blue.svg)](docs/STATUS.md)

## Status

Out-of-tree adapter for NautilusTrader **1.231.x**: live ticks/quotes, history requests, futures instruments, read-only PnL, and **gated** order-plant submit/cancel/modify.

Trading is **off by default** (`enable_trading=False` / unset `RITHMIC_ENABLE_TRADING`). Live place is gated; use the dry-run harness first. Scope vs done: [`docs/STATUS.md`](docs/STATUS.md) — see the **At-a-glance** scoreboard there (currently **79% implemented**: done + half of partial; regenerated with `python scripts/status_progress.py`).

**Connect mode (required):** set `RITHMIC_CONNECT_MODE` / `SessionConfig.connect_mode` (`ConnectMode.DIRECT` \| `ConnectMode.GATEWAY`) — this process + flock + plants, or dial `rithmic-gateway` over `unix://…` for a shared login. No silent default — see [`docs/references/ops-runbook.md`](docs/references/ops-runbook.md). Remote TLS is not shipped — [`docs/references/gateway-remote.md`](docs/references/gateway-remote.md).

**Lake / shared history (no Nautilus):** install the pure-Python client from `python/` (`uv pip install -e python` or `PYTHONPATH=python` + `protobuf`). Use `GatewayClient` + `load_time_bars_range` (auto-spawn or long-lived parent). market-data-lake hardcodes this path.

**Self-contained wheel:** `scripts/build_wheel.sh` produces a wheel that carries the adapter, the `rithmic_gateway` client, **and** the `rithmic-gateway` binary — consumers `pip install` one artifact and the gateway binary is resolved from `rithmic_gateway/bin/` (no `RITHMIC_GATEWAY_BIN`, no `cargo build`).

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
# Python deps (includes protobuf) + Rust extension
uv sync --extra dev
uv run maturin develop

# Unit tests (no live credentials)
cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect
uv run pytest -q

# Live LucidTrading smoke (close MotiveWave first)
cp .env.example .env   # fill credentials
uv run python scripts/smoke_lucid_nq.py

# Live test-account e2e (uses this repo's .env; refuses production systems)
RITHMIC_TEST_DOTENV=.env uv run pytest tests/e2e -v
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

session = SessionConfig.from_env()  # requires RITHMIC_CONNECT_MODE=direct|gateway
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
    session,
    instrument,
    start,
    end,
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

## Code quality

Local gates (run via `mise use -q && hk run check` — mirrors CI):

| Layer | Tool | Mode |
| --- | --- | --- |
| Python lint | `ruff check` | block |
| Python format | `ruff format --check` | block |
| Python types | `ty check python/rithmic_nt_connect tests` | block |
| Python tests | `pytest -q` | block |
| Rust lint | `cargo clippy --workspace --all-targets -- -D warnings` | block |
| Rust format | `cargo fmt --all --check` | block |
| Rust tests | `cargo test --workspace` | block |
| Orchestrator | [`qlty`](https://qlty.sh) (CI: `qltysh/qlty-action`) | block on changed files |

`qlty` runs the existing linters (**ruff**, **clippy**, hk's **`cargo fmt`** + **`ruff format`**) plus **[`bandit`](https://github.com/PyCQA/bandit)**, **[`trufflehog`](https://github.com/trufflesecurity/trufflehog)** (secrets), **[`shellcheck`](https://www.shellcheck.net/)**, **[`actionlint`](https://github.com/rhysd/actionlint)** + **[`zizmor`](https://github.com/woodruffw/zizmor)** (CI YAML), **[`osv-scanner`](https://github.com/google/osv-scanner)** (dep vulns — real findings appear in baseline), and smells (duplication, complexity). CI runs `qlty check` against the merge base, so pre-existing findings are grandfathered while new ones fail the PR. Full mode (`--all`) is local-only.

Install qlty (`mise.toml` pins it to the GitHub release):

```bash
mise install                # installs both hk and qlty
qlty check                  # staged files (pre-commit)
qlty check --all --level=low  # whole-repo scan with low-severity findings
qlty smells --all           # duplication / complexity inventory
```

## License

Apache-2.0 — see [LICENSE](LICENSE) for details.

---

*This project is not affiliated with, endorsed by, or supported by Nautech Systems Pty Ltd or the NautilusTrader project.*
