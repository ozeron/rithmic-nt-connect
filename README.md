# rithmic-connect

Unofficial **Rithmic R|Protocol** adapter compatible with NautilusTrader — Rust client core, Python strategy surface.

> This is an independent community project. It is not affiliated with, endorsed by, or supported by Nautech Systems Pty Ltd or the official NautilusTrader project.
>
> It is also not affiliated with, endorsed by, or supported by Rithmic, LLC.

## Status

**Phase 1** targeting **NautilusTrader 1.231.x**: market data + instruments + read-only account/PnL. **No order routing.**

Plan: [`docs/plans/2026-08-12-001-feature-rithmic-nt-adapter-plan.md`](docs/plans/2026-08-12-001-feature-rithmic-nt-adapter-plan.md)

## Quick start

```bash
# Rust extension + editable install
maturin develop

# Unit tests (no live credentials)
cargo test -p rithmic-connect
pytest -q

# Live LucidTrading smoke (close MotiveWave first)
cp .env.example .env   # fill credentials
python scripts/smoke_lucid_nq.py
```

Register on a `TradingNode`:

```python
from nautilus_trader.live.node import TradingNode
from rithmic_connect import (
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

Minimal ticks example: [`examples/live_nq_ticks.py`](examples/live_nq_ticks.py).

## Ops

- **One session per login** — close MotiveWave / R|Trader before connecting.
- LucidTrading defaults: system `LucidTrading`, gateway `wss://rprotocol.rithmic.com:443`.
- See [`docs/references/ops-runbook.md`](docs/references/ops-runbook.md).

## License

Apache-2.0 (see `LICENSE`).
