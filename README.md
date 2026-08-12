# rithmic-connect

Unofficial **Rithmic R|Protocol** adapter compatible with NautilusTrader — Rust client core, Python strategy surface.

> This is an independent community project. It is not affiliated with, endorsed by, or supported by Nautech Systems Pty Ltd or the official NautilusTrader project.
>
> It is also not affiliated with, endorsed by, or supported by Rithmic, LLC.

## Status

**Phase 1 in progress** (scaffold + Rust session facade + PyO3 bridge). Requirements live in [`docs/plans/`](docs/plans/). Reference notes in [`docs/references/`](docs/references/).

Target runtime for Phase 1: **NautilusTrader 1.231.x** (Python `TradingNode`) with a **Rust** Rithmic client exposed via PyO3. Pin via optional extra: `pip install 'rithmic-connect[nautilus]'` → `nautilus_trader==1.231.0`.

**Ops:** one Rithmic login session at a time — close MotiveWave / R|Trader before connecting.

## Why a separate repo

Upstream RFC [#3768](https://github.com/nautechsystems/nautilus_trader/issues/3768) was closed: gated protos, conformance/`app_name`, and Rithmic’s intermediary access model make it a poor fit for **official** inclusion. Nautilus still documents an **External / Community** path for out-of-tree adapters (`ADAPTERS.md`).

## Phases (product)

| Phase | Scope |
| --- | --- |
| **1** | Instruments + full MD plant (live, history, depth when available) for any configured symbol/exchange; best-effort account/positions via PnL plant. **No order routing.** Acceptance: NQ on LucidTrading. |
| **2** | Order submit/cancel/fills + harden account reconcile (may need conformance-issued `app_name`). |

## Layout (intended)

```text
rithmic-connect/
├── crates/                 # Rust Rithmic client + Nautilus model mapping + PyO3
├── python/rithmic_connect/ # LiveDataClient / factories / config
├── docs/plans/             # Requirements + (later) implementation plans
├── docs/references/        # Upstream decisions, plant probes, quirk notes
└── scripts/                # Smoke / acceptance harnesses
```

## Local references used while scoping

- MY046 MotiveWave / LucidTrading harness (market-data verified): `../algotrading/quant-guild-work/projects/MY046_motivewave_simple_ls_demo`
- Wire client candidates: [`pbeets/rithmic-rs`](https://github.com/pbeets/rithmic-rs), MY046 vendored Node client, `async_rithmic`

## License

Apache-2.0 (see `LICENSE`). Compatible with NautilusTrader community listing criteria (LGPL-compatible OSS).
