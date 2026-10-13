## Why

`rust-unified-session` made the plant surface one Rust trait, but everything that decides *what an order or fill means* still lives in Python next to Nautilus: the order-plant lifecycle state machine (`_order_plant.py`), the working-order drain and recovery pipeline and tracked-vs-untracked routing (`execution.py`, `notification_intake.py`, `_orders.py`), fill dedup, commission (`commission.py`), venue exposure (`exposure.py`), order-history recon (`recon.py`) and bar hygiene (`_convert.py`, `historical.py`). About 4k lines of it have no Nautilus dependency in their *logic*; only their inputs and outputs are Nautilus types.

That has two costs. The same venue-semantics rules (post-send is unknown, never retry a state change, dedup by trade id, never read `None` as zero) must be re-proved in Python for every client that wants them, and the lake or any non-Nautilus consumer cannot reuse them. And the logic is tested only through Nautilus fakes, never against the fake plant that already drives the gateway scenarios.

## What Changes

- Port the Nautilus-independent logic to a Rust crate (working name `rithmic-exec`) that depends only on the `Session` trait and the DTOs:
  - order-plant lifecycle state machine, including the real `LATCHED` state and the re-arm barrier;
  - drain/recovery pipeline: one owned row iterator, strict usable-row validation, per-tag recovery, drain-gated re-arm;
  - fill dedup by venue trade id and the stable-id fallback;
  - commission lookup (product code rate, account default fallback) and exposure (`net`/`open_orders` with `None` = unknown);
  - bar hygiene and history conversion boundary.
- Expose the result through the existing PyO3 class as plain dict/DTO events. The Python execution and data clients keep only Nautilus translation: build `OrderStatusReport`/`FillReport`, publish, and map commands to calls.
- Run the same state-machine and drain scenarios against `FakeSession` in Rust, so a venue behavior is specified once.
- No change to the wire protocol, the `Session` trait, or the supported modes.

Out of scope: any change to what the adapter advertises, live order placement, and the Nautilus-side translation layer.

## Capabilities

### New Capabilities
- `exec-core`: the Nautilus-independent execution semantics (lifecycle states and transitions, drain/recovery, dedup, commission, exposure, bar hygiene) as a Rust library with a stable event/DTO boundary.

### Modified Capabilities
- `adapter-wire-facade`: the PyO3 session additionally yields normalized exec events; Python no longer interprets raw plant rows.

## Impact

- **Rust**: new crate over `rithmic-plants`; PyO3 additions in `crates/rithmic-nt-connect/src/python/mod.rs`.
- **Python**: `_order_plant.py`, `_orders.py`, `notification_intake.py`, `commission.py`, `exposure.py`, `recon.py`, and the row-interpreting parts of `execution.py`/`data.py` shrink to translation.
- **Tests to port (these define the behavior; each must pass in Rust before its Python twin is deleted)**: `test_order_plant.py` (transition table), `test_working_orders_drain.py`, `test_notification_intake.py`, `test_execution_outcomes.py`, `test_exec_engine_repro.py`, `test_exec_recon.py`, `test_commission_registry.py`, `test_exposure.py`, `test_convert_ticks.py`, `test_depth_convert.py`, `test_history_convert.py`, `test_request_bars_live_md_retry.py`. The mutation-corpus property test ("bindable implies real closed-set terms") and the venue-shaped fixtures rule from AGENTS.md carry over: fixtures keep real payload shapes (contract symbol vs product code, omitted vs `None`).
- **Risk**: highest-churn code in the repo (2026-08-19 exec-hardening rounds). Port module by module behind the existing Python tests, never as one cut-over, and keep `test_exec_transport_e2e.py` green throughout.
- **Prerequisite**: `rust-unified-session` complete, including its open live-verification task on a market-data account.
