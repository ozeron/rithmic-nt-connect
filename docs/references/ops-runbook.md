# Ops runbook

1. Close MotiveWave / R|Trader (one Rithmic session per login).
2. Copy `.env.example` → `.env` and set `RITHMIC_USER` / `RITHMIC_PASSWORD`.
3. Build extension: `maturin develop`
4. Unit tests: `cargo test -p rithmic-nt-connect && pytest -q`
5. Live smoke: `python scripts/smoke_lucid_nq.py` (exits `2` if credentials missing — CI-safe).

Phase 1 does **not** submit orders by default. Phase 2 order APIs exist on the
session / exec client when `enable_trading=True` (or `RITHMIC_ENABLE_TRADING=1`).
Prefer `python scripts/verify_order_dry_run.py` before any live place. Never use
`--live-place` until conformance / `app_name` authorization is confirmed.

See also: `docs/references/my046-rithmic-access.md`, `docs/references/plant-probe-2026-08-12.md`.
