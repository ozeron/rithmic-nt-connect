# Ops runbook

1. Close MotiveWave / R|Trader (one Rithmic session per login).
2. Copy `.env.example` → `.env` with LucidTrading credentials (`RITHMIC_USER` / `RITHMIC_PASSWORD` / `RITHMIC_SYSTEM=LucidTrading`).
3. Build extension: `maturin develop`
4. Unit tests: `cargo test -p rithmic-connect && pytest -q`
5. Live smoke: `python scripts/smoke_lucid_nq.py` (exits `2` if credentials missing — CI-safe).

Phase 1 does **not** submit orders. The read-only exec client rejects order actions.

See also: `docs/references/my046-rithmic-access.md`, `docs/references/plant-probe-2026-08-12.md`.
