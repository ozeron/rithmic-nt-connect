# Ops runbook

1. Close MotiveWave / R|Trader (one Rithmic session per login).
2. Copy `.env.example` → `.env` and set `RITHMIC_USER` / `RITHMIC_PASSWORD`.
3. Build extension: `maturin develop`
4. Unit tests: `cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect && pytest -q`
5. Live smoke: `python scripts/smoke_lucid_nq.py` (exits `2` if credentials missing — CI-safe).

## Session modes (shared login)

**Direct (default)** — this process takes the credential flock and opens plants via PyO3 (`RITHMIC_SESSION_MODE=direct` or unset).

**Gateway** — only `rithmic-gateway` opens Rithmic; clients dial a unix listen URL:

```bash
# Parent (owns flock + plants)
cargo run -p rithmic-gateway --bin rithmic-gateway
# or: set RITHMIC_GATEWAY_BIN and let clients auto-spawn

# Child / lake (no maturin): pure Python
#   pip install '.[gateway]'   # protobuf
export RITHMIC_SESSION_MODE=gateway
export RITHMIC_GATEWAY_LISTEN=unix://$XDG_RUNTIME_DIR/rithmic-gateway-<hash>.sock
# Optional: RITHMIC_GATEWAY_AUTO_SPAWN=1, RITHMIC_GATEWAY_BIN=…
# Trading / cancel_all are parent-gated:
#   RITHMIC_ENABLE_TRADING=1  RITHMIC_GATEWAY_CANCEL_ALL=1
```

If a second direct/parent process hits the same `user|system|url` flock, it fails locally before a second Rithmic login.

Remote / Docker / TLS: [`gateway-remote.md`](gateway-remote.md) (v1 local unix; v1.5 tunnel recipe; v2 TLS not implemented).

Phase 1 does **not** submit orders by default. Phase 2 order APIs exist on the
session / exec client when `enable_trading=True` (or `RITHMIC_ENABLE_TRADING=1`).
Prefer `python scripts/verify_order_dry_run.py` before any live place. Never use
`--live-place` until conformance / `app_name` authorization is confirmed.

Paper-trade strategies with live Rithmic MD and Nautilus sandbox execution
(no Rithmic place): `python examples/live_nq_intraday_sandbox.py --seconds 90`.
Do not register both sandbox and Rithmic exec for venue `RITHMIC`.

Current advertised scope vs done: [`../STATUS.md`](../STATUS.md).
Phases / conventions: [`nautilus-adapter-phases.md`](nautilus-adapter-phases.md), [`nautilus-adapter-conventions.md`](nautilus-adapter-conventions.md).

See also: `docs/references/my046-rithmic-access.md`, `docs/references/plant-probe-2026-08-12.md`.
