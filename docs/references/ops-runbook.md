# Ops runbook

1. Close MotiveWave / R|Trader (one Rithmic session per login).
2. Copy `.env.example` → `.env` and set `RITHMIC_USER` / `RITHMIC_PASSWORD`.
3. Install system `protoc` (`protobuf-compiler` / `brew install protobuf`) if missing.
4. Sync + build extension: `uv sync --extra dev && uv run maturin develop`
5. Unit tests: `cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect && uv run pytest -q`
6. Live smoke: `uv run python scripts/smoke_lucid_nq.py` (exits `2` if credentials missing — CI-safe).
7. Shared gateway (two consumers, one parent): `uv run python scripts/smoke_gateway_shared_ticks.py --seconds 25` (NQ + MNQ; exits `2` if no creds).

## Connect mode (required)

Set **`RITHMIC_CONNECT_MODE`** (or `SessionConfig.connect_mode` / `ConnectMode`) to one of:

| Value | Who opens Rithmic | Use when |
| --- | --- | --- |
| `direct` (`ConnectMode.DIRECT`) | This process (flock + PyO3 plants) | Single Nautilus / script process |
| `gateway` (`ConnectMode.GATEWAY`) | Only `rithmic-gateway`; clients dial `unix://…` | Multiple processes share one login |

Unset or any other value → `ConfigError`. There is no silent default.

```bash
# Parent (owns flock + plants)
cargo run -p rithmic-gateway --bin rithmic-gateway
# or: set RITHMIC_GATEWAY_BIN and let clients auto-spawn

# Child / lake (no maturin): pure Python — protobuf is a core dep
#   uv sync
#   # or: pip install -e .
export RITHMIC_CONNECT_MODE=gateway
export RITHMIC_GATEWAY_LISTEN=unix://$XDG_RUNTIME_DIR/rithmic-gateway-<hash>.sock
# Optional: RITHMIC_GATEWAY_AUTO_SPAWN=1, RITHMIC_GATEWAY_BIN=…
# Trading / cancel_all are parent-gated (independent toggles):
#   RITHMIC_ENABLE_TRADING=1          # place / modify / cancel / order updates
#   RITHMIC_GATEWAY_CANCEL_ALL=1      # plant-wide cancel_all panic button (does not require trading)
# Optional remote-ready token (non-empty client auth_token must match):
#   RITHMIC_GATEWAY_AUTH_TOKEN=…
# Idle-exit after last client (parent lifetime):
#   RITHMIC_GATEWAY_IDLE_EXIT_SEC unset/-1 = stay until SIGTERM (manual/standalone)
#   0 = exit immediately when peers hit 0; N = grace seconds
#   Auto-spawn injects IDLE_EXIT_SEC=5 unless already set
# Clients refuse a dialable unix path unless the credential flock is held
# (impostor protection); auto-spawn also requires flock+listen before returning.
```

If a second direct/parent process hits the same `user|system|url|env` flock, it fails locally before a second Rithmic login. Default sock/lock paths live under `XDG_RUNTIME_DIR` or a private `$TMPDIR/rgw-$UID/` directory (short names so macOS `sockaddr_un` fits); oversized paths clamp to `/tmp/rgw-$UID/<hash8>.sock` (private dir, not a bare sticky `/tmp` file).

Remote / Docker / TLS: [`gateway-remote.md`](gateway-remote.md) (v1 local unix; v1.5 tunnel recipe; v2 TLS not implemented).

Phase 1 does **not** submit orders by default. Phase 2 order APIs exist on the
session / exec client when `enable_trading=True` (or `RITHMIC_ENABLE_TRADING=1`).
Prefer `python scripts/verify_order_dry_run.py` before any live place. Never use
`--live-place` until conformance / `app_name` authorization is confirmed.
`RITHMIC_ACCOUNT_ID` / `FCM_ID` / `IB_ID` are optional: the order plant discovers
the account list after login (multi-account users set `RITHMIC_ACCOUNT_ID` as a selector).

Paper-trade strategies with live Rithmic MD and Nautilus sandbox execution
(no Rithmic place): `python examples/live_nq_intraday_sandbox.py --seconds 90`.
Do not register both sandbox and Rithmic exec for venue `RITHMIC`.

Current advertised scope vs done: [`../STATUS.md`](../STATUS.md).
Phases / conventions: [`nautilus-adapter-phases.md`](nautilus-adapter-phases.md), [`nautilus-adapter-conventions.md`](nautilus-adapter-conventions.md).

See also: `docs/references/my046-rithmic-access.md`, `docs/references/plant-probe-2026-08-12.md`.
