# Ops runbook

1. Close MotiveWave / R|Trader whenever **any** process will hold the Rithmic login (direct plants **or** `rithmic-gateway` parent — cold-start auto-spawn counts). Shared gateway only multiplexes **gateway clients**, not MotiveWave.
2. Copy `.env.example` → `.env` and set `RITHMIC_USER` / `RITHMIC_PASSWORD`.
3. Install system `protoc` (`protobuf-compiler` / `brew install protobuf`) if missing.
4. Sync + build extension: `uv sync --extra dev && uv run maturin develop`
5. Unit tests: `cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect && uv run pytest -q`
6. Live smoke: `uv run python scripts/smoke_lucid_nq.py` (exits `2` if credentials missing — CI-safe).
7. Shared gateway (two consumers, one parent): `uv run python scripts/smoke_gateway_shared_ticks.py --seconds 25` (NQ + MNQ; exits `2` if no creds).

## Building a self-contained wheel

The wheel carries the adapter, the `rithmic_gateway` pure-Python client, **and** the
`rithmic-gateway` native binary in one artifact — consumers `pip install` it and the
binary is auto-resolved from `rithmic_gateway/bin/`, no `RITHMIC_GATEWAY_BIN`, no
`cargo build`, no `target/` on their disk.

```bash
scripts/build_wheel.sh            # cargo build -p rithmic-gateway --release, bundle, maturin build
scripts/build_wheel.sh --install  # also pip install the built wheel into the current env
```

`maturin develop` (step 4 above) leaves the binary out; `resolve_gateway_bin` then
falls back to `PATH` / `target/{release,debug}` for local dev.

## Connect mode (required for Nautilus adapter)

Set **`RITHMIC_CONNECT_MODE`** (or `SessionConfig.connect_mode` / `ConnectMode`) to one of:

| Value | Who opens Rithmic | Use when |
| --- | --- | --- |
| `direct` (`ConnectMode.DIRECT`) | This process (flock + PyO3 plants) | Single Nautilus / script process |
| `gateway` (`ConnectMode.GATEWAY`) | Only `rithmic-gateway`; clients dial `unix://…` | Multiple processes share one login |

Unset or any other value → `ConfigError` in the **adapter**. There is no silent default.
**market-data-lake** always uses the gateway client (ignores this key).

```bash
# Preferred for live + lake: long-lived parent (owns flock + plants)
# Leave RITHMIC_GATEWAY_IDLE_EXIT_SEC unset/-1 so the parent stays up.
cargo build -p rithmic-gateway --release
export RITHMIC_GATEWAY_BIN=$PWD/target/release/rithmic-gateway
cargo run -p rithmic-gateway --bin rithmic-gateway
# or: set RITHMIC_GATEWAY_AUTO_SPAWN=1 and let the first client auto-spawn
#     (auto-spawn injects IDLE_EXIT_SEC=5 — fine for lake-only; prefer manual parent with live)

# Child / lake (no maturin / no Nautilus):
#   cd python && uv pip install -e .
#   # or: PYTHONPATH=<repo>/python  +  pip install 'protobuf>=5'
# Env aliases (WSS URL): RITHMIC_GATEWAY or RITHMIC_URL
# Listen socket:        RITHMIC_GATEWAY_LISTEN=unix://…  (never use as WSS URL)
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
#
# Wide history: GatewayClient.load_time_bars_range chunks calendar windows
# (~4h for 1m bars) so each unix RPC stays within timeout/frame limits.
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

## Order-history reconciliation is best-effort (not provably complete)

Rithmic order history has no completion signal (order-notification rows stream
on the subscription channel with no end marker; the request returns only an
ack) and replays silently cap at 10,000 records with no truncation indication.
A silence-window drain therefore can never prove "venue has N and I got all N".
**Observed 2026-08-16:** both the return value and a 10s channel drain yield
zero rows despite the venue reporting orders (`user_msg` count). The drain is
live-venue-unproven as of this writing — see the TODO below.

`load_orders` now performs a best-effort drain: it calls `show_orders` and then
collects current working-order notifications off the order-plant stream until a
bounded silence window elapses (hard-capped at `ORDER_DRAIN_MAX_MS`). The result
is **advisory, not an authoritative venue snapshot**: empty means "no working
orders seen", which is *not* proof the venue has none (a lossy or premature
drain looks identical). The exec generators (`generate_order_status_reports` /
`generate_fill_reports`) therefore return whatever the drain yields — including
`[]` — when trading is enabled, and read-only status recon stays cache-backed
(honest: not a venue snapshot).

**Consumer requirement (important):** because an empty best-effort drain can
look like "no orders", a trading consumer must run with
`death_policy=trust_stop` (or otherwise not cancel on empty recon). Nautilus'
default `death_policy=TRACKED` would reconcile an empty result as "no working
orders" and **cancel tracked open orders**. The adapter does not enforce this;
the node operator is responsible for it.

TODO (live proof): validate against a non-empty, permissioned Lucid account that
the `show_orders` drain actually returns the venue's working orders (not zero).
A provably-complete retrieval path (e.g. per-basket `show_order_history_detail`
with exhaustive enumeration) would remove the best-effort caveat.

See also: `docs/references/my046-rithmic-access.md`, `docs/references/plant-probe-2026-08-12.md`.
