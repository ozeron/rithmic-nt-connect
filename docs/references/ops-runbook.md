# Ops runbook

1. Close MotiveWave / R|Trader whenever **any** process will hold the Rithmic login (direct plants **or** `rithmic-gateway` parent — cold-start auto-spawn counts). Shared gateway only multiplexes **gateway clients**, not MotiveWave.
2. Copy `.env.example` → `.env` and set `RITHMIC_USER` / `RITHMIC_PASSWORD`.
3. Install system `protoc` (`protobuf-compiler` / `brew install protobuf`) if missing.
4. Sync + build extension: `uv sync --extra dev && uv run maturin develop`
5. Unit tests: `cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect && uv run pytest -q`
6. Live smoke: `uv run python scripts/smoke_lucid_nq.py` (exits `2` if credentials missing — CI-safe).
7. Shared gateway (two consumers, one parent): `uv run python scripts/smoke_gateway_shared_ticks.py --seconds 25` (NQ + MNQ; exits `2` if no creds).

## Test-account integration tests

Live pytest suites must not load the repository-root `.env`. Supply an explicit,
local test-account file through `RITHMIC_TEST_DOTENV`; the adapter rejects missing
sources and production/LucidTrading systems before connecting.

```bash
RITHMIC_TEST_DOTENV=/secure/local/rithmic-test.env \
uv run pytest tests/e2e/test_data_client_live.py -v

RITHMIC_TEST_DOTENV=/secure/local/rithmic-test.env \
RITHMIC_ENABLE_TRADING=1 \
uv run pytest tests/e2e/test_exec_client_live.py -v
```

The test file must contain the test credentials and `RITHMIC_SYSTEM_NAME` naming
a test/demo system, plus `RITHMIC_CONNECT_MODE` (`direct` or `gateway`).
`RITHMIC_GATEWAY` is required only for `gateway` mode. Keep the file outside
the repository and never print or commit it. Run only when no other
process owns the same Rithmic login; a second direct/gateway session is refused
by the credential flock.

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
look like "no orders", a trading consumer must run with `open_check_open_only=True`
(the 1.231.x knob; `death_policy=trust_stop` was removed in this line) so a cached
open order missing from an empty recon is logged as advisory instead of being
**canceled**. With `open_check_open_only=False` the engine would reconcile an
empty result as "no working orders" and cancel tracked open orders. The adapter
does not enforce this; the node operator is responsible for it (the exec e2e
fixture pins it explicitly).

TODO (live proof): validate against a non-empty, permissioned Lucid account that
the `show_orders` drain actually returns the venue's working orders (not zero).
A provably-complete retrieval path (e.g. per-basket `show_order_history_detail`
with exhaustive enumeration) would remove the best-effort caveat.

## Engine in-flight checker fabricates a terminal UNKNOWN reject (operator knob)

Nautilus 1.231.x's `ExecutionEngine._check_inflight_orders` re-queries an order
that is still `SUBMITTED`/`PENDING_UPDATE`/`PENDING_CANCEL` after
`inflight_check_threshold_ms` (default 5s), up to `inflight_check_retries`
(default 5), then `_resolve_inflight_order` synthesizes
`OrderRejected(reason="UNKNOWN")` (SUBMITTED) or `OrderCanceled` (PENDING_*).

This happens **regardless of what the adapter does**: the engine increments
`_recon_check_retries` per *issued* `QueryOrder` (the query runs in a task, so
an adapter exception is swallowed), so the adapter cannot stop the synthesis by
raising or returning `None`. The adapter's only lever is to resolve the order
out of `SUBMITTED` first — it does so with a bounded per-`user_tag`
`load_orders` drain in `generate_order_status_report` — and to fail closed
(`VenueQueryUnavailable`) whenever the drain cannot resolve the order (the
query itself is not gated on plant state: a latched plant still attempts the
drain, which is the recovery path).

**Operator requirement for live trading:** the terminal UNKNOWN synthesis is
exactly the fabricated state the adapter forbids, and the only reliable way to
prevent it when the venue genuinely does not know the order is to disable the
engine checker — set `inflight_check_interval_ms=0` on the
`LiveExecEngineConfig` (via `TradingNodeConfig.exec_engine`; 1.231.x does not
define this field on the base `ExecEngineConfig` — verified against the
installed package). Interval `0` disables the check; verified `> 0` gate in
`live/execution_engine.py`. Keep `open_check_open_only=True` for the
reconciliation path (see above). If the check is left enabled, the operator
accepts that a genuinely-unknown in-flight order will be resolved as a terminal
UNKNOWN reject by the engine after ~25s (5 retries × 5s threshold) — a strategy
may then resubmit and duplicate a live order.

## Reconnect re-arm requires orders + a fresh PnL/position observation

When the order plant is latched (a prior operation's venue outcome is unknown),
a full reconnect re-arms it only after **both** complete (2026-08-19 exec
hardening, Oracle 2nd + 3rd passes, Macroscope review):

1. A bounded `load_orders` working-orders drain succeeds (one attempt per
   barrier — a failure is surfaced as unavailable and the next connect is the
   retry boundary), **and its rows are applied**: reconciliation status reports
   are published (publish-before-bind — a failed publication aborts the
   barrier, so the engine sees the venue state before trading resumes) and
   tracked in-flight orders then get their venue id bound, so an order the
   venue accepted while disconnected is not left unresolved/in-flight when
   trading resumes. Stale rows are skipped when the live stream has already
   advanced the tracked order past the captured snapshot.
2. A fresh account/position PnL snapshot is observed from the stream when the
   PnL stream is connected this connect (bounded `asyncio.Event` wait, default
   `_REARM_PNL_SNAPSHOT_TIMEOUT_S = 5.0`). Positions ride the PnL stream, so
   re-arming without re-observing it would be blind. With `soft_fail_pnl`
   there is no stream to observe and the gate is skipped — the plant then
   re-arms with **blind position context** (see the operational consequences
   below). Freshness: the drain row's venue timestamp gates the stale-row
   comparison — a row the venue sent without a timestamp (synthetic 0) is
   never treated as stale, so its valid snapshot still publishes and binds
   (Macroscope round 4, 2026-08-19).

The barrier runs on **every** trading (re)connect — not only when a prior
operation latched the plant — because the disconnect window can hide
venue-side accepts/fills/terminal outcomes and position changes even when
nothing was latched. A failed barrier leaves the plant `LATCHED` (recon
pending — submit/modify blocked) until a later successful connect. The order
poll loop keeps running during the barrier, so notifications are never
dropped. The barrier clears the latch **only if the plant is still
`CONNECTING` and the poll task is alive** when the drain finishes — so a
*newer* anomaly during the drain (an overfill latch, a handler break, a resync
failure, or a mid-drain resync, all of which leave `CONNECTING`) survives it.
A reconnect re-arm can never re-enable trading over a dead/broken order
stream.

The plant lifecycle is an explicit state machine (`OrderPlantPolicy`; the
transition table in `tests/test_order_plant.py` is the executable spec):
`DISCONNECTED` (down / never armed), `CONNECTING` (barrier in progress),
`LIVE` (armed), `RESYNCING` (mid-session transport resync — cancels stay
available), `LATCHED` (blocked pending a recon cycle). Execution code never
assigns the state directly — every transition goes through the policy, and
`rearm` is the **only** arming transition. Consequences operators should know:

- A mid-session transport resync never clears a latch: `LATCHED` stays
  `LATCHED` through `resync_start`/`resync_complete`/`resync_failed`. The
  recovery out of `LATCHED` is a successful re-arm barrier (a teardown also
  moves the plant out of it, but commands stay blocked by the down state).
- A resync DURING the reconnect barrier itself latches the plant (`CONNECTING`
  → `LATCHED`): a channel error while the drain is in flight means the drain
  rows may predate the drop, and the plant must never re-arm mid-barrier (the
  only arming transition is `rearm`). The barrier then fails and the next
  connect retries.
- A plant that is `DISCONNECTED` (e.g. after a failed resync) cannot re-arm
  via a later resync — `resync_start` from `DISCONNECTED` stays down. Only a
  full reconnect (drain + PnL gate) arms the plant. (This closes a hole the
  old code had: a resync failure followed by a channel-error resync could
  return to `LIVE` without re-observing venue state.)
- A persistent run of non-channel poll errors on the order stream (bounded
  transient streak, default 5) latches the plant: a stream that delivers
  garbage must not leave trading armed and silent. PnL keeps transient
  semantics (`soft_fail_pnl` is the operator's escape). The streak is
  **loop-local** (owned by the poll loop, per stream lifetime): a reconnect
  or a successful resubscribe starts a fresh count, so transients from a
  previous stream can never latch a healthy loop — the 4-before-drop + 1-
  after-recovery class is structurally impossible, not a convention.

**Operational consequences:**

- With `soft_fail_pnl=True`, the PnL gate is **skipped** whenever the PnL
  subscription soft-fails (`_pnl_connected` is false, which is the default
  mode): the plant re-arms after the order drain with **blind position
  context** — the barrier is order-drain-only in that case. This is a
  documented escape hatch (a soft-failed PnL stream would otherwise block
  trading with no recovery path), **not** a fail-closed guarantee. If you
  cannot accept trading on stale positions, restore PnL (or disable trading)
  before the plant re-arms; with the PnL stream connected, the gate does
  apply and the plant cannot re-arm until it delivers — deliberate
  fail-closed.
- The 5s PnL-snapshot timeout assumes Rithmic pushes account PnL on a short
  interval; validate the real interval on the P5 canary and raise
  `_REARM_PNL_SNAPSHOT_TIMEOUT_S` if needed.

See also: `docs/references/my046-rithmic-access.md`, `docs/references/plant-probe-2026-08-12.md`.
