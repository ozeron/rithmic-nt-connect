# Ops runbook

1. Close MotiveWave / R|Trader whenever **any** process will hold the Rithmic login (direct plants **or** a `rithmic-gateway` parent). Shared gateway only multiplexes **gateway clients**, not MotiveWave.
2. Copy `.env.example` → `.env` and set `RITHMIC_USER` / `RITHMIC_PASSWORD`. Only direct mode and the gateway **parent** need the password; gateway-mode clients never read, hold, or send it.
3. Install system `protoc` (`protobuf-compiler` / `brew install protobuf`) if missing. The **Rust** gateway build invokes `protoc` from PATH (prost-build). The **Python** `session_pb2.py` gencode is pinned instead: it is regenerated from `proto/rithmic_gateway/v1/session.proto` with the uv dev dependency `grpcio-tools==1.71.0` (bundles protoc 29.0) via `uv run python scripts/gen_gateway_proto.py`, which restores the repo's `5.29.6` gencode marker (protoc 29.0 and 29.6 emit byte-identical gencode except the version header). The script fails loudly if the gencode shape drifts.
4. Sync + build extension: `uv sync --extra dev && uv run maturin develop`
5. Unit tests: `cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-exec -p rithmic-nt-connect && uv run pytest -q`
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

For Test plants whose front-month response is empty, select an exact listed
contract with `RITHMIC_E2E_CONTRACT=NQZ6`; `RITHMIC_E2E_EXCHANGE` defaults to
`CME`. The data and execution fixtures build it through the production
reference-data loader. The adapter's instrument ID is `NQZ6-CME.RITHMIC`.
Set `RITHMIC_HISTORY_READY_PROBE=0` when history is unavailable so the
connect-time history readiness barrier does not block live ticker/book tests:

```bash
RITHMIC_TEST_DOTENV=/secure/local/rithmic-test.env \
RITHMIC_E2E_CONTRACT=NQZ6 RITHMIC_E2E_EXCHANGE=CME \
RITHMIC_HISTORY_READY_PROBE=0 \
uv run pytest tests/e2e/test_data_client_live.py \
  tests/e2e/test_data_internal_bars_live.py tests/e2e/test_reconnect_live.py -v -rs
```

History-dependent tests share one two-minute 1m replay probe per pytest session
(five-second replay deadline, up to 45 seconds to connect and load the
instrument). An unavailable history plant skips those tests with
`history plant unavailable on this account: <reason>`. Live quotes, trades,
books, internal-bar raw material, and ticker/book resync still run. Disabling
the production readiness probe does not disable this test availability probe.

The test file must contain the test credentials and `RITHMIC_SYSTEM_NAME` naming
a test/demo system, plus `RITHMIC_CONNECT_MODE` (`direct` or `gateway`).
`RITHMIC_GATEWAY` is required only for `gateway` mode. Keep the file outside
the repository and never print or commit it. Run only when no other
process owns the same Rithmic login; a second direct/gateway session is refused
by the credential flock. Direct mode is a process-wide singleton per credential
fingerprint: the data factory, exec factory, and `connect_market_data_session`
all share one login, and the credential flock is released when the **last**
holder disconnects (so a stopped node no longer blocks a separate process).

### LucidTrading override (explicit operator act)

Use only for proofs that still need LucidTrading (or market-hours
rollover) — not for ticker resync. Resync is already proven on Rithmic
Test (`tests/e2e/test_reconnect_live.py`, 2026-08-24). Remaining
Lucid-preferred cases: TC-D40 15m/1h/1d first-payload (Test 1m PASSED;
slower periods often skip within 65s), and optional TC-D54 re-eval.

```bash
# Refused without the flag; a flag inside the dotenv file is ignored.
RITHMIC_TEST_DOTENV=/secure/local/lucid.env RITHMIC_ALLOW_LUCID_E2E=1 \
uv run pytest tests/e2e/test_data_client_live.py -k tc_d40 -v
```

The adapter prints a loud WARNING to stderr when the override is active.
Set it only with MotiveWave / R|Trader closed — same one-login system.
Unknown system names stay refused. Venue gaps: skipped-spec below.

Book refresh contract (Rithmic Test, NQZ6/CME, 2026-10-15): repeat `subscribe_order_book_summary` is rejected (`[1029] update bit type already exists`); unsubscribe + subscribe resends the snapshot, so `Venue::refresh_book` uses that sequence ([design D8.10](../../openspec/changes/unify-session-host/design.md#d8-gaps-found-in-review-2026-10-15)).

## Skipped-spec register

Known test skips that are venue/environment facts, not adapter bugs. Each
entry names what would unblock it. Phase 7 deliverable.

| Skip | Reason | Unblock |
| --- | --- | --- |
| TC-D10 L2 book | LucidTrading denies book permission (`[13] permission denied`) | FCM/account with book entitlement |
| TC-D31/D41/D42 history empty | LucidTrading history plant transiently returns empty windows | Retry at market hours; no code change |
| TC-D40 15m/1h/1d on Rithmic Test | `D40[1m]` PASSED 2026-08-24; slower periods SKIPPED (no first payload in 65s — likely need rollover or Lucid) | LucidTrading via `RITHMIC_ALLOW_LUCID_E2E=1`, or longer poll / roll-boundary runs |
| TC-D54 full-node bar parity | Dropped: under some Test off-hours states ticker can go quiet while bars still run — INTERNAL==EXTERNAL parity not a reliable Test host | Re-evaluate at market hours; or Lucid |
| P1 exec e2e self-skip | Tests still self-skip on venue signature `COMPLETE` + `No such route exists.` (historical 2026-08-22 routing outage). Routing healthy again 2026-08-24 (E88/E89 PASSED). | Keep skip for future regressions; no action when routing is healthy |
| A4 client-order-id validation | OQ1: Rithmic `user_tag` length/format constraint unsourced | Vendor answer from Rithmic support |

Closed (kept for archaeology — do not re-open without new venue evidence):

| Closed | When | Note |
| --- | --- | --- |
| TC-D40 all-periods silent on Test | 2026-08-24 | Superseded: 1m streams; row above covers slower periods |
| P0.2 resync blocked on silent ticker | 2026-08-24 | `test_reconnect_live.py` PASSED; `[8] already exists` tolerated in `replay_subscription_intent` |
| P1 E88/E89 blocked on Test routing | 2026-08-24 | Drain + MY043 canary PASSED on Test |

## Wheels

`.github/workflows/wheels.yml` builds both wheels on every push to `main` and on
`v*` tags (Linux x86_64 / ARM64, macOS ARM64), installs them into a clean venv
with `--no-build` and runs the python-client scenarios against the fake parent,
then uploads them to the rolling `main-dev` prerelease (or the tag's release)
and regenerates the PEP 503 index on GitHub Pages (`scripts/publish_index.py`,
last 30 dev builds). Index URL: `https://alexlapchenko.com/rithmic-nt-connect/simple/`.
The account's user site carries that custom domain, so the `ozeron.github.io`
URL 301s to it over plain http; use the https custom-domain URL directly.
Consumer setup: README "Install". One-time repo setting: Pages source = GitHub
Actions.

`rithmic-gateway` is a maturin bin wheel: the binary lands in the venv's `bin/`
next to the client. Build one locally with `cd python && uvx maturin build --release`.
`maturin develop` (step 4 above) builds only the adapter; in a checkout
`resolve_gateway_bin` falls back to `PATH` and then `target/{release,debug}`.

## Connect mode (required for Nautilus adapter)

Set **`RITHMIC_CONNECT_MODE`** (or `SessionConfig.connect_mode` / `ConnectMode`) to one of:

| Value | Who opens Rithmic | Use when |
| --- | --- | --- |
| `direct` (`ConnectMode.DIRECT`) | This process (flock + PyO3 plants) | Single Nautilus / script process |
| `gateway` (`ConnectMode.GATEWAY`) | Only `rithmic-gateway`; clients dial `unix://…` | Multiple processes share one login |

Unset or any other value → `ConfigError` in the **adapter**. There is no silent default.
**market-data-lake** always uses the gateway client (ignores this key).

```bash
# Start the parent explicitly: it owns the flock + plants and needs RITHMIC_PASSWORD
# in ITS environment. Gateway clients never spawn it and never hold the password.
# Leave RITHMIC_GATEWAY_IDLE_EXIT_SEC unset/-1 so the parent stays up (service default).
rithmic-gateway               # installed wheel (or: python -m rithmic_gateway), env unchanged
# From a checkout (no wheel):
cargo build -p rithmic-gateway --release
RITHMIC_GATEWAY_BIN=$PWD/target/release/rithmic-gateway python -m rithmic_gateway
# (or run the binary directly: cargo run -p rithmic-gateway --bin rithmic-gateway)

# Child / lake (no maturin / no Nautilus): the rithmic-gateway wheel (README "Install"),
#   or from a checkout: PYTHONPATH=<repo>/python  +  pip install 'protobuf==5.29.6'
# Env aliases (WSS URL): RITHMIC_GATEWAY, else RITHMIC_URL; system: RITHMIC_SYSTEM,
#   else RITHMIC_SYSTEM_NAME; user: RITHMIC_USER, else RITHMIC_USERNAME. Parent binary
#   and Python clients share this precedence (`rithmic_gateway::env_keys`), so one env
#   file is one login — the binary used to read only RITHMIC_URL and sent a
#   RITHMIC_GATEWAY test login to production.
# Listen socket:        RITHMIC_GATEWAY_LISTEN=unix://…  (never use as WSS URL)
# Clients are attach-only: one socket per process and credential fingerprint,
# shared by the data and exec factories. Retired client knobs fail fast: setting
#   RITHMIC_GATEWAY_CLIENT_MODE, RITHMIC_GATEWAY_SPAWN_POLICY or
#   RITHMIC_GATEWAY_AUTO_SPAWN to any value raises ConfigError; unset them
#   (start the parent explicitly).
# Adapter gateway mode uses the Rust client inside the extension (there is no
#   client-implementation selector).
# Trading / cancel_all are parent-gated (independent toggles):
#   RITHMIC_ENABLE_TRADING=1          # place / modify / cancel / order updates
#   RITHMIC_GATEWAY_CANCEL_ALL=1      # plant-wide cancel_all panic button (does not require trading)
#     Direct mode honors the same two switches: the Rust session itself denies trading RPCs
#     (`trading_disabled`) until the exec client opens the gate from `enable_trading`, and
#     denies plant-wide cancel_all (`cancel_all_denied`) unless RITHMIC_GATEWAY_CANCEL_ALL is set.
# Optional remote-ready token (non-empty client auth_token must match):
#   RITHMIC_GATEWAY_AUTH_TOKEN=…
# Idle-exit after last client (parent lifetime):
#   RITHMIC_GATEWAY_IDLE_EXIT_SEC unset/-1 = stay until SIGTERM (manual/standalone)
#   0 = exit immediately when peers hit 0; N = grace seconds
#   Only the parent's own environment sets this; the launcher and clients never do.
#   (The test harness helper scripts/_gateway_parent.py defaults it to 60 so a killed
#   smoke cannot orphan a parent holding the login.)
# Clients refuse a dialable unix path unless the credential flock is held
# (impostor protection).
#
# Wide history: GatewayClient.load_time_bars_range chunks calendar windows
# sequentially (same slice lengths as Rust `bar_slice_secs`: 2h for ticks and 1s
# bars, 40 days for 1m bars, 90 / 180 days for 15m / 60m+ bars, one wide window
# for daily/weekly) so each unix RPC stays within timeout/frame limits.
#
# --- Live XOR heavy history ingest (2026-08-25 MY043 / RC2) ---
# Do NOT run multi-year catalog ingest (or other long LoadTicks/LoadTimeBars)
# on the same gateway process that serves live ticker/book/time-bar clients.
# History RPCs used to hold the plant session mutex for the whole load, so the
# event pump could not drain ticker → silent live MD, then lag. That lock is
# gone; heavy history still competes with live MD for the same plant login.
# The gateway refuses Load*/probe with error code `history_denied_live_md`
# while any ticker/book/time-bar intent is live — lake/qgw must use a
# separate gateway (or stop live MD) for heavy history. The live-MD gate is
# held only for that check (2026-10-14): a subscribe arriving mid-load goes
# ahead instead of waiting for the load, and loads are serialized by their own
# history lock, which never blocks subscribes, places or cancels.
# Read-only barrier: GatewayClient.get_live_md_state() / GetLiveMdState RPC
# (no plant lock). Deploy handoff: wait until live_md=false before hydrate.
# qgw RequestBars retries capability_denied for RITHMIC_HISTORY_LIVE_MD_WAIT_SEC
# (default 45) then fail-closes as HYDRATE_BLOCKED_LIVE_MD.
#
# --- History Ready ≠ plants connected (2026-08-31 MY043 / RC4) ---
# History plant login can still leave Load* silent until rithmic-rs
# DEFAULT_REQUEST_TIMEOUT (30s → `request timed out`). The parent now proves a
# tiny 1m Load* inside `Plants::connect` before advertising Ready /
# "plants connected (...; history ready)". Production Load* also retries
# RequestTimeout (transient). Override probe with
# RITHMIC_HISTORY_READY_SYMBOL / RITHMIC_HISTORY_READY_EXCHANGE (else
# RITHMIC_SYMBOL / RITHMIC_EXCHANGE, else ES/CME). RC2.3 refuse-under-live-MD
# is unchanged — a failed mid-session hydrate still cannot retry Load* while
# live ticker intents are up; qgw fail-closes empty mid-session lookback (no
# arm). Optional: raise RITHMIC_REQUEST_TIMEOUT_SECS only if cold plant needs
# >30s before first byte (global; slows every plant).
# Accounts without market data (e.g. Rithmic Test: no CME front month / bars)
# never answer the probe; RITHMIC_HISTORY_READY_PROBE=0 skips it so the order and
# PnL plants are still reachable. Never set it for a node that hydrates.
#
# Keep a long-lived live parent alive under a service manager (agent shells / nohup
# without a new session die on SIGHUP when the parent shell exits): see "Running the
# parent as a service" below.
```

## Running the parent as a service

The parent is started by its operator, never by a client. The wheel installs a
`rithmic-gateway` executable in the venv's `bin/`; run it directly, or use
`python -m rithmic_gateway`, which resolves the binary (`RITHMIC_GATEWAY_BIN`, then the
wheel's scripts dir, then `PATH`, then a checkout's `target/`) and `exec`s it with the
caller's environment: it does not detach and does not set
`RITHMIC_GATEWAY_IDLE_EXIT_SEC`. Put the credentials in the unit's
environment (the parent needs `RITHMIC_PASSWORD`; clients do not). If no binary
resolves it exits `2` naming `RITHMIC_GATEWAY_BIN`.

**systemd** (`~/.config/systemd/user/rithmic-gateway.service`):

```ini
[Unit]
Description=Rithmic gateway parent (one Rithmic login)

[Service]
# RITHMIC_USER, RITHMIC_PASSWORD, RITHMIC_SYSTEM_NAME, RITHMIC_URL, ...
EnvironmentFile=%h/.config/rithmic-gateway.env
ExecStart=%h/.venvs/rithmic/bin/rithmic-gateway
Restart=on-failure

[Install]
WantedBy=default.target
```

**launchd** (`~/Library/LaunchAgents/com.example.rithmic-gateway.plist`): `ProgramArguments`
= `[<venv>/bin/rithmic-gateway]`, credentials under `EnvironmentVariables`,
`KeepAlive` true.

**Kamal accessory**: run the same command as the accessory `cmd`
(`rithmic-gateway`), pass the secrets as accessory `env.secret`, and mount
the runtime directory so book and lake containers see the same
`RITHMIC_GATEWAY_LISTEN` socket path. Clients carry no password and fail with
`parent_not_running` until the accessory is up.

**`parent_not_running`**: a client dialed `RITHMIC_GATEWAY_LISTEN`, found no listener,
and no process holds the credential flock. The error names the listen path and the
launcher command. Start the parent (above) and retry; do not expect the client to do it.

If a second direct/parent process hits the same `user|system|url|env` flock, it fails locally before a second Rithmic login. **Attach path:** dial fail + flock held → bounded wait (`attach_timeout_sec`, default 90s) for the existing parent socket; dial fail + flock free → `parent_not_running` (the client never spawns). Flock released while waiting → `parent_flock_held_no_socket`. **Wrong listen path** (`RITHMIC_GATEWAY_LISTEN` ≠ running parent) looks like flock held but dial never succeeds — align paths on both sides. Default sock/lock paths live under `XDG_RUNTIME_DIR` or a private `$TMPDIR/rgw-$UID/` directory (short names so macOS `sockaddr_un` fits); oversized paths clamp to `/tmp/rgw-$UID/<hash8>.sock` (private dir, not a bare sticky `/tmp` file).

Remote / Docker / TLS: [`gateway-remote.md`](gateway-remote.md) (v1 local unix; v1.5 tunnel recipe; v2 TLS not implemented).

The adapter does **not** submit orders by default. Order APIs exist on the
session / exec client only when `enable_trading=True` (or `RITHMIC_ENABLE_TRADING=1`).
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

### Soft mass-status history contract (MY043 2026-09-02 / proof 2026-09-03)

Startup `generate_mass_status` soft-completes empty/unavailable order drains so
the TradingNode can start (RC-5). It must **not** feed that best-effort drain
into Nautilus `LiveExecutionEngine._adjust_mass_status_fills` /
`adjust_fills_for_partial_window`: that compatibility path invents synthetic
`OrderFilled` events with `venue_order_id=S-…` when fills do not explain the
venue position, and `generate_missing_orders=False` does **not** gate it (only
later position-report repair). Warm Redis + plant claim + S- fill double-counted
OMS on MY043 (incident `2026-09-02-002` in quant-guild-work).

Adapter contract on soft mass-status:

- Attach **position** reports (venue qty authority), including explicit **FLAT**
  rows for instruments that are open in the NT cache but absent from the venue
  PnL map (so startup recon sees Redis ghosts).
- Attach **order** reports only when the order drain succeeded.
- **Never** attach fill reports (empty list), even when the fill drain returned rows.
- Call `ExecutionMassStatus.set_report_window(lookback_start, reports_complete=False)`
  when the installed NT exposes it. **NT 1.231.x has no setter / no
  `lookback_start` field** — fill omission is the fail-closed defense against
  S- invent on that line. Master / 2.0.x documents the bound; Python bindings
  may still lack the setter (Rust in-tree adapters use `set_report_window`).

#### Warm Redis + plant flat (live proof 2026-09-03 ~07:36Z)

With `QGW_LIVE_RECON=1`, plant flat (`working_orders=0 positions=0`), warm Redis:

- Soft mass-status ran; adjust ended with `order_reports=0, fill_reports=0` (no S-).
- Portfolio still `net_position=1` / `Initialized 1 open position` (Redis ghost).
- Continuous check: `cached_qty=1, venue_qty=0` — NT will **not** invent a
  closing fill when `generate_missing_orders=False`, so OMS stays wrong.
- Strategy `trade recover freeze: orphan position` is the correct latch.

**Incomplete / lookback marking does not clear Redis ghosts** on NT 1.231 with
`generate_missing_orders=False`. Declaring `reports_complete=False` (when the
API exists) only avoids the S- invent path; it does not wipe cache open qty.

**qgw_book / operator requirement before recon-on with warm Redis:**

1. Confirm plant flat (`make flatten-status` or equivalent).
2. **Flush Redis** (or start with a cold/empty cache) so OMS cannot restore a
   ghost open qty.
3. Only then set `QGW_LIVE_RECON=1` / `reconciliation=True`.

**Do not re-enable default `QGW_LIVE_RECON=1` until** a supervised warm-open
after Redis flush proves: OMS net == plant net, and jsonl has no
`Generated OrderFilled` / `S-` synthetic ids. Keep default off until that proof.
Do not monkeypatch-drop S- fills in qgw_book.

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

## Benign exec-engine warnings (MY043-001, 2026-08-21)

Two `ExecEngine` WARNs seen in the MY043-001 live log are engine-side noise the
adapter now feeds cleanly; both are pinned against a real `LiveExecutionEngine`
in `tests/test_exec_engine_repro.py`:

- `report.avg_px was 'None' when a value was expected` — a status query on a
  bracket stop whose OPEN Rithmic defers (local `SUBMITTED`) used to fall
  through the engine's transition table into fill reconciliation. The
  cache-backed query answer now reports such legs as `ACCEPTED`
  (`_order_status_report_for`), which short-circuits before the warn.
- `InvalidStateTrigger: CANCELED -> ACCEPTED` — an open-order drain row taken
  before a cancel propagated used to reconcile ACCEPTED over a locally
  CANCELED leg. The bulk drain now suppresses stale non-terminal rows for
  locally closed orders (`row_stale_reason` in `recon.py`, backed by
  `stale_reason` in `crates/rithmic-exec`); terminal-vs-
  terminal rows (venue FILLED vs local CANCELED) still pass through.

Residual exposure: Nautilus applies recon reports without a monotonic guard,
so a race entirely inside the engine (report built before a cancel, applied
after) can still warn once. Adapter-fed paths are guarded; treat a single
occurrence right after a cancel as benign, repeated ones as a bug report.

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

## Venue and account quirks

- **Wrong system looks like "permission denied".** A LucidTrading prop login fails on Rithmic Test (`rituz00100`); use system `LucidTrading` with `wss://rprotocol.rithmic.com:443`. Rithmic Test needs its own test credentials.
- **Expired contract months have no history.** Rithmic history for expired contracts is empty; use another venue or the lake for deep history and Rithmic for the listed front month and live-adjacent checks.
- **Continuous roots are front aliases.** `CME:NQ`-style roots behave like the front month, so roll days diverge from continuous research series (e.g. Databento `NQ.c.0`).
- **Account state comes from the exec client.** On Nautilus 1.231.x account state is published by `ExecutionClient.generate_account_state`, not by a data client, so PnL / positions need the exec client even when trading is disabled (read-only exec client; it never submits).
- **Order-plant reachability is not submit authorization.** Logging in to the order plant works on a LucidTrading account; routing orders needs trading enabled and a conformance-issued `app_name` (see `AGENTS.md` Safety).
- Do not redistribute gated Rithmic `.proto` sources; keep them out of public git.


## Plant reconnect in the gateway parent

When a plant connection drops, the parent rebuilds the plants and re-issues every remembered subscription itself (ticker, book, time bars, PnL, order updates, brackets). Attached clients stay connected and receive a `TransportResumed` event (the Rust client turns it into `TransportDown` / `TransportUp`); market-data subscriptions are not issued a second time, and the exec client only re-reads account state (working orders, PnL) before trading resumes. A market-data subscription that cannot be restored after 3 passes (for example an expired contract) is left degraded: the gateway becomes ready and the next `subscribe` for that key retries it. An account-level subscription (PnL, orders, brackets) that keeps failing makes the parent reconnect fully and stay not ready. `RITHMIC_GATEWAY_RESTORE_BACKOFF_MS` (default 2000) sets the pause between passes; tests lower it. Scenario `reconnect_during_flood` in the load harness checks this behavior. Live reconnect on a market-data account is still unverified here.

## Load harness

`rithmic-load` (crate `crates/rithmic-loadtest`) drives the real gateway server, client and PyO3 layers over a fake plant, in one process, with no credentials. See `docs/perf/README.md` for the commands, how to read a report, and the recorded baselines.

```bash
cargo build -p rithmic-loadtest --release
./target/release/rithmic-load suite --out docs/perf/report.json     # ~8 min
./target/release/rithmic-load smoke                                  # CI subset, ~25 s
RITHMIC_PERF=1 uv run pytest tests/test_perf_ffi_throughput.py -s    # Python boundary
```

`smoke` is what CI runs (ticker flood at 10k events/s, order isolation behind a slow read, a stalled consumer). Its floors are deliberately loose: it fails on events lost without a gap, reordering, a place held up by a slow read, or a throughput collapse, never on a latency tail. The absolute budgets in `openspec/specs/gateway-throughput` are checked by the manual `suite --full` run and recorded in `docs/perf`.

Run it on a quiet machine and repeat before trusting a tail number: with a load average above the core count, tail latencies move by 10x between runs.
