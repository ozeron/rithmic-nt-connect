# Agent notes — rithmic-nt-connect

Unofficial out-of-tree Rithmic adapter for NautilusTrader **1.231.x**.
Read this before changing adapter, wire, or docs.

## Must follow

Semantic rules for this adapter:

**[`docs/references/nautilus-adapter-conventions.md`](docs/references/nautilus-adapter-conventions.md)**

Read that file (or the sections you are touching) before implementing or reviewing data, execution, config, or plant code. In-tree Rust v2 machinery is N/A here; the **semantic** conventions still apply.

### Direct ↔ gateway plant-surface parity (hard)

Direct and gateway are two implementations of one Rust trait, `Session`
(`crates/rithmic-host/src/backend.rs`): `RithmicVenue` (in-process plants, owns
the order-op gate) and `GatewaySession` (`crates/rithmic-gateway/src/client`,
attach-only, one socket per fingerprint). The PyO3 `Session` and the gateway
**server** (`crates/rithmic-gateway/src/server`) are both generic over that trait,
so the adapter has one call surface in either mode, including the read-only
fill-history and order-history queries (one-shot, trading-gated reads).

When you add or change a plant RPC (place / cancel / modify / brackets / ensure / subscribe / …):

1. Add it to the `Session` trait, with `RithmicVenue` and `FakeVenue` (feature `testing`) implementations.
2. Add its row to the RPC table (`crates/rithmic-host/src/rpc_table.rs`: kind + gate; gateway table: Body mapping), then the `session.proto` message, the server dispatch, the `GatewaySession` call (with its call mode: Read / Command / Intent), and the PyO3 method plus `_lib.pyi`.
3. Keep request field names and semantics aligned (e.g. bracket `localid` / `stop_ticks` / `target_ticks`).
4. Tests that fail on drift: `tests/rpc_table.rs` (proto ↔ trait ↔ table ↔ `RestorePlan`), `tests/client_scenarios.rs` + `tests/test_gateway_scenarios.py` (same scenarios through every client), `tests/test_wire_trait_parity.py` (PyO3 ↔ `WireSession`).
5. Do not ship “direct-only” for a new live capability unless STATUS marks gateway as deferred **and** book/runners refuse that mode.

Structure now enforces the first half of this: declare the RPC on `WireSession` in `session.py` and
`tests/test_wire_session_parity.py` fails until every adapter exposes it with a compatible signature: `_FlockedDirectSession` (direct),
`_RustGatewaySession` (gateway; both in `python/rithmic_nt_connect/session.py`), the test double
`WireSessionStub` (`tests/_stubs.py`), and the compiled extension via the hand-written `_lib.pyi` (itself
checked against the built extension). `tests/test_gateway_proto_parity.py`
fails when `session_pb2.py` was not regenerated after editing `session.proto`. Proto framing and gateway
dispatch/gate tests (`crates/rithmic-gateway/tests`) are still yours to extend.

#### Learning: gateway RPC kinds (do not confuse)

Gateway plant RPCs are **not** all the same shape. Shipping a new stream as a bare `session.subscribe_*` + Ack is how bracket updates lost fan-out delivery and reconnect restore. The RPC table's `kind` column is the single place this is decided, and its tests enforce it.

| Kind | Examples | Must include |
| --- | --- | --- |
| **One-shot command** | `place_*`, `adjust_*`, `cancel_*`, `modify_*` | Trading gate → plant call → map error. No fanout / `note_*` / restore. Client side: never retried after the request was written (`OutcomeUnknown`). |
| **Subscription intent** | ticker, book, time bars, PnL, **order updates**, **bracket updates** | Gate → attach hub fanout → `note_*` refcount → plant only on 0→1 → rollback on fail → **`restore_intents` re-issues after plant reconnect**. |

Every RPC also declares a **lane** in the table. A connection reads requests on its own task and serves them on two lanes: the **command** lane (subscriptions and every state-changing request) runs one request at a time in arrival order; the **slow** lane (history, order-history and fill-history loads, plus reference-data, front-month and resolved-account reads) runs a few at a time and replies as each finishes, so a slow read can never hold up a place or cancel on the same socket. New RPCs default to the command lane; a read that can take seconds is marked `slow(...)` in its table row, which is the only place lanes are declared (`rpc_table::lane_of` reads the table; `tests/lanes.rs` guards it). Never put a request that mutates the connection's `ClientCtx` on the slow lane.

**Recovery.** After a plant reconnect the parent re-issues every intent itself and keeps clients attached, sending each a `TransportResumed` event; clients must not re-subscribe or reset the shared plant. A gateway client never calls `reset_ticker` (only a direct session does); a stream `gap` is a resync of that consumer's intent with no reset. A new subscription intent needs its `RestorePlan` / `restore_intents` entry, and a market-data one a degraded-set path (`MdIntent`).

Bracket notifications ride the **order-plant** stream. Use `subscribe_order_plant_stream` in `server/dispatch/subscriptions.rs` (order and/or brackets flags) — never a one-shot plant call. Extend `RestorePlan` + `restore_intents` and the table row; `intent_rows_cover_exactly_the_restore_plan_fields` and `every_subscription_intent_is_reissued_by_restore_intents` fail otherwise. Proto string greps alone are not enough.

### Execution rules live in `rithmic-exec`

Venue-semantics rules that do not need Nautilus are implemented once, in the pure
`crates/rithmic-exec` crate (no plants, tokio, or Nautilus): the order-plant
lifecycle policy, exposure and commission lookup, the notification-kind mapping
(shared with `rithmic-plants`) and the per-kind notification action, book
`update_type` kinds, fill identity and dedup, the instrument-id codec,
the bounded recency cache, drain-row trust / freshest-row / stale-row decisions,
and inventory rules. The Python modules (`_order_plant.py`, `exposure.py`,
`commission.py`, `_convert.py`, `_orders.py`, `recon.py`, `inventory.py`) keep
only Nautilus translation, Python-object coercion (`enum_int`, `sentinel_none`,
`format_price_str`) and effects (publish, cache write, ordering).

- Change a ported rule in Rust, with a Rust test. Do not re-add it in Python.
- `tests/test_exec_rules_parity.py` keeps the original Python of each ported rule as an oracle; extend it when a rule gains inputs.
- New rule over typed values with no Nautilus dependency: add it to the crate. Rule that depends on what Python does to an arbitrary object: keep it at the Python boundary.

Related:

| Doc | Use |
| --- | --- |
| [`docs/references/nautilus-adapter-phases.md`](docs/references/nautilus-adapter-phases.md) | Upstream phase pattern (0–9) |
| [`docs/STATUS.md`](docs/STATUS.md) | Planned vs done; update marks when you close a gap |
| [`docs/references/nautilus-adapter-tiers.md`](docs/references/nautilus-adapter-tiers.md) | 1.231.x `TradingNode` seam; naming |
| [`docs/references/ops-runbook.md`](docs/references/ops-runbook.md) | How to run smokes |
| [`docs/references/gateway-remote.md`](docs/references/gateway-remote.md) | Gateway unix / tunnel / TLS roadmap |
| [`docs/adr/`](docs/adr/) | Decisions an architecture review should not re-litigate |

Do not invent a second status table. Do not copy upstream DataTester / ExecTester case lists into the repo.

## Review against conventions

After a change that emits Nautilus data or execution events, check the rows that apply. If you cannot mark a row, say so and point at the convention section. Lasting marks live in [`docs/STATUS.md`](docs/STATUS.md), not here.

### Config / identity

- [ ] Secrets stay out of `repr`, errors, and logs
- [ ] Credentials resolved at construction, not in request methods
- [ ] Live / Demo / Test endpoints cannot mix accidentally
- [ ] Distinct venue instruments never share an `InstrumentId`
- [ ] `connect_mode` / `RITHMIC_CONNECT_MODE` is explicit (`ConnectMode.DIRECT` \| `GATEWAY`; no silent default)
- [ ] Gateway-mode clients hold no Rithmic password and never spawn the parent (attach-only, one shared socket per process; the parent is started explicitly)

### Data

- [ ] Wire → Nautilus at one convert boundary; unknown closed-set values rejected
- [ ] `ts_event` from venue when present; `ts_init` from adapter clock
- [ ] Subscribe is intent; do not confirm from local send success alone
- [ ] Requests complete (data, empty, or error) — never hang
- [ ] Book snapshots end `F_SNAPSHOT | F_LAST`; every logical group ends `F_LAST`
- [ ] Do not advertise `INTERNAL` bars as venue `EXTERNAL`, or L2 summary as L3

### Execution

- [ ] Pre-send local failure is deny (no `OrderSubmitted`), not a venue reject
- [ ] After send, transport / timeout / channel errors stay **unknown** (in flight), not `OrderRejected`
- [ ] Tracked orders → typed events; untracked → reports only; never drop exec events
- [ ] Fills deduped by venue trade / match id
- [ ] No `[]` meaning “venue empty” when there is no snapshot API
- [ ] State-changing place / cancel / modify is not blindly retried

### Lifecycle

- [ ] Partial connect failure tears down what started
- [ ] Reconnect restores auth **and** subscription intent, not only the socket
- [ ] No nested `block_on` on the asyncio loop (PyO3 `block_on` stays in the extension; Python uses `asyncio.to_thread`)

### Advertise / docs

- [ ] Only claim capabilities that are implemented **and** tested
- [ ] Checklist marks updated if advertised status changed
- [ ] Testers stay dry-run unless the user explicitly opts into live place

## Safety

- One Rithmic login session — do not run against LucidTrading while MotiveWave / R|Trader is open.
- Trading is off unless `enable_trading=True` / `RITHMIC_ENABLE_TRADING=1`.
- Only the gateway **parent** holds `RITHMIC_PASSWORD`; gateway-mode clients (`SessionConfig`, `GatewayConfig`) never read, hold, log, or send it, and the client library never spawns a parent. Start the parent explicitly (`python -m rithmic_gateway`, a service manager, or the harness helper `scripts/_gateway_parent.py` for smokes/live e2e). Retired knobs (`RITHMIC_GATEWAY_CLIENT_MODE`, `SPAWN_POLICY`, `AUTO_SPAWN`) raise a config error for any set value; unset is the only valid state (`ConfigError` from `SessionConfig`, `GatewayConfigError` from the `rithmic_gateway` library).
- Do **not** run `scripts/verify_order_dry_run.py --live-place` unless `RITHMIC_ENABLE_TRADING=1` is set and an explicit far `--price` is passed (BUY below / SELL above market). Test-plant order routing with `DEFAULT_APP_NAME` is confirmed authorized (proven 2026-08-17: order routed to exchange); production `Rithmic 01` / LucidTrading place still needs a conformance `app_name`.
- Do not commit secrets, certs, or gated `.proto` sources.
- Do not commit one-off dev probe scripts; live checks belong in mechanism-only e2e tests (`tests/e2e/`) or documented runbook commands.
- The wire/session `cancel_all_orders` is plant-wide; do not use it to clean up a smoke order. NT `CancelAllOrders` on the exec client is cache-scoped (instrument / strategy / side, per basket); orders the cache does not hold as open are not reached.
- Gateway `cancel_all` is an independent parent panic button (`RITHMIC_GATEWAY_CANCEL_ALL=1`): it does **not** require `RITHMIC_ENABLE_TRADING`. Still plant-wide — never use it to clean up a single smoke order.

## Verify

**Venue-shaped fixtures.** When a change reads a field whose venue semantics
matter (symbol vs product code, omitted vs `None`, closed-set enums), unit
fixtures must use the real payload shape, and the happy-path test must keep
that shape — overriding a fixture default to make a lookup hit is a red flag
(2026-08-19: fills were charged $0 because the commission lookup keyed
contract symbols like `MNQU6` against product-code rates like `MNQ`; the
fixture already defaulted to `NQU6` and the tests overrode it).

Gateway client / wire tests need the plants/gateway crates, a system `protoc`
(`protobuf-compiler` on Debian/Ubuntu; `brew install protobuf` on macOS), and
the protobuf Python package (core dep):

```bash
uv sync --extra dev
# Rust: fmt (hk pre-commit/check only) + clippy (-D warnings) + tests (hk check
# + CI). clippy is the Rust analog of `ruff check`; `cargo fmt --check` runs
# locally via the hk hooks but is NOT a CI gate (the pre-commit hook enforces it).
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-exec -p rithmic-nt-connect
uv run ruff check .
uv run ruff format --check .
uv run ty check python/rithmic_nt_connect python/rithmic_gateway tests
uv run pytest -q
# Qlty (added 2026-08-20): orchestrator over ruff/clippy + bandit/trufflehog/
# shellcheck/actionlint/zizmor/osv-scanner/radarlint/smells. CI runs the
# `check` default (changed files vs base); `--all` is only for local full-
# repo scans and triage baseline reviews. `--sample=5` is a fast no-fail
# smoke for first-time wiring.
qlty check --fail-level=medium    # matches CI PR gate (changed files)
qlty check --all --level=low      # local full scan / baseline inventory
qlty smells --all                 # duplication / complexity report
qlty check --sample=5             # cheap visibility check
# Docs: STATUS.md scoreboard must match its own [x]/[~]/[ ] marks
python scripts/status_progress.py --check
```

Also regenerate the scoreboard after editing `docs/STATUS.md`:

```bash
python scripts/status_progress.py            # print rollup to paste into the At-a-glance table
python scripts/status_progress.py --check    # CI/docs guard: fails if the table drifts
```

Live (creds in `.env`, MotiveWave closed):

```bash
# Live test-account e2e (uses the repo .env; refuses production systems)
RITHMIC_TEST_DOTENV=.env uv run pytest tests/e2e -v
python scripts/smoke_lucid_nq.py
python scripts/smoke_gateway_shared_ticks.py --seconds 25
python examples/live_nq_intraday_sandbox.py --seconds 90
python scripts/verify_order_dry_run.py --seconds 5
```
