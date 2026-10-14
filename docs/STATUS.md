# rithmic-nt-connect — adapter status

**Planned vs done** for this unofficial out-of-tree Rithmic adapter on NautilusTrader **1.231.x**. Not a PR log (that lives in git).

**30-second read**

| | |
| --- | --- |
| **Done enough to use** | Futures MD (trades/quotes + history), paper path, account auto-discovery, TC-D data sweep, order dry-run, test-plant live place (2026-08-17), self-contained wheel |
| **Partial / advertised incomplete** | L2 book, EXTERNAL bars (1m proven; 15m/1h/1d open), order types/brackets (Test accept/survive both modes), PnL, submit/cancel/modify honesty, recon, gateway shared login, exec hardening |
| **Not started** | Definition updates · recovery suite — **2** marks left |
| **Out of scope (N/A)** | Non-futures · L3 MBO · mark/index/funding/greeks · catalog/Parquet · Python v2 LiveNode · native TLS remote · in-tree Rust v2 |

Acceptance target: **NQ / CME via LucidTrading**. One Rithmic login (close MotiveWave / R|Trader).

- Phases: [`references/nautilus-adapter-phases.md`](references/nautilus-adapter-phases.md)
- Conventions: [`references/nautilus-adapter-conventions.md`](references/nautilus-adapter-conventions.md)
- Runtime seam: [`references/nautilus-adapter-tiers.md`](references/nautilus-adapter-tiers.md)

In-tree Rust v2 build surfaces are N/A; semantic conventions still apply.

Legend: `[ ]` not started · `[~]` partial / advertised but incomplete · `[x]` done · `N/A` out of scope / not advertised

---

## At-a-glance (generated — run `python scripts/status_progress.py`)

Legend: **Done** `[x]` · **Partial** `[~]` · **Not started** `[ ]` · `N/A` out of scope.

| Area | Total | Done | Partial | Not started | N/A |
| --- | ---: | ---: | ---: | ---: | ---: |
| **Capability matrix** | 22 | 4 | 12 | 0 | 6 |
| **Phase marks (0–9)** | 29 | 14 | 9 | 2 | 4 |
| **Convention marks** | 18 | 14 | 4 | 0 | 0 |
| **Close-outs (advertised path)** | 10 | 5 | 3 | 0 | 2 |
| **Paper path (intraday)** | 8 | 8 | 0 | 0 | 0 |
| **TOTAL** | 87 | 45 | 28 | 2 | 12 |

**Implemented:** 79% (`45` done + `28` partial of `75` in-scope marked items (`12` N/A excluded); partial counts as half).

Not started (real leftover work): Phase 2 definition updates, Phase 7 recovery suite.

---

## Capability matrix

| Axis | Planned | Status |
| --- | --- | --- |
| Products | Futures only | [x] futures (`FuturesContract` from reference + front month). Other families **N/A**. No live definition-update stream. |
| Environments | `SessionConfig.env` Live / Demo / Test | [x] LucidTrading + `wss://rprotocol.rithmic.com:443`. |
| Account modes | Margin + `OmsType.NETTING`; trading gated | [~] `enable_trading=False` default. Trading / PnL need FCM / IB / account triple. |
| Live trades / quotes | `TradeTick`, `QuoteTick` | [x] |
| Order-book summary | `OrderBookDeltas` (L2) | [~] venue `update_type` honored (2026-10-14): snapshot (3) → CLEAR + ADDs `F_SNAPSHOT`, begin/middle/end (4/5/6) one batch with `F_LAST` only at end, solo (7) its own `F_LAST` group, clear / no-book (1/2) → CLEAR; size 0 → DELETE; unknown types rejected at the convert boundary (offline-tested; live `update_type` histogram logged by the e2e book test). Book unsubscribe wired plants/gateway/direct. |
| History ticks / time bars | Request path, `*_all` | [x] see notes |
| Live venue `EXTERNAL` bars | 1m / 15m / 1h / 1d | [~] see notes |
| Depth-by-order / L3 MBO | Not advertised | **N/A** until a separate slice |
| Mark / index / funding / greeks | Not advertised | **N/A** |
| Catalog / Parquet | Other repo | **N/A** |
| Order types | Market, limit, stop / stop-limit, trailing-stop (tick offset) | [~] mapped; test-plant live place confirmed 2026-08-17 (default `app_name` authorized to exchange) |
| Brackets / OCO | Plant bracket API | [~] wire on direct + gateway; Test accept+survival proven both modes 2026-08-24 (far LIMIT from BBO, plant redial, identity cancel; the spike harness was later removed, history in git). Spike-only `RITHMIC_BRACKETS=1` + `RITHMIC_ENABLE_TRADING=1`. |
| Account / positions | Best-effort PnL | [~] auto-discovers FCM/IB/account when unset (multi-account: `RITHMIC_ACCOUNT_ID`); soft-fail PnL otherwise. Venue exposure per product root (2026-10-12): `ExposureSnapshot` (net from PnL rows, open orders from a fresh bounded drain; `None` = unknown, `complete` is the only proof) on msgbus topic `rithmic.exposure.<ROOT>` and endpoint `RithmicExecutionClient.request_exposure`; rows cleared on every (re)connect (`tests/test_exposure.py`). Position reconciliation reports positions only once PnL is live (2026-10-14): until the first PnL push after a (re)connect `generate_position_status_reports` returns no reports (Nautilus leaves cached positions untouched, it does not flatten them) and the mass-status flat augment is skipped, with a warning. Live freshness (periodic `instrument_pnl`?) unproven. |
| Submit / cancel / modify + fills | Gated order plant | [~] see notes |
| Order status reports | Venue recon when trading; cache-backed when read-only | [~] (best-effort) — `load_orders` = `show_orders` + bounded silence. Not provably complete (no end-of-list; 10k replay cap). Advisory, not authoritative. Read-only → cache-backed. **TC-E88 drain-by-identity live-proven on Test 2026-08-24.** |
| Fill reports | Venue recon when trading; read-only declines | [~] (best-effort) — `generate_fill_reports` filters the same drain for filled rows. Working-order replays carry no historical fills; fills are live-stream only. Read-only declines. |
| Fill / order history (read-only) | Offline account recon | [~] **both paths wired (direct + gateway).** `load_fill_history` (`RequestShowFillHistory`; fills capped at 10,000 rows and a full page fails closed as `reconciliation_unavailable`, so the caller splits the date range), `load_order_history_dates`, `load_order_history(date)` (summary replay, best-effort drain like `load_orders`). One-shot reads, not subscriptions: trading-gated like `load_orders` (`trading_disabled` when trading is off), no fanout / intent / restore. Present on the Rust `Session` trait (so on `DirectSession` and the Rust `GatewaySession`, with RPC-table rows and a scenario test), on the pure-Python `GatewayClient` (lake) and on `WireSession`; `tests/test_wire_session_parity.py` and `tests/test_wire_trait_parity.py` enforce the parity. **Not yet consumed** by `generate_fill_reports` or mass status (fill-omission contract unchanged). Recon scripts use a one-shot direct login — not while a gateway parent or live node holds the credential. |
| Reconnect + MD resubscribe | Planned | [~] see notes |
| Session gateway (shared login) | `connect_mode` required (`direct` / `gateway`) | [~] see notes |
| Python v2 / LiveNode | Not this support line | **N/A** |

**Protocol boundaries:** ticker, history, PnL, order plants. Public MD vs private exec.

### Capability notes (evidence)

**History ticks / time bars.** Rust slices (`crates/rithmic-plants/src/history.rs`): 2h for ticks and 1s bars, 40 days for 1m bars, 90 / 180 days for 15m / 60m+ bars, one wide window for daily/weekly; transient + empty retry, sort; dedup only of rows repeated across a slice boundary (identical prints inside a slice survive); a reply with any error frame fails (retryable `partial reply`) instead of returning a silent subset. The pure-Python lake client (`history_window.py`) uses the same slice lengths. Daily/weekly replay uses calendar `YYYYMMDD` indexes (Lucid 2026-08-13). Python: `load_front_month_instrument` / `load_trade_ticks` / `load_time_bars`. **RC4 (2026-08-31):** `plants connected` ≠ history Ready — `RithmicSession::connect` proves a tiny 1m Load* before Ready; `request timed out` is transient (1–2 retries). RC2.3 refuse-under-live-MD unchanged, but the gate is held only for that check (2026-10-14): a subscribe that arrives mid-load goes ahead, and a separate history lock keeps loads serialized against the venue; qgw fail-closes empty mid-session lookback.

**Live venue `EXTERNAL` bars.** History `request_bars` + history-plant `subscribe_time_bar_updates`. 1m EXTERNAL subscribe **live-proven on LucidTrading 2026-08-14** and **again on Rithmic Test 2026-08-24** (`test_tc_d40_subscribe_external_bars`). 1s stays INTERNAL. Bars are stamped at their close by default (`bars_timestamp_on_close=True`, the Nautilus convention; ADR 0003), `False` keeps the venue's open-time marker; the convention is logged at start. Node-path dispatch fixed 2026-08-19: venue echoes `time_bar` event `period` in seconds (`"60"`) while the dispatch key was registered with native period (`1`), so node-path EXTERNAL bars were dropped as `time_bar_unsubscribed` — keys now register under both units (unit-tested). INTERNAL-vs-EXTERNAL seam pinned by `tests/test_bar_seam_parity.py`. Bar hygiene (2026-10-12): `request_bars` drops the still-forming bar (close > request end) and a live bar whose `ts_event` was already delivered (history or live) for that BarType is skipped (`time_bar_repeat`), so hydrate→subscribe never repeats a minute (`tests/test_bar_hygiene.py`). Live full-node parity e2e (TC-D54) dropped: under some Test off-hours / prior plant states the ticker side can go quiet (stale synthetic snapshots) while bars still arrive, so INTERNAL==EXTERNAL parity is not a reliable Test host — bar delivery itself remains proven by TC-D40; ticker+book incremental resume is separately proven by `test_reconnect_live.py` (Test 2026-08-24). **Streaming is per-symbol entitlement** (LucidTrading RTH, 2026-08-19): NQ 1m EXTERNAL streams live (about one `time_bar` per 45 s), while MNQ 1m streamed 0 events in 75 s even though `load_time_bars` / `request_bars` returned correct-volume bars for both. History returns only closed bars (volume final), available about 1-4 s after the close, so a rolling 3-5 s poll of the last ~3 min that consumes only new closed bars gives 5-10 s lag. Treat `subscribe_bars(EXTERNAL)` as a live source only for streaming-entitled symbols (NQ proven 2026-08-14 via `test_tc_d40`); MNQ is history-only.

**Submit / cancel / modify + fills.** Pre-send deny; post-send unknown; untracked → reports; fill dedup. Exec hardening landed 2026-08-19 with red→green transport-e2e tests; live canaries (TC-E88 / TC-E89) passed on Rithmic Test 2026-08-24. Current behavior:

- **In-flight orders.** A status query for an order with no venue id fails closed (`VenueQueryUnavailable`, never `None`) after one bounded per-`user_tag` drain; an order whose venue id lives on the model answers directly, without a drain. The tag drain binds only usable rows (strict validation), answers with the newest matching row, and never regresses a live-resolved order.
- **Drain rows.** Interpretation is one boundary (`_drain_row_from_fields`: build-then-bind, publish-before-bind) fed by one owned iterator (`_iter_drain_rows`); identity is `_drain_client_order_id` (cache-first, closed-order guards). A timestamp-less row (synthetic `ts_event` 0) is never skipped by the freshness rule; stale rows are skipped when the live stream already advanced the order (`row_stale_reason` / `stale_reason`); malformed `ts_event` or boolean closed-set rows never abort or bind. `triggered` rows recover as TRIGGERED for limit-style stops; modify/cancel-rejected rows report as working, never terminal REJECTED.
- **Events and fills.** Overfill emits and latches (never drop/cap); stale or duplicate TRIGGER is suppressed; basket-path `is_closed` guard; venue sentinel `-1.0` → `None`; unpriceable fills are suppressed without consuming the dedup key and latch the plant; a venue fill without `fill_id` / timestamp dedupes to a stable `TradeId` (clock fallback only in the report timestamp, never identity). Report builders own the event-time timestamp fallback, so no consumer publishes an epoch-0 status or fill.
- **Coercion at the convert boundary.** `price_type` / `duration` normalize to an exact int or `None` (`enum_int` — bools and non-integral numerics never survive). `row_is_trustworthy` (`order_rows.py`, rule in `rithmic-exec`) is the presence + known-value whitelist; convert-boundary and mutation-corpus property tests pin "bindable ⟹ real closed-set terms".
- **Plant lifecycle.** An explicit state machine (`OrderPlantPolicy`: `LATCHED` is a real state, `rearm` is the only arming transition, a down plant cannot re-arm via resync, a resync during the barrier latches; transition table in `tests/test_order_plant.py`). Reconnect re-arms only after a bounded drain **and** a fresh PnL/position observation (an `asyncio.Event` activity gate; skipped when PnL soft-fails, in which case the plant re-arms on the drain alone with blind position context — see ops-runbook). A dead or broken order stream (handler break, resync failure) latches, and so do persistent transient order-stream errors (the streak is loop-local, per stream lifetime). The re-arm drain skips closed tracked orders, and the barrier aborts when publishing a drained status fails.
- **Commission.** Fills carry venue RMS commission: per-product `commission_fill_rate` (e.g. MNQ 0.5) × fill qty, falling back to the account `default_commission`, keyed by product code (`MNQ`), not contract symbol (`MNQU6`). Rates load at connect on both paths (direct PyO3 `load_product_rms_info` / `load_account_rms_info`; the same-named gateway RPCs, gated on parent trading like `load_orders`); a load failure is non-fatal (zero fallback). `tests/e2e/test_rms_commission_live.py` verifies live rates per account (direct + gateway round-trip, no live-value expectations).
- Test-plant live place confirmed 2026-08-17 (default `app_name` authorized to exchange).

**Recon honesty caveat (trading).** `load_orders` drain is best-effort and *not* provably complete — empty means "no working orders seen", not "venue has none". Trading consumers **must** pin `open_check_open_only=True` (1.231.x knob; `death_policy=trust_stop` removed in this line) so a cached open order missing from an empty recon is advisory, not **canceled**. With `open_check_open_only=False` the engine would treat an empty drain as "no working orders" and cancel tracked opens. Adapter does not enforce this; node operator owns it (see `docs/references/ops-runbook.md`). Live-venue drain-by-identity proof: **TC-E88 PASSED on Rithmic Test 2026-08-24** (`test_tc_e88_drain_reports_working_limit_by_identity`).

**Engine in-flight UNKNOWN (Nautilus-side, 2026-08-19).** After `inflight_check_retries=5`, engine `_resolve_inflight_order` synthesizes `OrderRejected(reason="UNKNOWN")` for a still-`SUBMITTED` order — **regardless of whether the adapter raises or returns `None`** (retry counter increments per issued query; raise swallowed by task runner). Adapter resolves out of `SUBMITTED` via bounded per-`user_tag` drain and fails closed (`VenueQueryUnavailable`) when it cannot resolve. Only reliable prevention when the venue genuinely does not know the order: operator `inflight_check_interval_ms=0` on `LiveExecEngineConfig` (via `TradingNodeConfig.exec_engine`; not on base `ExecEngineConfig` — see `docs/references/ops-runbook.md`).

**Reconnect + MD resubscribe.** Every path that re-establishes the wire re-issues ticker + book + EXTERNAL bar intent through one replay boundary (`replay_subscription_intent`): channel-error resync (`_resync_ticker_subscription` → `reset_ticker` (direct session only) + replay) **and** client lifecycle (`_connect` re-issues remembered intent, clears one-sided BBO accumulators, restarts history poll when bars registered — disconnect→connect previously left live plants with zero subscriptions). Duplicate venue subscribe (`[8] already exists`) is treated as success on replay (history-plant bars survive ticker reset). Gateway typed restore covers ticker/book/bars/PnL/order/brackets after plant drop. Data resync is scoped: direct resets **only** the ticker plant (`reset_ticker_plant`; PnL/order untouched); a gateway client never resets the shared parent plant (the parent restores intents itself and sends clients `TransportResumed`; a stream `gap` replays intent without any reset). `ResetTickerPlantRequest` remains wired for explicit use. Delivery, gap, lane-isolation and reconnect behavior is exercised offline by the `rithmic-load` harness (fake plant; CI runs a `smoke` subset, budgets in `docs/perf`); this is **not** live tick-rate or live reconnect verification, which remains open on a market-data account. The direct-mode channel-error resync was **live-proven on Rithmic Test 2026-08-24** (`tests/e2e/test_reconnect_live.py`, before the gateway recovery changes); the gateway recovery path is not live-verified.

**Session gateway (shared login).** Plants/session parity on the wire (incl. book unsub, `probe_time_bars`, ensure_order, `resolved_account`).

- **Direct login.** A process-wide singleton per credential fingerprint: the data factory, exec factory, and `connect_market_data_session` share one login and take the flock once; the credential flock is released when the **last** holder disconnects, so a stopped node no longer blocks a separate process. The flock is the extension's Rust `SessionLock`, the same code as the parent's.
- **Parent.** The binary reads the same env keys/precedence as the Python clients (`env_keys`: `RITHMIC_GATEWAY`>`RITHMIC_URL`, `RITHMIC_SYSTEM`>`RITHMIC_SYSTEM_NAME`) and refuses a Test/Demo system without an explicit URL (env derived from the system name, as in Python). `RITHMIC_HISTORY_READY_PROBE=0` skips the RC4 probe for accounts without market data (Rithmic Test exec smoke 2026-10-12: exposure Known/complete 10/10). Its lifetime is the operator's (`RITHMIC_GATEWAY_IDLE_EXIT_SEC` unset = until SIGTERM); start it explicitly (`rithmic-gateway` from the wheel, `python -m rithmic_gateway`, systemd, launchd, Kamal — see ops-runbook). `session_pb2.py` is regenerated reproducibly with uv-pinned protoc (`grpcio-tools==1.71.0` bundles 29.0; `scripts/gen_gateway_proto.py` restores the 5.29.6 marker; byte-identical output).
- **Attach-only clients** (`simplify-gateway-modes`). One shared socket per process; the client never spawns the parent and has no `dual` topology. With no parent it fails `parent_not_running` (names the listen path + `python -m rithmic_gateway`), and it waits a bounded `attach_timeout_sec` for a parent whose flock is held. Gateway-mode clients are credential-free: `SessionConfig.password` is optional and discarded in gateway mode, only the parent holds `RITHMIC_PASSWORD`; direct mode still requires it. Retired knobs fail fast (`CLIENT_MODE`, `SPAWN_POLICY`, `AUTO_SPAWN` → `ConfigError` for any set value; the former `mux` / `never` / `AUTO_SPAWN=0` no-ops were removed by `retire-python-mux-and-noop-knobs`). Smokes/e2e start their parent through the harness helper `scripts/_gateway_parent.py`. Evidence: the shared-consumer smoke was green on Lucid before attach-only; after it, attach to a harness-started parent is verified on Rithmic Test 2026-10-13 (tick delivery unverified: that Test account has no reference data). Native TLS remote **N/A**.
- **Unified Rust session** (`rust-unified-session`). Direct and gateway are both implementations of the Rust `Session` trait; the gateway server is generic over it and shares a table-checked RPC surface (`rpc_table.rs`). The Rust `GatewaySession` is the only adapter gateway client (the Python `gateway_wire` facade and the `RITHMIC_GATEWAY_CLIENT` selector are gone): bounded per-session queues (overflow → a typed `gap` event; the single-event `poll_*` still reads it as `ChannelLagged`), single-flight automatic re-dial with adapter-visible `TransportDown/Up/RecoveryFailed`, post-send command loss → `OutcomeUnknown` (never retried), chunked `load_time_bars`. The adapter imports neither `rithmic_gateway` nor protobuf (`tests/test_import_isolation.py`). One scenario suite passes against the Python plain client and the Rust client; the Python mux stack (`GatewayMux`, `GatewayRuntime`, `transport_recovery`) was retired by `retire-python-mux-and-noop-knobs` and `load_time_bars_range` is sequential-only. The Rust client is the default on offline + partial live evidence (attach, handshake, typed gate/Rithmic errors, recovery from a real parent restart on Rithmic Test); tick delivery and the reconnect smoke through the Rust client are NOT live-verified (Test account has no reference data) — run `scripts/smoke_gateway_shared_ticks.py` and `tests/e2e/test_reconnect_live.py` on a market-data account when one is available.
- **Execution rules in Rust** (`rust-exec-core`). The Nautilus-independent exec rules live once in `crates/rithmic-exec`: order-plant lifecycle policy (exhaustive state x event table), exposure/commission lookup, the notification-kind mapping (also used by `rithmic-plants`), fill identity and dedup keys, the instrument-id codec, the bounded recency cache, drain-row trust / freshest-row / stale-row decisions, and inventory rules. Python keeps Nautilus translation, Python-object coercion and effects. Every ported rule is compared against its original Python implementation in `tests/test_exec_rules_parity.py`; the existing exec suites run unchanged against the bindings.
- **Wheels and lake.** Prebuilt wheels, Nautilus-style (`.github/workflows/wheels.yml`, 2026-10-14): every push to `main` builds `rithmic-gateway` (maturin bin wheel: parent binary in the venv's `bin/` + the pure-Python client, `protobuf` only) and `rithmic-nt-connect` (cp312–cp314, `[gateway]` extra) for Linux x86_64/ARM64 and macOS ARM64, installs them with `--no-build` and runs the python-client scenarios against the fake parent, attests provenance, and publishes to the rolling `main-dev` prerelease plus a GitHub Pages PEP 503 index (README "Install"). No PyPI, no Windows. The first CI publish is not yet run. market-data-lake hardcodes `GatewayClient` + `load_time_bars_range` (client-side chunks) and installs the `rithmic-gateway` wheel (no Nautilus).

### Test plan

| Harness | Role |
| --- | --- |
| `cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-exec -p rithmic-nt-connect` + `pytest -q` | Unit / conversion / gateway framing |
| `scripts/smoke_lucid_nq.py` | Live MD (exit `2` if no creds) |
| `scripts/smoke_gateway_shared_ticks.py` | Two consumers (NQ+MNQ) share one `rithmic-gateway` the harness starts via `scripts/_gateway_parent.py` (exit `2` if no creds) |
| `scripts/gateway_tick_consumer.py` | Single-symbol gateway tick consumer (used by shared smoke) |
| `scripts/verify_live_vs_history.py` | Live ↔ history compare |
| `scripts/verify_order_dry_run.py` | Order plant connect / subscribe; `--live-place` with `RITHMIC_ENABLE_TRADING=1` + far `--price` (test-plant `app_name` authorized) |
| `examples/live_nq_intraday_sandbox.py` | Live Rithmic MD + Nautilus sandbox exec (no Rithmic place) |

---

## Phase marks (0–9)

### Phase 0: Define scope

- [x] Capability matrix, constraints, plants, initial NQ slice
- [x] **Exit:** this file + test plan

### Phase 1: Protocol core

- [x] Crate + PyO3; env / URL / credentials; plant login + heartbeat
- [~] Venue error / retry taxonomy (plants use `ConnectStrategy::Retry`; no command-failure classes yet)
- [x] **Exit:** compiles; LucidTrading login works
- N/A HTTP signing

### Phase 2: Instruments

- [x] Futures + front month; fail closed on missing fields; `{symbol}.RITHMIC`
- [ ] Definition updates (not advertised)
- [~] **Exit:** advertised family only

### Phase 3: Market data

- [x] Live last-trade + BBO; history request (sliced `*_all`); book summary subscribe
- [x] Book snapshot flags set; book-unsubscribe wired (plants + gateway + data client); ticker resync **live-proven on Rithmic Test 2026-08-24** (`tests/e2e/test_reconnect_live.py`; `[8] already exists` idempotent in `replay_subscription_intent`)
- [x] **Exit:** close-out 4 live-proven (Test 2026-08-24); Lucid re-host optional

### Phase 4: Execution

- [~] PnL account / positions; auto-discovery when triple unset
- [x] Gated submit / cancel / modify + notification fills
- [x] Tracked vs external reports; fill dedup; post-send unknown outcomes
- [x] **Exit:** test-plant live place confirmed 2026-08-17 (default `app_name` authorized to exchange); production `app_name` conformance still open

### Phase 5: Optional

- N/A until advertised: depth-by-order, 1-SECOND-EXTERNAL, weekly/tick bars, catalog
- [~] Live EXTERNAL time bars (1m/15m/1h/1d): subscribe + poll wired; **1m live-proven on LucidTrading 2026-08-14** and **again on Rithmic Test 2026-08-24** (`test_tc_d40_subscribe_external_bars`); 15m/1h/1d wired but no live payload (SKIP).
- `cancel_all_orders` exists on the wire; not a safe default. The NT `CancelAllOrders` / `BatchCancelOrders` commands (2026-10-12) cancel per basket from the cache (cache-scoped: untracked / in-flight orders are not reached), scoped by instrument / strategy / side — never the plant-wide wire call, which stays the gateway parent's `RITHMIC_GATEWAY_CANCEL_ALL` panic flag (`tests/test_cancel_scope.py`)

### Phase 6: Factories

- [x] Typed configs, redaction, 1.231.x `TradingNode` factories, Python package
- N/A in-tree PyO3 registry / stubs / v2 LiveNode

### Phase 7: Conformance

- [~] Unit + MD smoke + live↔history; order dry-run exercised LucidTrading 2026-08-14 (local under gitignored `artifacts/`)
- [~] Recovery suite [ ] + skipped-spec register [x] (ops-runbook "Skipped-spec register")
  - Gateway transport recovery is unit-tested (`tests/test_recovery_bridge.py`, data/exec client tests); no end-to-end recovery suite yet. Direct mode has no L3 transport recovery at all (no unix transport): it relies on the poll loops / `PlantPoller` resync only
- [~] **Exit:** not claimed until advertised close-outs have evidence

### Phase 8: Perf / robustness

- N/A until advertised hot paths are closed

### Phase 9: Docs / ops

- [x] Matrix in this file; testers default dry-run
- [~] Prebuilt wheels from CI (`wheels.yml`): gateway bin wheel + adapter wheel, Pages index; local build + no-build install smoke verified, first CI publish pending
- [~] Recovery / troubleshooting still thin ([`references/ops-runbook.md`](references/ops-runbook.md))

---

## Convention marks

Cross-cutting items from [`references/nautilus-adapter-conventions.md`](references/nautilus-adapter-conventions.md).

| Convention | Mark | Notes |
| --- | --- | --- |
| Typed configs; secrets redacted | [x] | Password never in `repr` / errors |
| Creds at construction | [x] | |
| Env / endpoint not mixed | [x] | Live / Demo / Test on `SessionConfig` |
| Distinct `InstrumentId`s | [x] | |
| One convert boundary; reject closed-set unknowns | [~] | Wire normalization is strict (`_convert.py` / `_orders.py` `enum_int`; bindable gate `order_rows.row_is_trustworthy`), but advisory status/recon builders in `order_rows.py` still fall back to MARKET / GTC / ACCEPTED / BUY for unknown closed-set values instead of rejecting |
| `ts_event` venue / `ts_init` clock | [x] | |
| Book `F_LAST` / `F_SNAPSHOT` | [x] | Snapshot envelope; last delta includes `F_LAST` |
| Subscribe intent vs confirm | [~] | Ticks/quotes tracked; book unsubscribe wired (plants/gateway/direct) |
| Request always completes | [x] | History path; Rust empty window → `[]` |
| Reconnect restores intent | [x] | Ticker resync live-proven on Rithmic Test 2026-08-24 (`test_reconnect_live.py`); gateway typed restore (ticker/book/bars/PnL/order/brackets); `[8]` duplicate-subscribe tolerated on replay |
| Partial connect teardown | [x] | Order-plant subscribe fail |
| Three outcome classes | [x] | Submit deny; modify/cancel local reject; post-send unknown |
| Tracked vs external reports | [x] | Untracked fills → `_send_fill_report` when side/px/qty present; no invented side/type/TIF status reports |
| Fill dedup by venue trade id | [x] | `fill_dedup_key` / `trade_id_from_fill_fields` |
| Never drop exec events | [~] | Parseable untracked fills reported; incomplete/untyped untracked suppressed + log |
| No empty list as “venue empty” | [~] | Cache orders; recon is a bounded `show_orders` drain — empty means “no working orders seen”, best-effort not provably complete |
| No nested `block_on` on asyncio | [x] | `asyncio.to_thread` |
| Testers default dry-run | [x] | |

---

## Close-outs (advertised path)

Do not mark Phase 3, 4, or 7 `[x]` until the matching items are proven.

1. **Incremental book updates** — [~] venue `update_type` batches mapped to Nautilus deltas (offline-tested 2026-10-14); live histogram from the e2e book test still to review on a book-entitled account; L3 MBO N/A.
2. **Execution honesty** — [~] three evidence classes; untracked → reports; recon is a bounded `show_orders` drain (best-effort, not provably complete); dedup by venue id. `cancel_all_orders` is N/A for this line's honesty claim (2026-08-22): plant-wide panic only (`RITHMIC_GATEWAY_CANCEL_ALL`), not advertised OM (TC-E41 skip).
3. **Account auto-discovery** — [x] wire resolve on ensure_order/ensure_pnl; optional env triple / `ACCOUNT_ID` selector (resolve order: bootstrap → login → account list → pick → replace handle → cache).
4. **Live-prove ticker resync** — [x] 2026-08-24 Rithmic Test (`tests/e2e/test_reconnect_live.py`); code + unit + live.
5. **Data Testing Spec (TC-D) sweep** — [x] 2026-08-14 `tests/e2e/test_data_client_live.py` (7 passed / 2 skipped; TC-D01/03/20/30/40/41/70). TC-D40 1m EXTERNAL bars live-proven on LucidTrading. Skips: TC-D10 (Lucid denies L2 book `[13] permission denied`), TC-D31 (history plant transient empty).
6. **Record LucidTrading order dry-run** — [x] 2026-08-14 local dry-run (`mode=dry_run`, `placed=false`, account auto-resolved; artifact gitignored). Test-plant live place confirmed 2026-08-17 (default `app_name` authorized; order routed to exchange). Production LucidTrading place still needs conformance `app_name`.
7. **Exec hardening (e2e TDD)** — [~] see note below.

### Close-out 7 evidence (exec hardening)

The behavior is described under **Submit / cancel / modify + fills** above; transition and row-boundary tests live in `tests/test_order_plant.py`, `tests/test_exec_transport_e2e.py` and `tests/test_working_orders_drain.py`.

- A4 (client-order-id validation) is parked on OQ1: the Rithmic `user_tag` length/format constraint is unsourced.
- P5 live canary (TC-E89) **passed** on Rithmic Test 2026-08-24: no `avg_px was None`, and at most one engine-residual `InvalidStateTrigger` after cancel (ops-runbook).
- Live data e2e re-verified 2026-08-19 on Rithmic Test (15 passed).
- MY043-001 live-log follow-ups (2026-08-21): both recurring exec-engine WARNs are reproduced against a real `LiveExecutionEngine` (`tests/test_exec_engine_repro.py`, no creds) and closed adapter-side. (1) Cache-backed status-query answers remap SUBMITTED→ACCEPTED for deferred-OPEN bracket stops (`_order_status_report_for`), because the engine's transition table has no SUBMITTED branch and fell into fill reconciliation (`report.avg_px was None`). (2) The bulk drain suppresses stale non-terminal rows for locally closed orders (`row_stale_reason` in `recon.py`, backed by `stale_reason` in `rithmic-exec`; terminal-vs-terminal is still forwarded because fill-after-cancel races are real state), closing the recon-path `InvalidStateTrigger: CANCELED -> ACCEPTED` that the notification guard does not cover. Residual engine-internal races: see ops-runbook.

---

## Paper path (intraday)

Live Rithmic MD + Nautilus sandbox exec (no Rithmic orders).

- [x] Ticker poll resyncs and replays last-trade/BBO + book intent (`resync_ticker_session`)
- [x] Book snapshot `F_SNAPSHOT`; last delta `F_SNAPSHOT | F_LAST` (empty book = `Clear` with both)
- [x] `RithmicLiveDataClientConfig` for `TradingNode` (factory loads `SessionConfig.from_env()`, `plants=market_data` so account env does not attach PnL)
- [x] Historical helper + Rust history windowing (examples no longer chunk/convert)
- [x] Shared `examples/nq_four_bar.py` (SMA20 on 1-DAY EXTERNAL / VWAP on 1-MINUTE INTERNAL + 1s 4-bar). Live: `live_nq_intraday_sandbox.py` (sandbox exec). Backtest: `backtest_nq_today.py`. Refuses `RITHMIC_ENABLE_TRADING` on paper.
- [x] README + ops point at the paper example
- [x] Unit tests: flags + resync double (`pytest`)
- [x] Live Lucid run of the sandbox example (2026-08-13: NQU6 trades + two INTERNAL 1-minute bars; clean stop) — manual smoke, no committed e2e harness (`scripts/smoke_*`)

Do not register Rithmic exec on the same node as sandbox.

---

## Related

- Agent review list: [`../AGENTS.md`](../AGENTS.md)
- Ops: [`references/ops-runbook.md`](references/ops-runbook.md)
