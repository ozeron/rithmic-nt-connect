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
| **Capability matrix** | 21 | 4 | 11 | 0 | 6 |
| **Phase marks (0–9)** | 27 | 15 | 6 | 2 | 4 |
| **Convention marks** | 18 | 15 | 3 | 0 | 0 |
| **Close-outs (advertised path)** | 8 | 5 | 2 | 0 | 1 |
| **Paper path (intraday)** | 8 | 8 | 0 | 0 | 0 |
| **TOTAL** | 82 | 47 | 22 | 2 | 11 |

**Implemented:** 82% (`47` done + `22` partial of `71` in-scope marked items (`11` N/A excluded); partial counts as half).

Not started (real leftover work): Phase 2 definition updates, Phase 7 recovery suite.

---

## Capability matrix

| Axis | Planned | Status |
| --- | --- | --- |
| Products | Futures only | [x] futures (`FuturesContract` from reference + front month). Other families **N/A**. No live definition-update stream. |
| Environments | `SessionConfig.env` Live / Demo / Test | [x] LucidTrading + `wss://rprotocol.rithmic.com:443`. |
| Account modes | Margin + `OmsType.NETTING`; trading gated | [~] `enable_trading=False` default. Trading / PnL need FCM / IB / account triple. |
| Live trades / quotes | `TradeTick`, `QuoteTick` | [x] |
| Order-book summary | `OrderBookDeltas` (L2) | [~] snapshot flags `F_SNAPSHOT` / `F_LAST`. Book unsubscribe wired plants/gateway/direct. |
| History ticks / time bars | Request path, `*_all` | [x] see notes |
| Live venue `EXTERNAL` bars | 1m / 15m / 1h / 1d | [~] see notes |
| Depth-by-order / L3 MBO | Not advertised | **N/A** until a separate slice |
| Mark / index / funding / greeks | Not advertised | **N/A** |
| Catalog / Parquet | Other repo | **N/A** |
| Order types | Market, limit, stop / stop-limit, trailing-stop (tick offset) | [~] mapped; test-plant live place confirmed 2026-08-17 (default `app_name` authorized to exchange) |
| Brackets / OCO | Plant bracket API | [~] wire on direct + gateway; Test accept+survival proven both modes 2026-08-24 (`scripts/spike_bracket_order.py` far LIMIT from BBO, plant redial, identity cancel). Spike-only `RITHMIC_BRACKETS=1` + `RITHMIC_ENABLE_TRADING=1`. |
| Account / positions | Best-effort PnL | [~] auto-discovers FCM/IB/account when unset (multi-account: `RITHMIC_ACCOUNT_ID`); soft-fail PnL otherwise |
| Submit / cancel / modify + fills | Gated order plant | [~] see notes |
| Order status reports | Venue recon when trading; cache-backed when read-only | [~] (best-effort) — `load_orders` = `show_orders` + bounded silence. Not provably complete (no end-of-list; 10k replay cap). Advisory, not authoritative. Read-only → cache-backed. **TC-E88 drain-by-identity live-proven on Test 2026-08-24.** |
| Fill reports | Venue recon when trading; read-only declines | [~] (best-effort) — `generate_fill_reports` filters the same drain for filled rows. Working-order replays carry no historical fills; fills are live-stream only. Read-only declines. |
| Reconnect + MD resubscribe | Planned | [~] see notes |
| Session gateway (shared login) | `connect_mode` required (`direct` / `gateway`) | [~] see notes |
| Python v2 / LiveNode | Not this support line | **N/A** |

**Protocol boundaries:** ticker, history, PnL, order plants. Public MD vs private exec.

### Capability notes (evidence)

**History ticks / time bars.** Rust slices (15m ticks / 4h bars), transient + empty retry, sort/dedup. Daily/weekly replay uses calendar `YYYYMMDD` indexes (Lucid 2026-08-13). Python: `load_front_month_instrument` / `load_trade_ticks` / `load_time_bars`.

**Live venue `EXTERNAL` bars.** History `request_bars` + history-plant `subscribe_time_bar_updates`. 1m EXTERNAL subscribe **live-proven on LucidTrading 2026-08-14** and **again on Rithmic Test 2026-08-24** (`test_tc_d40_subscribe_external_bars`). 1s stays INTERNAL. Node-path dispatch fixed 2026-08-19: venue echoes `time_bar` event `period` in seconds (`"60"`) while the dispatch key was registered with native period (`1`), so node-path EXTERNAL bars were dropped as `time_bar_unsubscribed` — keys now register under both units (unit-tested). INTERNAL-vs-EXTERNAL seam pinned by `tests/test_bar_seam_parity.py`. Live full-node parity e2e (TC-D54) dropped: under some Test off-hours / prior plant states the ticker side can go quiet (stale synthetic snapshots) while bars still arrive, so INTERNAL==EXTERNAL parity is not a reliable Test host — bar delivery itself remains proven by TC-D40; ticker+book incremental resume is separately proven by `test_reconnect_live.py` (Test 2026-08-24).

**Submit / cancel / modify + fills.** Pre-send deny; post-send unknown; untracked → reports; fill dedup. **2026-08-19 exec-hardening:** overfill emits + latches (never-drop/cap); stale/duplicate TRIGGER suppressed; basket-path `is_closed` guard; venue sentinel `-1.0` → `None`; in-flight query fails closed + per-`user_tag` drain recovery (strict usable-row validation); reconnect re-arms only after a bounded drain **and** a fresh PnL/position observation (activity gate; skipped when PnL soft-fails — plant re-arms on drain alone with blind position context, see ops-runbook); tag drain binds only usable rows and never regresses a live-resolved order; dead/broken order stream (handler break / resync failure) latches or flags so the re-arm barrier cannot clear over it; unpriceable fills suppress without consuming the dedup key and latch; plant lifecycle is an explicit state machine (`LATCHED` real state, `rearm` the only arming transition, down plant cannot re-arm via resync, resync during barrier latches — transition table in `tests/test_order_plant.py`); drain-row interpretation is one boundary (`_drain_row_from_fields`; build-then-bind, publish-before-bind; barrier aborts on publish failure). **Oracle 4th pass:** strict recovery recognizes `triggered` rows (TRIGGERED for limit-style stops); modify/cancel-rejected report as working (never terminal REJECTED); per-tag recovery answers with newest row; persistent transient order-stream errors latch — streak is loop-local (per stream lifetime: fresh poll loop or successful resubscribe starts at zero); re-arm drain skips closed tracked orders; malformed `ts_event`/boolean closed-set rows never abort or bind. **Root-cause refactor (Macroscope round 3):** transient streak is loop-local (no client attribute across lifetimes); raw-row drain pipeline is ONE owned iterator (`_iter_drain_rows` — normalize/basket/ts guards inside; four loops consume it); drain identity is `_drain_client_order_id` (cache-first, closed-order guards); mutation-corpus property test: "bindable ⟹ real closed-set terms". **Coercion-as-validation (round 3):** `price_type`/`duration` normalize to exact int or `None` at convert boundary (`enum_int` — bools/non-integral numerics never survive); `_row_is_trustworthy` collapses to same whitelist (presence + known-value only); report builders own event-time timestamp fallback (no consumer publishes epoch-0 status/fill); convert-boundary property tests pin the whitelist (Oracle 2nd pass + root-cause refactor 2026-08-19). **Macroscope round 4 (2026-08-19):** timestamp-less drain row never skipped by freshness rule (0 = synthetic missing-ts — snapshot still publishes and binds); in-flight query gate uses effective venue-id lookup (order with venue id on model answers directly, no drain); venue fill without `fill_id`/timestamp dedupes to stable `TradeId` (clock fallback only in report timestamp, never identity). **Commission (2026-08-19):** fills carry venue RMS commission — per-product `commission_fill_rate` (e.g. MNQ 0.5) × fill qty with account `default_commission` fallback; fetched at connect on both paths (direct PyO3 + gateway RPC `fetch_product_rms_info` / `fetch_account_rms_info`, gated on parent trading like `load_orders`); fetch failure non-fatal (zero fallback). Live rates verified per account by `tests/e2e/test_rms_commission_live.py` (direct + gateway round-trip, no live-value expectations). Test-plant live place confirmed 2026-08-17 (default `app_name` authorized to exchange).

**Recon honesty caveat (trading).** `load_orders` drain is best-effort and *not* provably complete — empty means "no working orders seen", not "venue has none". Trading consumers **must** pin `open_check_open_only=True` (1.231.x knob; `death_policy=trust_stop` removed in this line) so a cached open order missing from an empty recon is advisory, not **canceled**. With `open_check_open_only=False` the engine would treat an empty drain as "no working orders" and cancel tracked opens. Adapter does not enforce this; node operator owns it (see `docs/references/ops-runbook.md`).     Live-venue drain-by-identity proof: **TC-E88 PASSED on Rithmic Test 2026-08-24** (`test_tc_e88_drain_reports_working_limit_by_identity`).

**Engine in-flight UNKNOWN (Nautilus-side, 2026-08-19).** After `inflight_check_retries=5`, engine `_resolve_inflight_order` synthesizes `OrderRejected(reason="UNKNOWN")` for a still-`SUBMITTED` order — **regardless of whether the adapter raises or returns `None`** (retry counter increments per issued query; raise swallowed by task runner). Adapter resolves out of `SUBMITTED` via bounded per-`user_tag` drain and fails closed (`VenueQueryUnavailable`) when it cannot resolve. Only reliable prevention when the venue genuinely does not know the order: operator `inflight_check_interval_ms=0` on `LiveExecEngineConfig` (via `TradingNodeConfig.exec_engine`; not on base `ExecEngineConfig` — see `docs/references/ops-runbook.md`).

**Reconnect + MD resubscribe.** Every path that re-establishes the wire re-issues ticker + book + EXTERNAL bar intent through one replay boundary (`replay_subscription_intent`): channel-error resync (`_resync_ticker_subscription` → `reset_ticker` + replay) **and** client lifecycle (`_connect` re-issues remembered intent, clears one-sided BBO accumulators, restarts history poll when bars registered — disconnect→connect previously left live plants with zero subscriptions). Duplicate venue subscribe (`[8] already exists`) is treated as success on replay (history-plant bars survive ticker reset). Gateway typed restore covers ticker/book/bars/PnL/order/brackets after plant drop. Data resync is scoped: direct resets **only** the ticker plant (`reset_ticker_plant`; PnL/order untouched); gateway re-dials only that client via `ResetTickerPlantRequest` (wired both paths, framing-tested). **Live-proven on Rithmic Test 2026-08-24** (`tests/e2e/test_reconnect_live.py`).

**Session gateway (shared login).** Plants/session parity on wire (incl. book unsub, `probe_time_bars`, ensure_order, `resolved_account`). Direct is a process-wide singleton per credential fingerprint (data factory, exec factory, and `connect_market_data_session` share one login; flock taken once); credential flock released when the **last** holder disconnects, so a stopped node no longer blocks a separate process. `session_pb2.py` regenerated reproducibly with uv-pinned protoc (`grpcio-tools==1.71.0` bundles 29.0; `scripts/gen_gateway_proto.py` restores the 5.29.6 marker; byte-identical output). Auto-spawn idle-exit after last client (`RITHMIC_GATEWAY_IDLE_EXIT_SEC`, default 5s via spawn; unset = never for standalone). Shared-consumer Lucid smoke green. Native TLS remote **N/A**. **Wheel:** self-contained — `scripts/build_wheel.sh` bundles `rithmic-gateway` binary + `rithmic_gateway` client into the maturin wheel; `resolve_gateway_bin` prefers `rithmic_gateway/bin/`. **Lake:** market-data-lake hardcodes `GatewayClient` + `load_time_bars_range` (client-side chunks); install `python/` package (no Nautilus).

### Test plan

| Harness | Role |
| --- | --- |
| `cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect` + `pytest -q` | Unit / conversion / gateway framing |
| `scripts/smoke_lucid_nq.py` | Live MD (exit `2` if no creds) |
| `scripts/smoke_gateway_shared_ticks.py` | Two consumers (NQ+MNQ) share one `rithmic-gateway` (exit `2` if no creds) |
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
- [x] Live EXTERNAL time bars (1m/15m/1h/1d): subscribe + poll wired; **1m live-proven on LucidTrading 2026-08-14** and **again on Rithmic Test 2026-08-24** (`test_tc_d40_subscribe_external_bars`). 15m/1h/1d still need live proof (Test skips into register; Lucid preferred).
- `cancel_all_orders` exists on the wire; not a safe default

### Phase 6: Factories

- [x] Typed configs, redaction, 1.231.x `TradingNode` factories, Python package
- N/A in-tree PyO3 registry / stubs / v2 LiveNode

### Phase 7: Conformance

- [~] Unit + MD smoke + live↔history; order dry-run exercised LucidTrading 2026-08-14 (local under gitignored `artifacts/`)
- [ ] Recovery suite; skipped-spec register
- [~] **Exit:** not claimed until advertised close-outs have evidence

### Phase 8: Perf / robustness

- N/A until advertised hot paths are closed

### Phase 9: Docs / ops

- [x] Matrix in this file; testers default dry-run
- [x] Self-contained wheel (`scripts/build_wheel.sh`) bundles `rithmic-gateway` binary + client
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
| One convert boundary; reject closed-set unknowns | [x] | `_convert.py` / `_orders.py` |
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

1. **Incremental book updates** — summary snapshots only (L2); L3 MBO N/A.
2. **Execution honesty** — [~] three evidence classes; untracked → reports; recon is a bounded `show_orders` drain (best-effort, not provably complete); dedup by venue id. `cancel_all_orders` is **decided N/A for this support line's honesty claim** (2026-08-22, gap-closure plan P1.3): it is plant-wide by venue contract, kept only as the parent panic button (`RITHMIC_GATEWAY_CANCEL_ALL`), never adapter-advertised and never live-tested as an order-management primitive (TC-E41 collection-skip stands).
3. **Account auto-discovery** — [x] wire resolve on ensure_order/ensure_pnl; optional env triple / `ACCOUNT_ID` selector. Plan: [`plans/2026-08-13-001-account-auto-discovery-plan.md`](plans/2026-08-13-001-account-auto-discovery-plan.md) + umbrella [`plans/2026-08-14-001-exec-honesty-account-discovery-dryrun-plan.md`](plans/2026-08-14-001-exec-honesty-account-discovery-dryrun-plan.md).
4. **Live-prove ticker resync** — [x] 2026-08-24 Rithmic Test (`tests/e2e/test_reconnect_live.py`); code + unit + live.
4. **Data Testing Spec (TC-D) sweep** — [x] 2026-08-14 `tests/e2e/test_data_client_live.py` (7 passed / 2 skipped; TC-D01/03/20/30/40/41/70). TC-D40 1m EXTERNAL bars live-proven on LucidTrading. Skips: TC-D10 (Lucid denies L2 book `[13] permission denied`), TC-D31 (history plant transient empty).
5. **Record LucidTrading order dry-run** — [x] 2026-08-14 local dry-run (`mode=dry_run`, `placed=false`, account auto-resolved; artifact gitignored). Test-plant live place confirmed 2026-08-17 (default `app_name` authorized; order routed to exchange). Production LucidTrading place still needs conformance `app_name`.
6. **Exec hardening (e2e TDD)** — [~] see note below.

### Close-out 6 evidence (exec hardening)

2026-08-19 P1 (in-flight fail-closed + per-tag drain + drain-gated re-arm), P2 (A2 overfill, A1 stale trigger, A5 basket guard, A3 sentinel), P3 (race/recovery), P4 (gateway-facade restore) landed with red→green transport-e2e tests.

Oracle 2nd-pass: latch-generation guard, PnL-snapshot re-arm gate, validate-then-bind + stale-row re-read in tag drain, venue-row `ts_event` in recovered reports. Oracle merge: stream-failure latch, strict report-builder validation, PnL-marker placement + activity-gate doc, unpriceable-fill latch. Oracle simplification: latch-generation counter + stream-failed flag collapsed into plant state (re-arm keyed on `CONNECTING` + poll-task liveness); unresolved in-flight queries always fail closed (no `None`); per-tag drain cooldown removed; `-1.0 → None` centralized at convert boundary (fill classification decoupled from priceability); PnL gate is an `asyncio.Event`; drain retry loop dropped to one bounded attempt.

Root-cause refactor (Macroscope round 2 + Oracle): plant lifecycle is explicit state machine (`OrderPlantPolicy` — `LATCHED` a real state, owned transitions, `rearm` only arming transition; `DISCONNECTED` cannot re-arm via resync; mid-barrier resync latches); drain-row interpretation is one boundary (`_drain_row_from_fields`, build-then-bind + publish-before-bind); transition table + row-boundary property tests in `tests/test_order_plant.py`.

Oracle 4th pass (2026-08-19): `triggered` strict rows bind and report TRIGGERED for limit-style stops; modify/cancel-rejected report as working (never terminal REJECTED); per-tag recovery picks newest matching row; drain publish failure aborts re-arm barrier; stale drain rows skip when live stream advanced the order; persistent transient order-stream errors latch; `rearm` requires a real poll task.

Root-cause refactor round 2 (Macroscope round 3): transient streak became loop-local (deleted client attribute — fresh loop/resubscribe starts at zero structurally); raw-row drain pipeline collapsed into one owned iterator (`_iter_drain_rows`; four drain loops are consumers); drain identity is `_drain_client_order_id`; mutation-corpus property test pins "bindable ⟹ real closed-set terms".

A4 (client-order-id validation) parked on OQ1 (Rithmic `user_tag` constraint unsourced). **P5 live canary (TC-E89) PASSED on Rithmic Test 2026-08-24** — no `avg_px was None`; ≤1 engine-residual `InvalidStateTrigger` after cancel (ops-runbook). Live data e2e re-verified 2026-08-19 (15 passed on Rithmic Test) after fixing two pre-existing `main` regressions from the 8862b62 tooling commit: flocked direct session forwarded a `timeout_ms` arg the no-arg PyO3 `poll_event` rejects; `LiveDataClient` dual-base MRO broke `RithmicDataClient.__init__` (`TradingNode.build` raised missing-`loop`); single base restored and `LiveDataClient` factory contract satisfied by vendored stub.

MY043-001 live-log follow-ups (2026-08-21): both recurring exec-engine WARNs reproduced against a real `LiveExecutionEngine` (`tests/test_exec_engine_repro.py`, no creds) and closed adapter-side. (1) Cache-backed status-query answers remap SUBMITTED→ACCEPTED for deferred-OPEN bracket stops (`_order_status_report_for`) — the engine's transition table has no SUBMITTED branch and fell into fill reconciliation warning `report.avg_px was None`. (2) The bulk drain suppresses stale non-terminal rows for locally closed orders (`_row_regresses_terminal_order`; terminal-vs-terminal still forwarded — fill-after-cancel races are real state), closing the recon-path `InvalidStateTrigger: CANCELED -> ACCEPTED` that the LAP-42 notification guard (#27) does not cover. Runbook note added for residual engine-internal races.

---

## Paper path (intraday)

Live Rithmic MD + Nautilus sandbox exec. Plan: [`plans/2026-08-13-002-intraday-sandbox-paper-plan.md`](plans/2026-08-13-002-intraday-sandbox-paper-plan.md).

- [x] Ticker poll resyncs and replays last-trade/BBO + book intent (`resync_ticker_session`)
- [x] Book snapshot `F_SNAPSHOT`; last delta `F_SNAPSHOT | F_LAST` (empty book = `Clear` with both)
- [x] `RithmicLiveDataClientConfig` for `TradingNode` (factory loads `SessionConfig.from_env()`, `plants=market_data` so account env does not attach PnL)
- [x] Historical helper + Rust history windowing (examples no longer chunk/convert)
- [x] Shared `examples/nq_four_bar.py` (SMA20 on 1-DAY EXTERNAL / VWAP on 1-MINUTE INTERNAL + 1s 4-bar). Live: `live_nq_intraday_sandbox.py` (sandbox exec). Backtest: `backtest_nq_today.py`. Refuses `RITHMIC_ENABLE_TRADING` on paper.
- [x] README + ops point at the paper example
- [x] Unit tests: flags + resync double (`pytest`)
- [x] Live Lucid run of the sandbox example (2026-08-13: NQU6 trades + two INTERNAL 1-minute bars; clean stop)

Do not register Rithmic exec on the same node as sandbox.

---

## Related

- Agent review list: [`../AGENTS.md`](../AGENTS.md)
- Ops: [`references/ops-runbook.md`](references/ops-runbook.md)
- Phase 1 product plan: [`plans/2026-08-12-001-feature-rithmic-nt-adapter-plan.md`](plans/2026-08-12-001-feature-rithmic-nt-adapter-plan.md)
