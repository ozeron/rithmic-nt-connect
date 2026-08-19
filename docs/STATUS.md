# rithmic-nt-connect — adapter status

Single source of truth for **planned scope vs done**. Not a PR log (that lives in git).

- Phases (upstream 0–9): [`references/nautilus-adapter-phases.md`](references/nautilus-adapter-phases.md)
- Conventions (how): [`references/nautilus-adapter-conventions.md`](references/nautilus-adapter-conventions.md)
- Runtime seam: [`references/nautilus-adapter-tiers.md`](references/nautilus-adapter-tiers.md)

This adapter is **out-of-tree on NautilusTrader 1.231.x**. In-tree Rust v2 build surfaces are N/A; semantic conventions still apply.

Legend: `[ ]` not started · `[~]` partial / advertised but incomplete · `[x]` done · `N/A` out of scope / not advertised

---

## Capability matrix

Acceptance: **NQ / CME via LucidTrading**. One Rithmic login (close MotiveWave / R|Trader).

| Axis | Planned | Status |
| --- | --- | --- |
| Products | Futures only | **Done** for futures (`FuturesContract` from reference + front month). Other families **N/A**. No live definition-update stream. |
| Environments | `SessionConfig.env` Live / Demo / Test | **Done**. LucidTrading + `wss://rprotocol.rithmic.com:443`. |
| Account modes | Margin + `OmsType.NETTING`; trading gated | **Partial**. `enable_trading=False` default. Trading / PnL still require FCM / IB / account triple. |
| Live trades / quotes | `TradeTick`, `QuoteTick` | **Done** |
| Order-book summary | `OrderBookDeltas` (L2) | **Partial** — snapshot flags `F_SNAPSHOT` / `F_LAST`. Book unsubscribe wired through plants/gateway/direct. |
| History ticks / time bars | Request path, `*_all` | **Done** — Rust slices (15m ticks / 4h bars), transient + empty retry, sort/dedup. Daily/weekly replay uses calendar `YYYYMMDD` indexes (Lucid 2026-08-13). Python `load_front_month_instrument` / `load_trade_ticks` / `load_time_bars`. |
| Live venue `EXTERNAL` bars | 1m / 15m / 1h / 1d | **Partial** — history `request_bars` + history-plant `subscribe_time_bar_updates`. 1m EXTERNAL subscribe **live-proven on LucidTrading 2026-08-14** (`test_TC_D40_subscribe_external_bars`). 1s stays INTERNAL. |
| Depth-by-order / L3 MBO | Not advertised | **N/A** until a separate slice |
| Mark / index / funding / greeks | Not advertised | **N/A** |
| Catalog / Parquet | Other repo | **N/A** |
| Order types | Market, limit, stop / stop-limit, trailing-stop (tick offset) | **Partial** — mapped; test-plant live place confirmed 2026-08-17 (default `app_name` authorized to exchange) |
| Brackets / OCO | Plant bracket API | **Partial** — wire on direct + gateway; Lucid accept/survival **not proven**. Live spike: `scripts/spike_bracket_order.py` (`RITHMIC_BRACKETS=1` + `RITHMIC_ENABLE_TRADING=1`; spike-only flag, not enforced in plants/gateway). |
| Account / positions | Best-effort PnL | **Partial** — auto-discovers FCM/IB/account when unset (multi-account needs `RITHMIC_ACCOUNT_ID` selector); soft-fail PnL otherwise |
| Submit / cancel / modify + fills | Gated order plant | **Partial** — submit pre-send deny; post-send unknown; untracked → reports; fill dedup. 2026-08-19 exec-hardening: overfill emits + latches (never-drop/cap); stale/duplicate TRIGGER suppressed; basket-path `is_closed` guard; venue sentinel `-1.0` → `None`; in-flight query fails closed + per-`user_tag` drain recovery (strict usable-row validation); reconnect re-arms only after a bounded drain **and** a fresh PnL/position observation (activity gate); the tag drain binds only usable rows and never regresses a live-resolved order; a dead/broken order stream (handler break / resync failure) latches or flags so the re-arm barrier cannot clear over it; unpriceable fills suppress without consuming the dedup key and latch (Oracle 2nd pass 2026-08-19). Test-plant live place confirmed 2026-08-17 (default `app_name` authorized to exchange). |
| Order status reports | Venue recon when trading; cache-backed when read-only | **Partial (best-effort)** — `load_orders` drains the current working orders via `show_orders` + a bounded silence window. Not provably complete (no end-of-list signal; replays cap at 10k), so the result is advisory, not an authoritative venue snapshot. Read-only returns cache-backed. |
| Fill reports | Venue recon when trading; read-only declines | **Partial (best-effort)** — `generate_fill_reports` filters the same bounded `load_orders` drain for filled rows; working-order replays carry no historical fills, so fills are only those seen on the live stream. Read-only declines. |

> ⚠ **Recon honesty caveat (trading):** the `load_orders` drain is best-effort and *not* provably complete — an empty result means "no working orders seen", not "venue has none". A trading consumer **must** pin `open_check_open_only=True` (the 1.231.x knob; `death_policy=trust_stop` was removed in this line) so a cached open order missing from an empty recon is advisory, not **canceled**. With `open_check_open_only=False` the engine would reconcile an empty drain as "no working orders" and cancel tracked open orders. The adapter does not enforce this; the node operator is responsible (see `docs/references/ops-runbook.md`). Live-venue proof that the drain returns the venue's working orders is still TODO.
> ⚠ **Engine in-flight UNKNOWN (Nautilus-side, 2026-08-19):** after `inflight_check_retries=5` the engine's `_resolve_inflight_order` synthesizes `OrderRejected(reason="UNKNOWN")` for a still-`SUBMITTED` order — **regardless of whether the adapter raises or returns `None`** (the retry counter increments per issued query; a raise is swallowed by the task runner). The adapter resolves the order out of `SUBMITTED` via a bounded per-`user_tag` drain and fails closed (`VenueQueryUnavailable`) whenever it cannot resolve the order, but the only reliable prevention when the venue genuinely does not know the order is the operator's `inflight_check_interval_ms=0` knob on `LiveExecEngineConfig` (via `TradingNodeConfig.exec_engine`; not on the base `ExecEngineConfig` — see `docs/references/ops-runbook.md`).
| Reconnect + MD resubscribe | Planned | **Partial** — ticker poll resyncs last-trade/BBO + book + EXTERNAL bar intent. Gateway typed restore covers ticker/book/bars/PnL/order/brackets after plant drop. Not Lucid-proven. |
| Session gateway (shared login) | `connect_mode` required (`direct` / `gateway`) | **Partial** — plants/session parity on wire (incl. book unsub, `probe_time_bars`, ensure_order, `resolved_account`). Auto-spawn idle-exit after last client (`RITHMIC_GATEWAY_IDLE_EXIT_SEC`, default 5s via spawn; unset = never for standalone). Shared-consumer Lucid smoke green. Native TLS remote **N/A**. **Wheel:** self-contained — `scripts/build_wheel.sh` bundles the `rithmic-gateway` binary + `rithmic_gateway` client into the maturin wheel; `resolve_gateway_bin` prefers `rithmic_gateway/bin/`. **Lake:** market-data-lake hardcodes `GatewayClient` + `load_time_bars_range` (client-side chunks); install `python/` package (no Nautilus). |
| Python v2 / LiveNode | Not this support line | **N/A** |

**Protocol boundaries:** ticker, history, PnL, order plants. Public MD vs private exec.

**Test plan:**

| Harness | Role |
| --- | --- |
| `cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect` + `pytest -q` | Unit / conversion / gateway framing |
| `scripts/smoke_lucid_nq.py` | Live MD (exit `2` if no creds) |
| `scripts/smoke_gateway_shared_ticks.py` | Two consumers (NQ+MNQ) share one `rithmic-gateway` (exit `2` if no creds) |
| `scripts/gateway_tick_consumer.py` | Single-symbol gateway tick consumer (used by shared smoke) |
| `scripts/verify_live_vs_history.py` | Live ↔ history compare |
| `scripts/verify_order_dry_run.py` | Order plant connect / subscribe; `--live-place` allowed with `RITHMIC_ENABLE_TRADING=1` + far `--price` (test-plant `app_name` authorized) |
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
- [x] Book snapshot flags set; book-unsubscribe wired (plants + gateway + data client); ticker resync unit-tested, not live-proven
- [~] **Exit:** not claimed until close-out 4 is live-proven

### Phase 4: Execution

- [~] PnL account / positions; auto-discovery when triple unset
- [x] Gated submit / cancel / modify + notification fills
- [x] Tracked vs external reports; fill dedup; post-send unknown outcomes
- [x] **Exit:** test-plant live place confirmed 2026-08-17 (default `app_name` authorized to exchange); production `app_name` conformance still open

### Phase 5: Optional

- N/A until advertised: depth-by-order, 1-SECOND-EXTERNAL, weekly/tick bars, catalog
- [x] Live EXTERNAL time bars (1m/15m/1h/1d): subscribe + poll wired; **1m live-proven on LucidTrading 2026-08-14** (`test_TC_D40_subscribe_external_bars`). 15m/1h/1d still need live proof.
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
| Reconnect restores intent | [~] | Ticker resync unit-tested; gateway typed restore (ticker/book/bars/PnL/order); not Lucid-proven |
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
2. **Execution honesty** — [~] three evidence classes; untracked → reports; recon is a bounded `show_orders` drain (best-effort, not provably complete); dedup by venue id. (`cancel_all_orders` still out of honesty claim.)
3. **Account auto-discovery** — [x] wire resolve on ensure_order/ensure_pnl; optional env triple / `ACCOUNT_ID` selector. Plan: [`plans/2026-08-13-001-account-auto-discovery-plan.md`](plans/2026-08-13-001-account-auto-discovery-plan.md) + umbrella [`plans/2026-08-14-001-exec-honesty-account-discovery-dryrun-plan.md`](plans/2026-08-14-001-exec-honesty-account-discovery-dryrun-plan.md).
4. **Live-prove ticker resync** on LucidTrading (code + unit test landed).
4. **Data Testing Spec (TC-D) sweep** — [x] 2026-08-14 `tests/e2e/test_data_client_live.py` (7 passed / 2 skipped; TC-D01/03/20/30/40/41/70). TC-D40 1m EXTERNAL bars live-proven on LucidTrading. Skips: TC-D10 (Lucid denies L2 book `[13] permission denied`), TC-D31 (history plant transient empty).
5. **Record LucidTrading order dry-run** — [x] 2026-08-14 local dry-run (`mode=dry_run`, `placed=false`, account auto-resolved; artifact gitignored). Test-plant live place confirmed 2026-08-17 (default `app_name` authorized; order routed to exchange). Production LucidTrading place still needs conformance `app_name`.
6. **Exec hardening (e2e TDD)** — [~] 2026-08-19 P1 (in-flight fail-closed + per-tag drain + drain-gated re-arm), P2 (A2 overfill, A1 stale trigger, A5 basket guard, A3 sentinel), P3 (race/recovery), P4 (gateway-facade restore) landed with red→green transport-e2e tests. Oracle 2nd-pass fixes landed: latch-generation guard, PnL-snapshot re-arm gate, validate-then-bind + stale-row re-read in the tag drain, venue-row `ts_event` in recovered reports. Oracle merge items landed: stream-failure latch, strict report-builder validation, PnL-marker placement + activity-gate doc, unpriceable-fill latch. Oracle simplification pass landed: latch-generation counter + stream-failed flag collapsed into plant state (re-arm keyed on `CONNECTING` + poll-task liveness), unresolved in-flight queries always fail closed (no `None`), per-tag drain cooldown removed, `-1.0 → None` centralized at the convert boundary (fill classification decoupled from priceability), PnL gate is an `asyncio.Event`, and the drain retry loop dropped to one bounded attempt. A4 (client-order-id validation) parked on OQ1 (Rithmic `user_tag` constraint unsourced). P5 live canary on the test account not yet run.

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
