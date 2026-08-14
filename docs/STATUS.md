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
| Live venue `EXTERNAL` bars | 1m / 15m / 1h / 1d | **Partial** — history `request_bars` + history-plant `subscribe_time_bar_updates`. Not Lucid-proven. 1s stays INTERNAL. |
| Depth-by-order / L3 MBO | Not advertised | **N/A** until a separate slice |
| Mark / index / funding / greeks | Not advertised | **N/A** |
| Catalog / Parquet | Other repo | **N/A** |
| Order types | Market, limit, stop / stop-limit, trailing-stop (tick offset) | **Partial** — mapped and gated; live place blocked on authorized `app_name` |
| Brackets / OCO | Plant bracket API | **Partial** — `place_bracket_order` / adjust / `subscribe_bracket_updates` in `rithmic-plants` + PyO3 (2026-08-14). Lucid/Demo accept, basket-id semantics, disconnect survival **not proven**. OCO fallback not wired. Capability: `RITHMIC_BRACKETS=1` + `connect_mode=direct`. Spike: `scripts/spike_bracket_order.py` |
| Account / positions | Best-effort PnL | **Partial** — auto-discovers FCM/IB/account when unset (multi-account needs `RITHMIC_ACCOUNT_ID` selector); soft-fail PnL otherwise |
| Submit / cancel / modify + fills | Gated order plant | **Partial** — submit pre-send deny; post-send unknown; untracked → reports; fill dedup. Live place still gated on `app_name`. |
| Order status reports | Cache-backed only | **Done** (honest: not a venue snapshot) |
| Fill reports | Query unavailable | **Done** (`VenueQueryUnavailable`) |
| Reconnect + MD resubscribe | Planned | **Partial** — ticker poll resyncs last-trade/BBO + book + EXTERNAL bar intent. Gateway typed restore covers ticker/book/bars/PnL/order after plant drop. Not Lucid-proven. |
| Session gateway (shared login) | `connect_mode` required (`direct` / `gateway`) | **Partial** — plants/session parity on wire (incl. book unsub, `probe_time_bars`, ensure_order). Auto-spawn idle-exit after last client (`RITHMIC_GATEWAY_IDLE_EXIT_SEC`, default 5s via spawn; unset = never for standalone). Shared-consumer Lucid smoke green. Native TLS remote **N/A**. |
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
| `scripts/verify_order_dry_run.py` | Order plant connect / subscribe; **no** `--live-place` until `app_name` |
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
- [~] **Exit:** not claimed until live place / app_name close-out

### Phase 5: Optional

- N/A until advertised: brackets / OCO, depth-by-order, 1-SECOND-EXTERNAL, weekly/tick bars, catalog
- [~] Live EXTERNAL time bars (1m/15m/1h/1d): subscribe + poll wired; Lucid proof still open
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
| No empty list as “venue empty” | [x] | Cache orders; `VenueQueryUnavailable` fills |
| No nested `block_on` on asyncio | [x] | `asyncio.to_thread` |
| Testers default dry-run | [x] | |

---

## Close-outs (advertised path)

Do not mark Phase 3, 4, or 7 `[x]` until the matching items are proven.

1. **Incremental book updates** — summary snapshots only (L2); L3 MBO N/A.
2. **Execution honesty** — [x] three evidence classes; untracked → reports; fill query stays unavailable; dedup by venue id. (`cancel_all_orders` still out of honesty claim.)
3. **Account auto-discovery** — [x] wire resolve on ensure_order/ensure_pnl; optional env triple / `ACCOUNT_ID` selector. Plan: [`plans/2026-08-13-001-account-auto-discovery-plan.md`](plans/2026-08-13-001-account-auto-discovery-plan.md) + umbrella [`plans/2026-08-14-001-exec-honesty-account-discovery-dryrun-plan.md`](plans/2026-08-14-001-exec-honesty-account-discovery-dryrun-plan.md).
4. **Live-prove ticker resync** on LucidTrading (code + unit test landed).
5. **Record LucidTrading order dry-run** — [x] 2026-08-14 local dry-run (`mode=dry_run`, `placed=false`, account auto-resolved; artifact gitignored). Live place stays gated on authorized `app_name`.

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
