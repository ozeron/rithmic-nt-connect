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
| Order-book summary | `OrderBookDeltas` (L2) | **Partial** — `Clear` + `Add`, flags `0`; book unsubscribe is a no-op. **Close-out.** |
| History ticks / time bars | Request path, `*_all` | **Done** |
| Live venue `EXTERNAL` bars | Not advertised | **N/A** — `INTERNAL` aggregate from ticks/quotes |
| Depth-by-order / L3 MBO | Not advertised | **N/A** until a separate slice |
| Mark / index / funding / greeks | Not advertised | **N/A** |
| Catalog / Parquet | Other repo | **N/A** |
| Order types | Market, limit, stop / stop-limit, trailing-stop (tick offset) | **Partial** — mapped and gated; live place blocked on authorized `app_name` |
| Brackets / OCO | Not advertised | **N/A** |
| Account / positions | Best-effort PnL | **Partial** — works when account triple is set; soft-fail otherwise |
| Submit / cancel / modify + fills | Gated order plant | **Partial** — on `main`; post-send errors reported as reject; untracked notifications dropped. **Close-out.** |
| Order status reports | Cache-backed only | **Done** (honest: not a venue snapshot) |
| Fill reports | Query unavailable | **Done** (`VenueQueryUnavailable`) |
| Reconnect + MD resubscribe | Planned | **Not done** — exec plant resyncs; ticker does not. **Close-out.** |
| Python v2 / LiveNode | Not this support line | **N/A** |

**Protocol boundaries:** ticker, history, PnL, order plants. Public MD vs private exec.

**Test plan:**

| Harness | Role |
| --- | --- |
| `cargo test -p rithmic-nt-connect` + `pytest -q` | Unit / conversion |
| `scripts/smoke_lucid_nq.py` | Live MD (exit `2` if no creds) |
| `scripts/verify_live_vs_history.py` | Live ↔ history compare |
| `scripts/verify_order_dry_run.py` | Order plant connect / subscribe; **no** `--live-place` until `app_name` |

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

- [x] Live last-trade + BBO; history request; book summary subscribe
- [~] Book `F_LAST` / `F_SNAPSHOT`; book unsubscribe; ticker reconnect
- [~] **Exit:** not claimed until close-outs 1 and 4

### Phase 4: Execution

- [~] PnL account / positions; trading still needs account triple
- [x] Gated submit / cancel / modify + notification fills
- [ ] Tracked vs external; fill dedup; unknown outcomes
- [~] **Exit:** not claimed until close-outs 2 and 3

### Phase 5: Optional

- N/A until advertised: brackets / OCO, depth-by-order, live EXTERNAL bars, catalog
- `cancel_all_orders` exists on the wire; not a safe default

### Phase 6: Factories

- [x] Typed configs, redaction, 1.231.x `TradingNode` factories, Python package
- N/A in-tree PyO3 registry / stubs / v2 LiveNode

### Phase 7: Conformance

- [~] Unit + MD smoke + live↔history; order dry-run harness exists, Lucid run **not recorded**
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
| Book `F_LAST` / `F_SNAPSHOT` | [ ] | Close-out 1 |
| Subscribe intent vs confirm | [~] | Ticks/quotes tracked; book unsub no-op |
| Request always completes | [x] | History path |
| Reconnect restores intent | [ ] | Close-out 4 (MD) |
| Partial connect teardown | [x] | Order-plant subscribe fail |
| Three outcome classes | [ ] | Close-out 2 |
| Tracked vs external reports | [ ] | Close-out 2 |
| Fill dedup by venue trade id | [ ] | Close-out 2 |
| Never drop exec events | [ ] | Close-out 2 |
| No empty list as “venue empty” | [x] | Cache orders; `VenueQueryUnavailable` fills |
| No nested `block_on` on asyncio | [x] | `asyncio.to_thread` |
| Testers default dry-run | [x] | |

---

## Close-outs (advertised path)

Do not mark Phase 3, 4, or 7 `[x]` until the matching items are proven.

1. **Book summary** — `F_SNAPSHOT | F_LAST`; last incremental `F_LAST`; book unsubscribe or documented shared ticker unsub.
2. **Execution honesty** — three evidence classes; untracked → reports; fill query stays unavailable; dedup by venue id.
3. **Account auto-discovery** — [`plans/2026-08-13-001-account-auto-discovery-plan.md`](plans/2026-08-13-001-account-auto-discovery-plan.md).
4. **Ticker reconnect + resubscribe intent** (exec plant already resyncs).
5. **Record LucidTrading order dry-run** (`python scripts/verify_order_dry_run.py --seconds 5`, no `--live-place`). Live place stays gated on authorized `app_name`.

---

## Related

- Agent review list: [`../AGENTS.md`](../AGENTS.md)
- Ops: [`references/ops-runbook.md`](references/ops-runbook.md)
- Phase 1 product plan: [`plans/2026-08-12-001-feature-rithmic-nt-adapter-plan.md`](plans/2026-08-12-001-feature-rithmic-nt-adapter-plan.md)
