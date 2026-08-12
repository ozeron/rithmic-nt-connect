---
title: "Rithmic NT Adapter - Plan"
date: 2026-08-12
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
product_contract_source: ce-brainstorm
execution: code
---

# Rithmic NT Adapter - Plan

## Goal Capsule

- **Objective:** Ship an out-of-tree Rithmic R|Protocol adapter that lets a Python strategy on NautilusTrader 1.231.x consume live/historical market data (and best-effort account state) through a Rust client core.
- **Product authority:** This plan owns Phase 1 (market data + instruments + best-effort account/positions). Phase 2 order routing is related work, not active scope.
- **Authority hierarchy:** Product Contract (Rs/AEs) > Planning Contract (KTDs) > Implementation Units > ad-hoc implementer choice.
- **Stop conditions:** Stop Phase 1 if LucidTrading MD login fails after quirk checklist; stop claiming Nautilus account integration if read-only exec client cannot call `generate_account_state` on 1.231.x — fall back is out of scope without amending R7.
- **Execution profile:** Test-first for mapping/parsers; smoke-first for live LucidTrading acceptance (credentials-gated).
- **Open blockers:** None for Phase 1 implementation.

## Product Contract

### Summary

Build **rithmic-connect**: a separate External adapter repo with a Rust Rithmic client and Python Nautilus factories so Python strategies run on stable NautilusTrader 1.231.x with a fast Rust wire path. Phase 1 covers configurable symbols/exchanges (NQ LucidTrading acceptance), full MD plant capabilities when available, and best-effort account/positions via a **read-only execution client**. No order routing in Phase 1.

### Problem Frame

Official Nautilus inclusion was rejected ([#3768](https://github.com/nautechsystems/nautilus_trader/issues/3768)) for gated API/conformance reasons. LucidTrading R|Protocol access is already verified for market data and PnL in the MY046 harness. The missing piece is a durable, Rust-backed Nautilus adapter outside the core monorepo.

### Key Decisions

- **Separate repo, not a Nautilus fork** `(session-settled: user-directed — chosen over full fork: upstream rejected official inclusion; avoids rebase tax)` — Governs R1, R2
- **Target NautilusTrader 1.231.x Python TradingNode** `(session-settled: user-directed — chosen over Python v2 develop: v2 lacks out-of-tree adapter seam today)` — Governs R2, R8
- **Rust-first wire client; Python/Node clients are quirk references only** `(session-settled: user-directed — chosen over Python-spike/dual-backend: one production client, speed with Rust core)` — Governs R3, R4
- **Phase 1 = MD + instruments + best-effort account/positions; Phase 2 = orders** `(session-settled: user-directed — chosen over all-or-nothing: LucidTrading MD/PnL verified; orders deferred)` — Governs R5, R6, R7, R11
- **Any configured symbol/exchange; NQ LucidTrading is the acceptance test** `(session-settled: user-directed — chosen over NQ-only MVP)` — Governs R5, R10
- **Full MD plant slice when exposed (live + history + depth/DOM)** `(session-settled: user-directed)` — Governs R6
- **Trademark-safe name `rithmic-connect`** — Governs R1
- **Account path uses read-only LiveExecutionClient** `(session-settled: user-approved — requirements check RQ-1: data client cannot publish AccountState on 1.231.x)` — Governs R2, R7, R9

<!-- ce-section: work-relationships -->
### How This Work Fits Together

This plan owns **Phase 1 adapter delivery** in `rithmic-connect`. Broader understanding (not committed roadmap):

- **Phase 2 order routing** — Depends on Phase 1 client/plants + order authorization / possible conformance `app_name`; extends the read-only exec client into a full exec client
- **Optional Community listing** on Nautilus `ADAPTERS.md` — Can proceed independently after Phase 1 is documented and licensed
- **MY046 MotiveWave research harness** — Shares LucidTrading credentials/quirks; Can proceed independently as a consumer/test oracle

### Actors

- A1. Adapter maintainer — builds and operates the External adapter
- A2. Python strategy developer — writes strategies against Nautilus 1.231.x using this adapter
- A3. Rithmic / LucidTrading plants — ticker, history, pnl; order plant must not receive trading commands in Phase 1
- A4. NautilusTrader 1.231.x TradingNode — hosts data and read-only exec clients

### Requirements

**Packaging and positioning**

- R1. The adapter lives in its own repository (`rithmic-connect`) as an External Nautilus-compatible project with required trademark disclaimers; it is not submitted for official in-tree inclusion.
- R2. Phase 1 integrates with NautilusTrader **1.231.x** via out-of-tree factories registerable on `TradingNode`: a data-client factory always, and an execution-client factory when account/position publishing is enabled.

**Rust client and Python surface**

- R3. The production wire protocol client is implemented in Rust (building on `rithmic-rs` unless a blocking gap appears), with plant quirks informed by MY046 `async_rithmic` and Node client behavior.
- R4. Rust functionality needed by the Python adapter is exposed through a PyO3 extension consumed by the Python package.
- R8. A Python strategy can run live against Rithmic market data without modifying the NautilusTrader core repository.

**Phase 1 market data**

- R5. Users can configure arbitrary Rithmic symbol/exchange pairs; instrument loading resolves definitions needed to subscribe and request history.
- R6. When the connected plants expose them, Phase 1 supports live LastTrade+BBO, historical ticks/bars, and market depth (OrderBook summary required when entitled; depth-by-order when entitled).
- R10. Acceptance for Phase 1 is demonstrated on **NQ / CME via LucidTrading** (front-month resolution + live data path).

**Phase 1 account (best-effort)**

- R7. When the PnL plant is available, a **read-only** execution client surfaces account and position updates into Nautilus account/position pathways on a best-effort basis; absence or denial must soft-fail without breaking the MD path.

**Safety and non-goals encoded as requirements**

- R9. Phase 1 must not submit, modify, or cancel orders (even if an order plant connection exists internally for account listing helpers).
- R11. Single-login session conflicts are documented; users must not run MotiveWave/R|Trader concurrently with the adapter session.
- R12. Gated Rithmic protobuf sources and certificates are not committed to the public repository unless redistribution is clearly permitted.

### Key Flows

- F1. Connect and discover
  - **Trigger:** TradingNode starts with Rithmic data (and optional read-only exec) config.
  - **Actors:** A2, A3, A4
  - **Steps:** Load credentials/system/gateway; connect ticker/history/pnl plants; resolve configured instruments (front month when requested); optionally snapshot accounts via PnL/order-plant list helpers without placing orders.
  - **Outcome:** MD path ready; account path up or soft-failed.
  - **Covered by:** R3, R5, R7, R9

- F2. Live market data
  - **Trigger:** Strategy subscribes to trades/quotes/depth for a configured instrument.
  - **Actors:** A2, A3, A4
  - **Steps:** Rust client subscribes on ticker plant; maps venue messages; Python data client publishes into the node.
  - **Outcome:** Strategy receives live updates.
  - **Covered by:** R4, R6, R8, R10

- F3. Historical request
  - **Trigger:** Strategy or loader requests historical ticks/bars for a listed contract window.
  - **Actors:** A2, A3, A4
  - **Steps:** History plant request (`*_all` loaders to avoid silent 10k caps); map responses; return as Nautilus expects.
  - **Outcome:** Finite history window returned for listed contracts; empty expired months handled without crash.
  - **Covered by:** R6

- F4. Account/position snapshot (best-effort)
  - **Trigger:** Read-only exec client connects with PnL plant available.
  - **Actors:** A3, A4
  - **Steps:** Subscribe/snapshot PnL; call Nautilus `generate_account_state` / position status reports; continue if plant denies.
  - **Outcome:** Account balance/positions visible when permitted; MD unaffected otherwise.
  - **Covered by:** R7, R9

### Acceptance Examples

- AE1. NQ LucidTrading live path
  - **Covers:** R5, R6, R10
  - **Given:** Valid LucidTrading credentials and MotiveWave closed
  - **When:** Node starts and subscribes to front-month NQ/CME
  - **Then:** Front month resolves and live trade/quote updates flow into the strategy within the smoke window

- AE2. Configurable non-NQ symbol
  - **Covers:** R5
  - **Given:** Config sets a second entitled symbol/exchange
  - **When:** Instrument load + subscribe runs
  - **Then:** Adapter attempts the configured pair without NQ-hardcoding; entitlement failures are explicit errors

- AE3. PnL best-effort via read-only exec client
  - **Covers:** R7, R9
  - **Given:** PnL plant connects (as in 2026-08-12 probe)
  - **When:** Read-only exec client starts
  - **Then:** Account and/or position fields appear on the node account pathway; no order commands are sent; if PnL is denied, MD remains healthy

- AE4. No order side effects
  - **Covers:** R9
  - **Given:** Order plant APIs exist in dependencies
  - **When:** Phase 1 smoke suite completes
  - **Then:** No order submit/modify/cancel requests are sent

### Success Criteria

- S1. Phase 1 smoke: connect → resolve NQ front → receive live updates on LucidTrading without a Nautilus fork.
- S2. Historical request for a short listed-contract window returns data or a clear empty/entitlement result (no hang).
- S3. Account/position path works via read-only exec client on LucidTrading or degrades cleanly.
- S4. README states External/unofficial status and session/conformance caveats.

### Scope Boundaries

**In scope (Phase 1)**

- Separate repo packaging; Rust client + PyO3 + Python data client + read-only exec client + factories/config
- Instruments + live MD + history + depth when available
- Best-effort account/positions
- LucidTrading NQ acceptance harness

**Deferred (Phase 2 / later)**

- Order submit, modify, cancel, brackets, fills, full execution client
- Conformance-issued `app_name` acquisition workflow
- Python v2 out-of-tree / in-tree fork packaging
- Community listing request on Nautilus `ADAPTERS.md`
- Deep historical archive for expired contracts

**Non-goals**

- Merging into official `nautechsystems/nautilus_trader`
- Replacing MotiveWave as a charting UI
- Guaranteeing prop-firm risk rules beyond Rithmic/LucidTrading

### Dependencies / Assumptions

- D1. Operator has LucidTrading (or equivalent) R|Protocol credentials with MD entitlements.
- D2. `rithmic-rs` covers ticker/history/pnl plants needed for Phase 1 (confirmed in planning research).
- D3. NautilusTrader 1.231.x remains the Phase 1 support line for out-of-tree Python adapters.
- A1. Gated protos stay inside `rithmic-rs` published generated code; this repo does not commit `.proto` sources.
- A2. Smoke `app_name` remains sufficient for Phase 1 MD/PnL on LucidTrading (verified 2026-08-12).
- A3. LucidTrading is selected via system-name override on the Live (or appropriate) env profile, not `rithmic-rs` defaults.

### Outstanding Questions

**Deferred to implementation**

- Q2. Prefer `OrderBookDeltas` vs `OrderBookDepth10` for OrderBook summary mapping — choose during U5 against sample payloads.
- Q3. Final `nautilus_trader` pin (`==1.231.0` vs compatible range) after first green build.
- Q5. Whether optional live smoke runs in CI via repository secrets or stays maintainer-local.

**Resolved in planning**

- Q1. Use `rithmic-rs` 3.x as wire foundation (wrap; do not vendor protos).
- Q4. Account/positions require read-only `LiveExecutionClient` on 1.231.x (RQ-1).

### Sources / Research

- `docs/references/upstream-rfc-3768.md`
- `docs/references/nautilus-adapter-tiers.md`
- `docs/references/my046-rithmic-access.md`
- `docs/references/plant-probe-2026-08-12.md`
- `docs/references/wire-clients.md`
- `docs/references/requirements-check-2026-08-12.md`
- https://github.com/pbeets/rithmic-rs
- https://docs.rs/rithmic-rs/latest/rithmic_rs/
- Nautilus local patterns: `nautilus_trader/live/node.py`, `live/data_client.py`, `adapters/tardis/`, `adapters/_template/`

## Planning Contract

### Key Technical Decisions

- KTD1. **Wire layer = `rithmic-rs` ^3** — wrap plants behind a small `rithmic-connect` Rust facade; do not fork unless a Phase 1 gap appears. Governs U2.
- KTD2. **Read-only exec client for R7** — register `LiveExecutionClient` that only publishes account/position state; all order methods no-op or raise “Phase 1 read-only”. Governs U6. (RQ-1)
- KTD3. **Out-of-tree type boundary** — Rust emits structured venue DTOs via PyO3; Python constructs `nautilus_trader` Cython domain objects (`TradeTick`, `QuoteTick`, instruments, `AccountState`) for engine handoff. Avoid linking Nautilus Rust crates in this repo for ABI simplicity. Governs U3, U4, U6.
- KTD4. **Depth priority** — implement LastTrade+BBO first, then OrderBook summary; depth-by-order only after summary path works. Governs U5.
- KTD5. **LucidTrading config** — document/require system name override (`LucidTrading`) + `wss://rprotocol.rithmic.com:443`; map MY046 env names to `rithmic-rs` env/config. Governs U1, U7.
- KTD6. **Reconnect** — use `rithmic-rs` connect strategies + explicit app-level resubscribe loop (library does not auto-resubscribe). Governs U2.
- KTD7. **History loaders** — prefer `*_all` APIs to avoid silent 10k truncation. Governs U5.
- KTD8. **Tests** — fixture/unit tests for parsers always; live smoke script gated by env credentials. Governs U7.

### High-level design

```text
TradingNode (nautilus_trader 1.231.x)
  ├─ RithmicLiveDataClientFactory → LiveMarketDataClient
  │     └─ InstrumentProvider
  └─ RithmicLiveExecClientFactory → read-only LiveExecutionClient
          │
          ▼
python/rithmic_connect (Cython NT types + factories)
          │
          ▼
rithmic_connect._lib (PyO3)
          │
          ▼
Rust facade → rithmic-rs plants (ticker / history / pnl)
```

Follow Nautilus adapter layout conventions from `adapters/tardis` and `adapters/_template` (config, constants, providers, data, execution, factories).

### Assumptions / constraints

- One Rithmic login session at a time (close MotiveWave).
- Do not commit secrets, certs, or gated `.proto` files.
- Phase 1 must compile against published `nautilus_trader` wheels (no monorepo patch).

### Sequencing

U1 → U2 → U3 → U4 → U5 → U6 → U7  
U5 may start after U4 live ticks work. U6 can proceed in parallel with U5 after U3.

## Implementation Units

### U1. Repo scaffold and Nautilus pin

- **Goal:** Make the package installable and document LucidTrading env mapping.
- **Requirements:** R1, R11, R12
- **Files:**
  - `pyproject.toml`
  - `crates/rithmic-connect/Cargo.toml`
  - `crates/rithmic-connect/src/lib.rs`
  - `python/rithmic_connect/config.py`
  - `python/rithmic_connect/constants.py`
  - `.env.example`
  - `README.md`
  - `tests/test_config.py`
- **Approach:** Maturin workspace; pin `nautilus_trader==1.231.*`; config objects mirroring Tardis/template patterns; map MY046 env names onto `rithmic-rs` expectations (system name LucidTrading).
- **Test scenarios:**
  - Config loads from env with LucidTrading system override.
  - Missing credentials fail with clear error (no password echo).
  - Package imports without live network.
- **Verification:** `pytest tests/test_config.py`; `maturin develop` succeeds on a clean venv with NT 1.231.
- **Dependencies:** none

### U2. Rust multi-plant session facade

- **Goal:** Connect ticker/history/pnl via `rithmic-rs`, expose a single session API for Python, never send order commands.
- **Requirements:** R3, R9, R12
- **Files:**
  - `crates/rithmic-connect/src/session.rs`
  - `crates/rithmic-connect/src/config.rs`
  - `crates/rithmic-connect/src/error.rs`
  - `crates/rithmic-connect/tests/session_unit.rs`
- **Approach:** Wrap `RithmicTickerPlant`, `RithmicHistoryPlant`, `RithmicPnlPlant`; optional account list helper only if it cannot place orders; implement reconnect+resubscribe policy (KTD6); quarantine any `RithmicOrderPlant` trading APIs behind unused/feature-gated code or omit entirely.
- **Test scenarios:**
  - Unit: config builder rejects incomplete Live LucidTrading settings.
  - Unit: facade API surface has no public place/cancel/modify order methods.
  - Optional live (ignored by default): connect+disconnect ticker plant with credentials.
- **Verification:** `cargo test -p rithmic-connect`
- **Dependencies:** U1

### U3. PyO3 bridge and venue DTOs

- **Goal:** Expose session subscribe/request APIs and typed venue events to Python without binding Nautilus Rust crates.
- **Requirements:** R4
- **Files:**
  - `crates/rithmic-connect/src/python/mod.rs`
  - `crates/rithmic-connect/src/dto.rs`
  - `python/rithmic_connect/_convert.py`
  - `tests/test_convert_ticks.py`
- **Approach:** PyO3 classes/functions returning structured dicts or pyclasses for LastTrade, BBO, OrderBook summary, history bars/ticks, account/instrument PnL; Python `_convert.py` builds NT Cython types (KTD3).
- **Test scenarios:**
  - Fixture LastTrade → `TradeTick` fields (instrument_id, price, size, ts).
  - Fixture BBO → `QuoteTick` bid/ask.
  - Fixture Account PnL → dict suitable for `AccountState` construction.
  - Malformed/partial fixture → explicit conversion error.
- **Verification:** `pytest tests/test_convert_ticks.py`
- **Dependencies:** U2

### U4. Instrument provider + live data client (ticks/quotes)

- **Goal:** Out-of-tree `LiveMarketDataClient` delivering live trades/quotes for configured symbols.
- **Requirements:** R2, R5, R8, R10
- **Files:**
  - `python/rithmic_connect/providers.py`
  - `python/rithmic_connect/data.py`
  - `python/rithmic_connect/factories.py`
  - `python/rithmic_connect/__init__.py`
  - `tests/test_data_client_unit.py`
  - `examples/live_nq_ticks.py`
- **Approach:** Mirror Tardis/template: provider resolves front month / reference data; data client `_connect`/`_subscribe_trade_ticks`/`_subscribe_quote_ticks`; factory `create` for `TradingNode.add_data_client_factory`.
- **Test scenarios:**
  - Provider maps NQ/CME config → instrument id with venue constant.
  - Data client subscribe path calls Rust subscribe once per instrument.
  - Unsubscribe removes subscription.
  - Factory registers and constructs client with msgbus/cache/clock fakes or NT test doubles where available.
- **Verification:** `pytest tests/test_data_client_unit.py`
- **Dependencies:** U3

### U5. History + depth

- **Goal:** Historical ticks/bars requests and OrderBook summary (then optional depth-by-order).
- **Requirements:** R6
- **Files:**
  - `python/rithmic_connect/data.py` (extend)
  - `crates/rithmic-connect/src/session.rs` (extend)
  - `tests/test_history_convert.py`
  - `tests/test_depth_convert.py`
- **Approach:** Wire `_request_*` and depth subscribe methods; use history `*_all` loaders (KTD7); map OrderBook summary before depth-by-order (KTD4).
- **Test scenarios:**
  - History fixture → list of bars/ticks with monotonic timestamps.
  - Empty history fixture → empty list, no exception.
  - OrderBook summary fixture → NT order book deltas/depth type chosen in Q2.
  - Depth entitlement error → surfaced as clear client error, MD ticks unaffected.
- **Verification:** `pytest tests/test_history_convert.py tests/test_depth_convert.py`
- **Dependencies:** U4

### U6. Read-only execution client (account/positions)

- **Goal:** Publish PnL plant account/position updates into Nautilus without order capability.
- **Requirements:** R2, R7, R9
- **Files:**
  - `python/rithmic_connect/execution.py`
  - `python/rithmic_connect/factories.py` (extend)
  - `tests/test_exec_readonly.py`
- **Approach:** Subclass `LiveExecutionClient`; on connect subscribe PnL; `generate_account_state` / position status reports; implement order methods as hard errors or no-ops that log and never call Rust order APIs (KTD2).
- **Test scenarios:**
  - PnL fixture → `generate_account_state` invoked with expected balances.
  - Position fixture → position status report fields mapped.
  - `submit_order` / `cancel_order` do not call wire layer (assert mock).
  - PnL connect failure → client reports degraded account path; does not crash node startup when configured soft-fail.
- **Verification:** `pytest tests/test_exec_readonly.py`
- **Dependencies:** U3

### U7. LucidTrading acceptance harness + docs

- **Goal:** Prove AE1–AE4 on real LucidTrading; document ops quirks.
- **Requirements:** R10, R11, S1–S4
- **Files:**
  - `scripts/smoke_lucid_nq.py`
  - `docs/references/ops-runbook.md`
  - `README.md` (extend)
  - `.github/workflows/ci.yml`
- **Approach:** Credentials-gated smoke: connect, resolve NQ front, receive N ticks, optional history window, optional account snapshot, assert no order calls; CI runs unit tests only by default (KTD8).
- **Test scenarios:**
  - Smoke exits 0 on healthy MD path when env present; exits 2 when env missing (skip semantics for CI).
  - README contains unofficial disclaimer + single-session warning.
  - CI green on unit tests without secrets.
- **Verification:** local `python scripts/smoke_lucid_nq.py`; `pytest`; CI workflow present.
- **Dependencies:** U4, U5, U6

## Verification Contract

- **Unit / conversion:** `pytest`
- **Rust:** `cargo test -p rithmic-connect`
- **Editable install:** `maturin develop` (or `pip install -e .` via maturin)
- **Live acceptance (manual):** `python scripts/smoke_lucid_nq.py` with `.env` (MotiveWave closed)
- **Quality gates:** no secrets in git; no `.proto` sources committed; Phase 1 public API has no order-submit entry points

## Definition of Done

- All Implementation Units U1–U7 complete with their verifications passing (live smoke where credentials exist).
- Product requirements R1–R12 satisfied for Phase 1; AE1 demonstrated; AE3–AE4 demonstrated or explicitly soft-failed with MD still healthy.
- README + ops runbook document LucidTrading config, single-session rule, and Phase 2 order deferral.
- No official-Nautilus branding; Apache-2.0 LICENSE retained.

## Appendix

### Requirements check summary

See `docs/references/requirements-check-2026-08-12.md`. Primary correction: account publishing requires read-only `LiveExecutionClient` (RQ-1).

### External pattern pointers

- `rithmic-rs` plants: ticker subscribe LastTrade+BBO; separate OrderBook/depth APIs; history `*_all`; PnL subscribe/snapshot; no auto-resubscribe on reconnect.
- Nautilus: `TradingNode.add_data_client_factory` / `add_exec_client_factory` before `build()`; Tardis for data-only shape; Hyperliquid exec for `generate_account_state` pattern.
