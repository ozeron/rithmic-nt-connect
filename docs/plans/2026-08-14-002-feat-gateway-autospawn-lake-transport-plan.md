---
title: "Gateway auto-spawn harden + lake history transport (E2E shared login)"
date: 2026-08-14
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
product_contract_source: ce-plan-bootstrap
execution: code
origin: session — migrate market-data-lake Rithmic fetch onto rithmic-gateway so live trading and lake history share one login
related: docs/plans/2026-08-13-004-feat-session-gateway-plan.md
sibling_repo: ../market-data-lake
reviewed: gap-fix 2026-08-14 (closed)
settled: lake hardcodes gateway; client-side history chunking in rithmic_gateway
---

# Gateway auto-spawn harden + lake history transport (E2E shared login)

## Goal Capsule

Make **Python auto-spawn of `rithmic-gateway` reliable**, add **library-owned history window chunking + sane history RPC timeouts** on `GatewayClient`, then **hardcode market-data-lake** Rithmic history onto that client so lake sync shares the live login — **without a second Rithmic login / ForcedLogout**.

Authority (highest first): `docs/references/ops-runbook.md` + `docs/references/gateway-remote.md` → `docs/STATUS.md` session-gateway row → lake `AGENTS.md` / `docs/rithmic-futures-bars-design.md` → this plan → `docs/plans/2026-08-13-004-feat-session-gateway-plan.md`.

**Product Contract preservation:** session bootstrap + gap-fix closures (2026-08-14). Auto-spawn meaning unchanged; lake transport decision changed from opt-in `direct` default → **gateway-only**.

Stop when: lake `download-data --venue rithmic` always uses `GatewayClient` (auto-spawn or warm attach); wide windows succeed via library chunking (not a single 30s RPC); unit tests cover spawn aliases/overwrite, bin resolve, chunk merge, daily markers; Lucid shared smoke optional; `async_rithmic` removed from the lake fetch path (dep may remain unused until a follow-up cleanup PR).

## Product Contract

### Problem

1. Live MotivesWave / Nautilus and lake `async_rithmic` both open **direct** Rithmic MD sessions → **one login fights**.
2. Shared-login gateway + Python auto-spawn exist, but bin discovery / env aliases are brittle, and lake never dials `rithmic_gateway`.
3. **History scale gap:** Rust plants already time-slice (~4h for 1m) inside `load_time_bars_all`, but Python `GatewayClient.load_time_bars` is **one unix RPC** with **`DEFAULT_RPC_TIMEOUT_SEC = 30`** and a **16MiB** frame — month-long lake syncs time out or blow the frame even though the parent is slicing correctly.
4. **async_rithmic** handles long windows via **replay pagination** (`max_pages` + idle/stall timeout that resets on progress) — different mechanism than Rust calendar slices; lake today relies on that (`max_pages=10_000`). The gateway Python binding must own an equivalent “wide window just works” API so lake does not invent chunking.

### Requirements

- R1. Python `GatewayClient` auto-spawn remains the **only** child-side parent starter for local `unix://` (no second Rithmic login from the lake process).
- R2. Auto-spawn resolves the gateway binary from, in order: explicit `gateway_bin` / `RITHMIC_GATEWAY_BIN` → `PATH` → walk-up from package/`cwd` for `target/{release,debug}/rithmic-gateway` and `CARGO_TARGET_DIR` when set. Clear `SpawnError` if none found. **No auto-`cargo build`** in library resolve (smoke scripts may build separately).
- R3. Env alias parity for fingerprint (read) with **SessionConfig / lake precedence**:
  - WSS: `RITHMIC_GATEWAY` then `RITHMIC_URL` (never `RITHMIC_GATEWAY_LISTEN`);
  - system: `RITHMIC_SYSTEM` then `RITHMIC_SYSTEM_NAME`;
  - user: `RITHMIC_USER` then `RITHMIC_USERNAME` (parity with lake).
  Spawn **overwrites** (not `setdefault`) canonical child env: `RITHMIC_URL`, `RITHMIC_SYSTEM_NAME`, `RITHMIC_USER`, `RITHMIC_ENV` from resolved `GatewayConfig`.
- R4. Password stays in curated env only — never argv / never protobuf Handshake. If lake has `RithmicCredentials` but process env lacks password, gateway transport **injects** password into spawn environ or errors clearly.
- R5. Auto-spawn still injects `RITHMIC_GATEWAY_IDLE_EXIT_SEC=5` when unset; preserves explicit `0` / `-1`.
- R6. **Lake hardcodes gateway transport** — always `GatewayClient`; no `RITHMIC_CONNECT_MODE=direct` lake path; no silent dual-client. (`RITHMIC_CONNECT_MODE` remains an **adapter-only** required key for Nautilus; lake ignores it.)
- R7. Gateway bar dicts feed lake normalize (`marker` / OHLCV). Daily: prefer `ts_event_ns` when present, else port `marker_to_ssboe` / YYYYMMDD handling before open math. Must not invent volume.
- R8. One fetch path inside lake: **sync and backfill** share the gateway transport; **audit** stays partition-only (no fetch). No separate “verify” client.
- R9. Lake must not hard-require Nautilus / maturin. Depend on pure Python `rithmic_gateway` (+ protobuf) only.
- R10. Docs: ops-runbook + lake AGENTS/design. **MotiveWave / R|Trader must be closed whenever a `rithmic-gateway` parent holds the login** (cold-start auto-spawn counts). “Shared parent” only multiplexes **gateway clients**, not MotiveWave.
- R11. **Library history API** (`rithmic_gateway`): wide-window load that **chunks client-side** and merges, so each unix RPC stays within timeout + frame limits. Lake calls this helper — lake does not reimplement slice math.
- R12. History RPC timeout policy: per-chunk socket timeout long enough for one plant slice (default ≥ 120s, configurable); not a single 30s budget for a multi-month job.
- R13. Explicitly **out of lake scope this change:** explorer IBKR fetch, IBKR queue, CLI `--connect-mode` (gateway is hardcoded), remote TLS.

### Scope boundary

**In scope**

- `python/rithmic_gateway/` spawn/config harden; **client-side history chunking helper**; history timeout; tests; ops-runbook.
- Nautilus-free install story for `rithmic_gateway`.
- Sibling lake: replace `async_rithmic` fetch with gateway helper; normalize daily markers; credentials password inject for spawn; AGENTS/design.

**Out of scope**

- Gateway remote TLS / TCP (v2).
- Changing Rust plant slice lengths (already correct SoT for plant protocol).
- Streaming `LoadTimeBars` responses (chunked unary RPCs are enough).
- Replacing IBKR; merging venues; live tick lake product; orders from lake.
- Publishing to PyPI.
- Forcing Nautilus adapter defaults to change.

### Actors

- A1. Live trader / Nautilus (may hold long-lived gateway parent).
- A2. Lake operator (`download-data --venue rithmic`).
- A3. CI (mock spawn + mock RPC chunks; no Lucid).

### Key flows

- F1. **Cold start:** no listener → lake connect → auto-spawn → Ready → chunked `load_time_bars_*` → normalize → QA → parquet. MotiveWave closed.
- F2. **Warm attach:** long-lived parent already up (`IDLE_EXIT` unset/-1) → lake dials → chunked history → write. Preferred for live+lake.
- F3. **Bin missing:** actionable `SpawnError`.
- F4. **Wide window:** client splits `[start,end]` into slices matching `bar_slice_secs` semantics → N RPCs → merge/dedupe by `marker` → return full list.

### Acceptance examples

- AE1. Alias-only env + dual conflicting `RITHMIC_GATEWAY`/`RITHMIC_URL` → config uses SessionConfig precedence; spawn child env **overwritten** to config.url (canonical keys); password on env not argv.
- AE2. `resolve_gateway_bin` finds fake `target/release/rithmic-gateway` when PATH empty; missing → error mentions `RITHMIC_GATEWAY_BIN`.
- AE3. Lake fetch always constructs gateway transport (no async branch); mocked chunked client returns bars → normalize frame OK.
- AE4. Daily bar with `marker=YYYYMMDD` (and/or `ts_event_ns`) → sane open UTC + Globex `session_date`.
- AE5. Unit: multi-day window → helper issues **multiple** `load_time_bars` RPCs (mock), merged length = sum of chunks, boundary dupes removed.
- AE6. Ops (optional Lucid): long-lived parent + lake sync one NQ 1m day + second gateway consumer; no ForcedLogout.

## Planning Contract

### Key technical decisions

| ID | Decision | Rationale |
| --- | --- | --- |
| KTD1 | Ship/install **`rithmic_gateway` Nautilus-free** — concrete: nested package or documented `PYTHONPATH=<rithmic-connect>/python` + `protobuf`, with CI import test that fails if `nautilus_trader` loads. Prefer nested `python/rithmic_gateway/pyproject.toml` if path-dep on root package pulls maturin. | R9 hard constraint |
| KTD2 | **Lake = gateway only** (hardcoded). No lake `RITHMIC_CONNECT_MODE`. Adapter keeps requiring `RITHMIC_CONNECT_MODE`. Shared `.env` with `gateway` does not change lake (already gateway). | Session decision; closes mode-clash gap |
| KTD3 | Normalize SoT in lake; daily via `ts_event_ns` prefer else YYYYMMDD→ssboe (port plants semantics). | Proto keeps raw `marker` |
| KTD4 | Bin search: env → PATH → walk-up `target/{release,debug}` + `CARGO_TARGET_DIR`. Drop invented `RITHMIC_CONNECT_ROOT` unless implemented. No library auto-build. | Match smoke intent without cargo in-library |
| KTD5 | Spawn **assigns** canonical URL/system/user/env from `GatewayConfig` (overwrite). Aliases resolved only in `from_env`. | Fixes setdefault fingerprint bug |
| KTD6 | Dial-first connect (existing). Lake does not flock. | Session-gateway F2/F3 |
| KTD7 | Sequence: U1 spawn/config → U2 history chunking+timeout in `rithmic_gateway` → U3 install/docs → U4 lake swap → U5 optional Lucid | Lake blocked on chunking API |
| KTD8 | Wire ints in **lake or `rithmic_gateway` constants module** — copy `(1s→1,1) (1m→2,1) (1h→2,60) (1d→3,1)`. **Never** import `rithmic_nt_connect.data` (pulls Nautilus). | R9 |
| KTD9 | Lucid AE6 evidence optional; AE1–AE5 merge gates. | STATUS pattern |
| KTD10 | **Two-layer history windowing (do not conflate):** (1) **Rust plants** `load_sliced` / `bar_slice_secs` (~4h for 1m) — plant protocol SoT, already used by gateway `LoadTimeBars` dispatch. (2) **Python `GatewayClient`** client-side chunked unary RPCs using the **same slice lengths**, merge+dedupe — unix timeout/frame SoT. async_rithmic’s `max_pages` pagination is **not** reimplemented; calendar slicing replaces it for this stack. | Closes timeout/frame gap without rewriting Rust |
| KTD11 | Add `GatewayClient.load_time_bars_range` (name flexible) that chunks; keep raw `load_time_bars` as single-slice RPC. Lake calls the range helper only. | Clear API boundary |
| KTD12 | Per-chunk RPC timeout default **120s** (configurable on client); dial timeout can stay 30s. | One 4h plant slice can exceed 30s |
| KTD13 | E2E ops order: prefer **start long-lived parent first** (`IDLE_EXIT` unset/-1), then lake + live clients. Lake-only auto-spawn OK (5s idle after last client). | Idle-exit hazard |
| KTD14 | Map `GatewayError` through lake hint wrapper (MotiveWave / plant) on failure paths. | Operator parity with async hints |
| KTD15 | Auth token: document `RITHMIC_GATEWAY_AUTH_TOKEN` must match parent for warm-attach when set. | Handshake |

### Chunking research (settled)

| Stack | Mechanism | Where |
| --- | --- | --- |
| async_rithmic | Replay **pages** (`max_pages`) + **idle/stall** timeout reset on progress | `async_rithmic/plants/history.py` |
| rithmic-plants | Calendar **time slices** (~4h 1m bars; wider for ≥15m/1h; whole-window for daily) + retry + dedup | `crates/rithmic-plants/src/history.rs` → `session.load_time_bars_all` |
| Gateway dispatch today | One proto RPC → `load_time_bars_all` (Rust slices internally) → one response | `dispatch.rs` `LoadTimeBars` |
| Gateway Python today | One RPC, **30s** hard socket timeout, **16MiB** max frame | `client.py` |

**Implication:** Rust slicing alone does not fix the Python client waiting on the full aggregated job. Library must chunk at the **unix RPC** layer (KTD10–12).

### Technical design (directional)

```text
download-data --venue rithmic
        │
        ▼
  GatewayConfig.from_env (aliases + precedence)
  GatewayClient.connect()          # dial or auto-spawn
  load_time_bars_range(...)        # NEW: client chunk loop
        │  for each slice (bar_slice_secs semantics):
        │      load_time_bars(slice)  # existing unary RPC
        │  merge + dedupe(marker)
        ▼
  normalize (+ daily marker fix) → QA → parquet

Parent (per RPC): load_time_bars_all → plant load_sliced (unchanged)
```

### Assumptions

- Sibling checkouts `rithmic-connect` next to `market-data-lake`.
- LucidTrading + `wss://rprotocol.rithmic.com:443` production defaults.
- Password available via env and/or `RithmicCredentials` at fetch time.

### Risks

| Risk | Mitigation |
| --- | --- |
| Dual-env fingerprint drift | KTD5 overwrite + AE1 |
| Idle-exit kills shared parent | KTD13 docs; live holds Ready peer or manual parent |
| Frame 16MiB on fat chunk | Client slices ≈ Rust 4h; still under limit for 1m |
| Stale cargo binary | Document rebuild; no auto-build |
| Removing async path surprises operators | AGENTS + design call out gateway-only; clear import/bin errors |
| Nested packaging friction | U3 DoD: import-without-nautilus CI check |

### Open questions

- None blocking. Deferred: drop `async-rithmic` from lake `pyproject` in a follow-up cleanup once gateway path is proven.

### Execution order

1. U1 spawn/config harden + overwrite + precedence tests
2. U2 `load_time_bars_range` + timeout + slice constants + tests
3. U3 Nautilus-free install + ops-runbook (MotiveWave + idle-exit + auth token)
4. U4 lake gateway-only swap + normalize daily + hints + docs
5. U5 optional Lucid evidence

## Implementation Units

### U1. Harden Python auto-spawn (aliases + overwrite + bin discovery)

**Goal.** Reliable spawn fingerprint matching SessionConfig/lake env.

**Repos:** `rithmic-connect`

**Primary files**

- modify: `python/rithmic_gateway/spawn.py` — overwrite canonical keys; curated env stays canonical-only
- modify: `python/rithmic_gateway/config.py` — alias precedence; user alias
- modify: `tests/test_rithmic_gateway_client.py` and/or `tests/test_gateway_spawn_resolve.py`

**Requirements.** R2–R5, AE1, AE2

**Test scenarios**

- Dual `RITHMIC_GATEWAY`≠`RITHMIC_URL` → child gets config.url via overwrite.
- Precedence matches SessionConfig order.
- `RITHMIC_GATEWAY_LISTEN` never used as WSS.
- Bin resolve release/debug/missing.
- Idle-exit 5 / 0 / -1 preserved.
- Password never on argv.

### U2. Client-side history chunking + timeout (`rithmic_gateway`)

**Goal.** Wide windows work through multiple unary RPCs; lake consumes this API only.

**Repos:** `rithmic-connect`

**Primary files**

- modify: `python/rithmic_gateway/client.py` — `load_time_bars_range` (or equivalent); per-chunk timeout default 120s for history RPCs
- create: `python/rithmic_gateway/history_window.py` — `bar_slice_secs` / `window_slices` ported from `crates/rithmic-plants/src/history.rs` (Python, no Rust/PyO3)
- modify: tests for chunk count, merge dedupe, timeout kwarg

**Requirements.** R11, R12, AE5

**Approach**

- Port slice length table from Rust `bar_slice_secs` (1m→4h, ≥15m minute→12h, ≥60m→24h, daily/weekly→single wide window, second→15m).
- `window_slices` inclusive boundaries + dedupe by `marker` after merge (same as plant boundary overlap).
- Do not change proto or Rust dispatch in this unit unless a bug is found.

**Test scenarios**

- 12h of 1m → ≥2 mock RPC calls; merged length correct; shared boundary marker once.
- Daily type → single slice.
- Short window → one RPC.
- Configurable timeout passed through to chunk RPCs.

### U3. Nautilus-free install + ops docs

**Goal.** Importable `rithmic_gateway` without Nautilus; honest ops story.

**Repos:** `rithmic-connect`

**Primary files**

- modify: packaging per KTD1; `docs/references/ops-runbook.md`; `README.md` pointer; optional `docs/STATUS.md`

**Requirements.** R9, R10, R13, KTD13, KTD15

**Test scenarios**

- `python -c "from rithmic_gateway import GatewayClient"` without `nautilus_trader` importable/loaded.
- Docs: MotiveWave closed for any parent; warm attach vs auto-spawn idle-exit; `RITHMIC_GATEWAY` vs `LISTEN`; auth token; build bin / `RITHMIC_GATEWAY_BIN`.

### U4. Lake gateway-only transport

**Goal.** Replace async_rithmic fetch with `GatewayClient` + `load_time_bars_range`.

**Repos:** `market-data-lake`

**Primary files**

- modify: `packages/market-data-lake-tools/src/market_data_lake/tools/downloader/rithmic_client.py` — gateway only
- create (optional): `.../rithmic_gateway_transport.py`
- modify: `.../rithmic_normalize.py` — daily marker / `ts_event_ns`
- modify: `.../rithmic_credentials.py` — ensure spawn sees password; user/system/url aliases already mostly present
- modify: `packages/market-data-lake-tools/pyproject.toml` — path/extra for `rithmic_gateway` (+ protobuf); do **not** add nautilus; `async-rithmic` removal optional follow-up
- modify: `packages/market-data-lake-tools/tests/test_rithmic_futures.py` (+ transport/chunk/normalize daily tests)
- modify: `AGENTS.md`, `docs/rithmic-futures-bars-design.md`

**Requirements.** R1, R4, R6–R8, R10, KTD14, AE3, AE4

**Approach**

- Hardcode gateway connect + range load.
- Inject password into environ for spawn when needed.
- Wrap failures with existing MotiveWave/plant hints.
- sync/backfill only; audit unchanged.
- Wire ints local (KTD8).

**Test scenarios**

- No code path imports `async_rithmic` in client module (prefer removed from fetch).
- Mock range helper → normalize.
- Daily YYYYMMDD fixture.
- Missing `rithmic_gateway` import → clear error (install/PYTHONPATH/bin).
- Credentials password inject / missing password error.

**Explicit non-goals in this unit:** explorer, queue, CLI mode flag.

### U5. Optional Lucid shared-login evidence

**Repos:** both (ops)

**Requirements.** AE6, KTD9, KTD13

**Approach.** Long-lived parent first; lake short window; parallel `gateway_tick_consumer` / shared smoke. Record only if run.

## Verification Contract

**rithmic-connect**

```bash
cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect
uv run pytest -q tests/test_rithmic_gateway_client.py tests/test_gateway_spawn_resolve.py tests/test_gateway_history_window.py
```

(Adjust names to files actually added.)

**market-data-lake**

```bash
uv run pytest -q packages/market-data-lake-tools/tests/test_rithmic_futures.py
```

**Optional Lucid**

```bash
# long-lived parent (IDLE_EXIT unset)
cargo run -p rithmic-gateway --bin rithmic-gateway
# lake — gateway hardcoded; needs bin or running parent
download-data --venue rithmic --dataset futures_bars ...
```

Quality gates: overwrite spawn test; chunk RPC count test; no Nautilus in lake deps; password not on argv; MotiveWave guidance corrected in docs.

## Definition of Done

- [ ] U1–U4 green on AE1–AE5
- [ ] Lake Rithmic fetch has **no** live `async_rithmic` connect path
- [ ] `load_time_bars_range` (or named equivalent) lives in `rithmic_gateway`, used by lake
- [ ] Spawn overwrites canonical env from config
- [ ] Ops-runbook: MotiveWave + idle-exit + auth token + chunking note
- [ ] Lake AGENTS/design: gateway-only + shared parent recipe
- [ ] U5 optional

## Appendix

### Already exists (do not re-implement)

- `GatewayClient.connect` dial-first + auto-spawn + flock attest
- Rust `load_time_bars_all` / `load_sliced` / `bar_slice_secs`
- Idle-exit inject on auto-spawn
- Lake credentials aliases for system/url (extend tests; password inject for spawn)

### Chunking research detail

**async_rithmic:** one calendar window; pages until `end_time` covered or `max_pages`; idle timeout resets on each bar/completion message.

**rithmic-plants:** splits calendar window into fixed-length slices before each plant replay; daily uses one wide slice so YYYYMMDD markers are not missed.

**Why Python still chunks:** parent returns one aggregated response per unix RPC; 30s socket timeout and 16MiB frame apply to that whole response. Client-side unary chunking keeps each wait/frame bounded even when lake asks for months.

### Gap-fix closures (2026-08-14)

| Finding | Resolution |
| --- | --- |
| Spawn `setdefault` dual-env bug | KTD5 overwrite |
| `RITHMIC_CONNECT_MODE` clash | KTD2 lake ignores; gateway hardcoded |
| 30s RPC / wide windows | KTD10–12 client chunking + 120s/chunk |
| Daily YYYYMMDD | KTD3 / AE4 |
| MotiveWave doc wrong | R10 |
| Idle-exit vs live | KTD13 |
| Import `data.py` Nautilus footgun | KTD8 |
| R8 “verify” | R8 audit-only |
| Scope holes explorer/queue/CLI | R13 |
| U4 credentials “already done” | Reframed to gateway hardcode + password inject + docs |
| `RITHMIC_CONNECT_ROOT` | Dropped (KTD4) |
| async_rithmic paging vs Rust slices | Documented; calendar chunking chosen for gateway Python |

### Settled in session

- E2E scope (harden + lake).
- Shared gateway parent isolation model.
- **Lake hardcodes gateway connect.**
- Chunking belongs in **`rithmic_gateway` Python** (RPC layer), reusing Rust slice semantics; plant slicing stays in Rust.
