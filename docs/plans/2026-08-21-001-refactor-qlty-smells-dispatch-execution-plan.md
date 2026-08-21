---
title: "Refactor qlty smells: gateway dispatch + exec handlers"
date: 2026-08-21
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
execution: code
origin: session — `qlty smells --all` baseline review; reduce the two real complexity hotspots without disturbing intentional parity duplication
baseline: qlty smells --all run 2026-08-21 (see smell inventory below)
---

# Refactor qlty smells: gateway dispatch + exec handlers

## Goal Capsule

Cut the two genuine complexity hotspots found by `qlty smells --all` —
`dispatch()` in `crates/rithmic-gateway/src/server/dispatch.rs` (complexity 227,
68 returns, 4 duplication blocks) and the order-notification path in
`python/rithmic_nt_connect/execution.py` (file total 441) — **with zero behavior
change**, verified by the existing gate/reconnect/parity test suites.

Explicitly **preserve** smells that are structural by design (direct ↔ gateway
parity signatures, closed-set enum mappers, script/test mains).

Stop when: `qlty smells --all` shows no function above complexity 100; all
AGENTS.md verify commands pass; gate + reconnect + parity tests unchanged and
green.

## Baseline inventory (2026-08-21)

| Location | Smell | Count | Verdict |
| --- | --- | --- | --- |
| `crates/rithmic-gateway/src/server/dispatch.rs::dispatch` | high complexity / many returns / dup blocks (mass 232×2, 140×2, 122×2) | 227 / 68 rets | **Fix** (P1) |
| `python/rithmic_nt_connect/execution.py` | file total 441; `_handle_order_notification` 44, `_plant_poll_loop` 37, `_apply_drain_rows` 25 | 441 | **Fix** (P1–P2) |
| `crates/rithmic-gateway/src/server/mod.rs::teardown`, `python/rithmic_gateway/spawn.py::spawn_gateway` | high complexity | 46 / 46 | Fix if cheap (P3) |
| `session.py` ↔ `gateway_wire.py` ↔ `client.py` ↔ PyO3 `mod.rs`: `place_order` / `place_bracket_order` / `modify_order` param-count + dup | many params ×6, dup mass 86–149 | — | **Keep** — AGENTS.md direct↔gateway parity hard rule |
| `_orders.py::kind_from_notify`, `execution.py::_order_status_from_event` | many returns (28 / 10) | — | **Keep** — flat closed-set enum maps |
| `scripts/*` mains (`verify_order_dry_run` 39, `smoke_gateway_shared_ticks` 45), `status_progress.py::parse_marks` 21 | complexity | — | **Keep** — dev tooling |
| `tests/e2e/exec_harness.py` total 75, `order_dsl.py`, `parity_helpers.py` | params / total | — | **Keep** — harness |

## Product Contract

### Problem

1. `dispatch()` is one giant `match body { … }` where every RPC arm re-inlines
   the same prologue (no-session check → trading gate → plant ensure → handle
   clone → call → map error). Each new RPC grows it linearly and the
   subscription-intent arms duplicate attach/rollback/note plumbing — exactly
   the shape that already caused the bracket-fanout regression noted in
   AGENTS.md.
2. `_handle_order_notification` mixes routing (tracked vs untracked vs drain),
   fill extraction, dedup, and status-event publication in one 180-line body.
   Exec honesty rules (unknown ≠ rejected, dedup by venue id, never drop exec
   events) are enforced inside nested branches, making them easy to break.

### Requirements

- R1. **Behavior-preserving only.** No protocol, frame, gating, refcount, or
  restore semantics change. Diff review must show moves, not rewrites.
- R2. Dispatch refactor splits into a `server/dispatch/` module directory with
  submodule boundaries mirroring the AGENTS.md RPC-kind taxonomy; each
  `Body` variant routes through its kind's module, and subscription-intent arms
  share one intent-handling path per direction (`subscribe_topic` /
  `unsubscribe_topic` over a `TopicKind` enum) so attach / note / rollback
  ordering lives exactly once **per failure template** — the two templates must
  stay separate (subscribe rolls back on first-join failure; unsubscribe
  re-notes intent on teardown failure).
- R3. The four dispatch duplication blocks (mass 232/140/122) collapse into the
  shared helpers; no copy remains.
- R4. `_handle_order_notification` splits into: field parse → route decision →
  tracked-path handler → untracked-path handler. Fill dedup and unknown-status
  handling stay in exactly one place each.
- R5. **Parity surfaces untouched:** `session.py` / `gateway_wire.py` /
  `GatewayClient` / PyO3 method signatures stay as-is. Do not "fix" those
  many-parameter or duplication smells.
- R6. Every phase ends green: fmt/clippy/tests + ruff/ty/pytest per AGENTS.md
  Verify section; `docs/STATUS.md` scoreboard check unaffected.
- R7. Gate/reconnect tests keep asserting intent bits (`RestorePlan`,
  `restore_intents`) — if any test needs semantic edits to pass, stop and
  reassess (that means R1 was violated).

### Scope boundary

**In scope**: `dispatch.rs` decomposition; `execution.py` notification-path
split; optionally `spawn_gateway` / `teardown` step-splitting.

**Out of scope**: parity-signature dedup across direct/gateway paths; script
rewrites; test-harness reshaping; new capabilities; STATUS capability changes.

## Phases

### P1 — `dispatch.rs` → `server/dispatch/` split by RPC kind (chosen: B+A hybrid)

Module layout mirrors the AGENTS.md RPC-kind taxonomy so future "wire it on
both paths" changes have an obvious home:

```
crates/rithmic-gateway/src/server/dispatch/
├── mod.rs            # thin match body → handler; frame ctor helpers; tests stay here
├── subscriptions.rs  # ALL subscription-intent logic (ticker/book/time-bars/pnl/
│                     # order+brackets), incl. existing subscribe_order_plant_stream,
│                     # fail_order_plant_stream, rollback fns
├── orders.rs         # trading one-shots (place/cancel/modify/cancel-all) + order-handle loads
├── history.rs        # LoadTicks / LoadTimeBars / bar slices
└── info.rs           # RequestPlants / GetFrontMonth / GetReferenceData / ResolvedAccount / RMS
```

Style constraint: no higher-ranked async closures (awkward on stable Rust).
Helpers return values / `Result<_, Frame>`; arms keep their own call + error
mapping.

1. **Subscription intents** (`subscriptions.rs`) — introduce a `TopicKind` enum
   (`Ticker`, `Book`, `TimeBars`) discriminating: client set, `note_*`/`forget_*`,
   session call, error string. Two helpers with **deliberately separate**
   failure templates:
   - `subscribe_topic(...)`: dedup check → `topic_lock` → `attach_shared_topic`
     → `note_*` (0→1) → insert → on first: session guard + venue join +
     rollback on failure.
   - `unsubscribe_topic(...)`: mirror-lock → remove → `forget_*` → venue leave
     → **on failure re-note the intent** so reconnect still re-joins (the
     line-351 semantics) → `release_md_if_unused`.
   Do NOT merge into one parameterized function: the rollback directions differ
   by design, and flattening them is how fanout regressions happen.
2. **Order-handle prelude** (`orders.rs`) — `order_handle_or_frame(state,
   request_id, err_code) -> Result<OrderHandle, Frame>` absorbing the repeated
   `ensure_order_plant()` / `clone_order_handle()` nested match (lines ~506,
   ~543, ~576). Used by `LoadOrders` / `LoadProductRmsInfo` /
   `LoadAccountRmsInfo`; each arm keeps its own load call and error mapping
   (incl. the `ReconciliationUnavailable` special case).
3. One-shot command arms keep their inline gate → call → map shape (~15 lines
   each); only their home moves to `orders.rs`. No gating abstraction.
4. `Body::ResetTickerPlant` restore loop may reuse the `TopicKind` session-call
   discrimination if it fits without behavior change; otherwise leave.
5. `mod.rs::dispatch()` becomes a pure router: per-variant delegation →
   response frame, ~80 lines, complexity < 15. Shared plumbing
   (`topic_lock`, `ack_frame`/`error_frame`/`plant_err_frame`,
   `no_session_frame`) stays in `mod.rs` as `pub(super)`.
6. Collapse the structural dup pairs into the helpers (R3): intent templates
   (Subscribe↔Book, mass 232/140/122) and order-handle loads (RMS pair,
   mass 134→gone via `RmsKind`). **Accepted residue:** the
   `place_order`↔`place_bracket_order` twin (~mass 134) stays — item 4 keeps
   one-shot arms inline and they differ in gate/message/session call, so the
   mass-100 dup target does not apply to them (plan amendment 2026-08-21,
   post-implementation).

Verify: `cargo clippy -p rithmic-gateway --all-targets -- -D warnings`;
`cargo test -p rithmic-gateway` (gate/reconnect suites); grep confirms no
remaining mass-200+ dup block via `qlty smells`.

### P2 — `execution.py` notification split

1. Extract from `_handle_order_notification`:
   - `_order_fill_event_from_fields(...)` (fill construction + dedup-key use),
   - `_tracked_status_transition(...)` (typed event emission for tracked
     orders),
   - leaving the router as: resolve client order id → tracked? tracked-path :
     `_handle_untracked_notification`.
2. `_plant_poll_loop`: extract per-iteration body (`_poll_once`) so loop
   bookkeeping (reconnect re-arm, transient errors) reads top-down.
3. `_apply_drain_rows`: split row-classification from event application.
4. Keep all docstring-level honesty rules intact; move code, don't restate
   semantics.

Verify: `uv run ruff check . && uv run ty check python/rithmic_nt_connect &&
uv run pytest -q`; exec unit fixtures keep venue payload shapes (AGENTS.md
venue-shaped-fixtures rule).

### P3 (optional, only if diff stays mechanical)

Split `spawn_gateway` (spawn.py) and `teardown` (server/mod.rs) into ordered
per-resource steps (resolve bin → build env → spawn → verify handshake;
reverse-order teardown). Skip if either requires touching control flow, not
just extraction.

## Risks

- **Bracket/order-plant stream regression**: the fanout-loss bug happened here
  before. Mitigation: R2 single intent path + existing gates/reconnect tests
  asserting intent bits must pass unmodified.
- **Exec event loss during refactor**: fills/status must never drop. Mitigation:
  dedup keys and unknown-status policy each live in exactly one extracted
  function; pytest exec suite green.
- **Scope creep into parity dedup**: explicitly refused (R5).

## Verification (whole change)

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect
uv run ruff check . && uv run ruff format --check .
uv run ty check python/rithmic_nt_connect tests
uv run pytest -q
qlty smells --all   # confirm: nothing above complexity 100; parity dups remain (accepted)
```

No live smokes required (behavior-preserving), but `scripts/smoke_lucid_nq.py`
is a reasonable post-merge confidence run when MotiveWave is closed.
