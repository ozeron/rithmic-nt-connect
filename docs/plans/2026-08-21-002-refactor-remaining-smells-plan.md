---
title: "Refactor remaining qlty smells: spawn/teardown + gateway-bin reconnect"
date: 2026-08-21
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
execution: code
origin: session — follow-up to 2026-08-21-001 after dispatch/execution hotspots were closed
baseline: qlty smells --all at commit 267d82d
related: docs/plans/2026-08-21-001-refactor-qlty-smells-dispatch-execution-plan.md
---

# Refactor remaining qlty smells: spawn/teardown + reconnect pump into lib

## Goal Capsule

Close the remaining **genuine** smell candidates left after the dispatch/
execution refactor — `spawn_gateway` (51), `ClientCtx::teardown` (46), and the
gateway **reconnect trio** (`run_event_pump` 24, `restore_intents` 28,
`reconnect_loop`) — with zero behavior change, then record explicit
accept-verdicts for everything else so future `qlty smells` reviews have a
stable baseline.

The reconnect trio's smell is architectural, not local: **event-pump and
reconnect business logic lives in the binary**, which makes it untestable by
the lib test suites and blocks sharing `TopicIntent` intent discrimination
with `restore_intents`. The root fix relocates it into the library; a bin-local
extraction would be a patch that leaves that class of problem open.

Stop when: pump/reconnect logic lives under `src/` (lib), `main()` in the bin
is CLI orchestration only; `spawn_gateway` < 25; `teardown` < 25;
`restore_intents` reuses `TopicIntent::venue_join`; new lib-side unit tests
cover restore counting; AGENTS.md verify gates green; baseline verdict table
added to completion notes.

## Baseline (267d82d) and verdicts

| Location | Smell | Verdict |
| --- | --- | --- |
| `python/rithmic_gateway/spawn.py::spawn_gateway` | 51 | **Fix** (R1) |
| `crates/rithmic-gateway/src/server/mod.rs::teardown` | 46 | **Fix** (R2) |
| `bin/rithmic_gateway.rs` reconnect trio (`run_event_pump` 24, `restore_intents` 28, `reconnect_loop`) | business logic in binary; 3 dup resubscribe blocks; untestable in lib suites | **Fix — relocate to lib** (R4) |
| `data.py` total 153 | aggregate over ~20 small converters, no fn > 20 | **Keep** — conversion layer; splitting adds indirection |
| `client.py::_poll_filtered_unlocked` 24 / total 116 | lake-facing client poll filter | **Keep** |
| `config.py::from_env` 25, `verify.py::compare_ticks` 24 | env parsing / dev verifier | **Keep** |
| parity dups (`session.py`, `gateway_wire.py`, PyO3 mod.rs, orders.rs mass-134) | — | **Keep** (AGENTS.md hard rule / documented residue) |
| scripts mains, test harness, closed-set enum mappers | — | **Keep** |
| `idle_exit.rs::wait_until_should_exit` 21 | single wait loop | **Keep** |

## Requirements

- R1. Behavior-preserving only; diff review must show moves/extractions, not
  rewrites. Any test needing semantic edits stops the change.
- R2. `spawn_gateway`: split into ordered steps — listen validation → child-env
  build → spawn → handshake/ready wait → error cleanup — each independently
  named; the existing alias/env-overwrite semantics (spawn **overwrites**
  canonical child env; preserves explicit idle-exit values) must not move.
  Do NOT redesign credential truth (env aliases vs `GatewayConfig`) — that
  contract was settled by the 2026-08-14 autospawn plan; naming the precedence
  layers is the fix at this scope.
- R3. `teardown`: rewrite the three near-identical ticker/book/time-bars drain
  loops over `TopicIntent` discrimination (kills that dup block); pnl/order/
  brackets teardown stays bespoke.
- R4. **Relocate the reconnect trio from bin into lib**:
  - move `run_event_pump`, `reconnect_loop`, `restore_intents` (plus their
    private helpers `is_connection_issue` / `is_not_connected`) to
    `crates/rithmic-gateway/src/server/pump.rs`;
  - the bin keeps only CLI arg parsing / help / startup orchestration;
  - once in the lib module tree, `restore_intents` reuses
    `TopicIntent::venue_join` via `pub(in crate::server)` visibility — no
    public-API widening;
  - **failure policies stay separate and explicit**: dispatch rollback-on-fail
    vs pump log-and-continue are intentionally different; sharing session-call
    mapping must not merge the policies;
  - add lib-side unit tests for restore counting (restored/attempted tallies
    per intent kind) using a mock/fake where feasible; if the session type is
    not fake-able, test the pure parts (plan batching, tally math) and say so
    in completion notes.
- R5. `run_event_pump`: extract one poll-and-publish step over the four
  identical poll blocks after relocation. These are **sync** method calls, so
  plain fn pointers work (no async-closure problem) — contingent on the four
  poll methods sharing one signature; if they do not, keep four blocks inside
  the relocated function and record Keep.
- R6. Every phase ends green per AGENTS.md Verify section.

## Phases

### P1 — Python spawn steps

`spawn_gateway` actual seams (verified against source): listen validation
prologue → argv password defense → **inline env construction** (curated_env →
spawn_environ overlay → fingerprint overwrite → idle-exit setdefault →
password presence check) → Popen + stderr-drain thread → socket wait. Extract
`_build_child_env(config, environ)` (the inline block, ~25 lines — the real
complexity driver) and `_validate_listen(listen)`; `resolve_gateway_bin`
already exists as its own function — do not re-wrap it. Keep the
env-overlay **ordering** byte-identical (spawn_environ before fingerprint
overwrite is load-bearing for empty/stale process env).

Verify: `uv run pytest tests/test_gateway_spawn_resolve.py tests/test_packaging.py -q`;
ruff/ty green.

### P2 — Reconnect trio into lib, then teardown

1. **Relocate first** (R4): create `crates/rithmic-gateway/src/server/pump.rs`;
   move `run_event_pump`, `reconnect_loop`, `restore_intents`,
   `is_connection_issue`, `is_not_connected` verbatim; bin keeps CLI
   orchestration. Compile + existing tests green before touching bodies.
2. **Share intent discrimination**: widen only what's needed
   (`TopicIntent` + `key`/`forget`/`venue_leave` → `pub(in crate::server)`);
   rewrite `restore_intents`' three match blocks over intents. Tally counts
   and logging strings byte-identical.
3. **Extract poll-and-publish** in the relocated `run_event_pump` (R5).
4. **Teardown** (R3): rewrite the three drain loops over the same widened
   `TopicIntent`. Semantics that must NOT change: fire-and-forget errors
   (`let _ =`, no re-note — the client is dying; differs from
   `unsubscribe_topic`) and topic_lock held across forget + unsubscribe.
5. Add unit coverage for restore tallies per R4.

Verify: `cargo fmt --check && cargo clippy --workspace --all-targets -- -D
warnings && cargo test -p rithmic-gateway` (fanout/reconnect suites
unmodified; new pump tests added).

### P3 — Baseline recording + confidence smoke

Append final verdict table + residual counts to this plan's completion notes;
no STATUS.md capability changes (refactor-only). Because P1 touches the
auto-spawn path shared with lake and smokes, run one gateway smoke when
MotiveWave / R|Trader is closed: `python scripts/smoke_gateway_shared_ticks.py
--seconds 25`.

## Risks

- **Spawn env semantics**: alias precedence and overwrite behavior are load-
  bearing for shared-login (no second Rithmic login). Mitigation: R2 + spawn
  unit tests must pass untouched.
- **Teardown ordering**: detach-only refcount decrement order matters under
  concurrent first-peer subscribe. Mitigation: keep topic_lock usage identical;
  fanout suite green.
- **Restore policy divergence**: dispatch rollback vs pump log-and-continue
  are intentionally different — sharing `venue_join` must never merge them
  (mirrors plan-001 R2 lesson). Guard: separate callers, one shared session
  call, policies visible at each call site.
- **Relocation regressions in the pump**: moving code between compilation
  units can silently change visibility or drop a behavior comment. Mitigation:
  verbatim-move commit step before any body edits; reconnect/fanout suites +
  new tally tests.

## Completion notes (2026-08-21)

Implemented same day; final smells state:

| Item | Before | After | Notes |
| --- | --- | --- | --- |
| `spawn_gateway` | 51 | < 20 | env build → `_build_child_env`; listen check → `_validate_listen`; socket wait → `_wait_for_socket` (20) |
| `teardown` | 46 | < 20 | MD drains via `TopicIntent` (`teardown_topic`); pnl/order split into `teardown_pnl` / `teardown_order_plants` (21); dup block gone |
| reconnect trio | bin, untestable | lib `server/pump.rs` | `restore_intents` 28→22 reusing `TopicIntent::venue_join` (+ `resubscribe_fail_log` keeps strings byte-identical); `run_event_pump` uses fn-pointer poll table; bin is CLI-only |
| restore tallies | untested | 3 unit tests | offline-session tallies incl. brackets-counted-but-skipped rule |

Accepted residue (unchanged): orders.rs mass-134 one-shot twins, parity dups,
scripts/harness, enum mappers, `_handle_order_notification` 33 (closed-set
action chain), `execution.py` total 416. Drive-by: `_resolve_user_bin`
annotation `str | None` (pre-existing latent ty diagnostic outside the
AGENTS.md gate); wheel-data mirror resynced.

Verify evidence: fmt OK; workspace clippy `-D warnings` clean; cargo tests
92 passed / 0 failed (gateway 54 incl. 3 new pump tally tests); ruff
check+format OK; ty widened to `python/rithmic_gateway` all-pass; pytest 376
passed / 75 skipped; STATUS scoreboard check OK.

Pending operator step: gateway shared-ticks smoke
(`python scripts/smoke_gateway_shared_ticks.py --seconds 25`, MotiveWave
closed) as post-merge confidence for the auto-spawn path.
