---
title: "Refactor exec pure classifiers + emit guards: module seams, now_fn injection"
date: 2026-08-21
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
execution: code
origin: session — follow-up to 2026-08-21-001 P2 after the emit-handler extraction landed (267d82d); TDD/DI planning for the remaining execution.py complexity concentrations
baseline: qlty smells --all 2026-08-21 — execution.py file total 416; _handle_order_notification 33; _poll_iteration 20; _handle_untracked_notification 22; generate_fill_reports 22
related: docs/plans/2026-08-21-001-refactor-qlty-smells-dispatch-execution-plan.md, docs/plans/2026-08-21-002-refactor-remaining-smells-plan.md
---

# Refactor exec pure classifiers + emit guards into testable seams

## Goal Capsule

Finish the `execution.py` deepening that plan 001 P2 started, using **TDD at
pre-agreed seams** instead of new abstraction layers:

1. Hoist the five **pure classifiers** (`order_status_from_fields`,
   `order_type_from_fields`, `tif_from_fields`, `trigger_type_from_fields`,
   `is_benign_bare_complete`) from client methods to module-level functions.
   The function *is* the seam — no dependency injection, no interface.
2. Extract the three **decision-bearing emit branches** of
   `_handle_order_notification` (LAP-42 accepted guard, UPDATED terms
   fallback matrix, #3812 triggered guard) into named methods, each with a
   failing-test-first protocol rule.
3. Inject a `now_fn` clock seam into `_recon_window_sec` so window-clamp
   behavior is deterministic under test.
4. Split `_handle_untracked_notification` into status-phase / fill-phase,
   pinning the load-bearing coupling (status-publication failure suppresses
   the fill path).

**DI verdict (decided, do not relitigate):** cache/clock/msgbus/session are
already constructor-injected by Nautilus — do not wrap them. Classification
needs no DI (pure module functions). `generate_*` emitters are not abstracted;
tests spy via subclass. The only new injection is `now_fn`.

Amendment vs plan 001 baseline: `_order_status_from_event` was marked **Keep**
(flat closed-set map). Revised to **Fix-as-pure-module-function**: the body
stays a flat map; only its home moves off the class. No table-driven rewrite,
no dispatch registry.

Stop when: all slices green, `qlty smells --all` shows `_handle_order_notification`
and `_handle_untracked_notification` below complexity 15, full AGENTS.md verify
passes, existing public method signatures unchanged.

## Seams contract (test nowhere else)

| # | Seam | Kind | Test surface |
| --- | --- | --- | --- |
| S1 | Module-level pure classifiers | pure function import | direct call, no fixture |
| S2 | `_emit_accepted`, `_resolve_updated_terms`, `_emit_triggered_guarded` | client methods, explicit args | tiny recorder stub, no stream |
| S3 | `_recon_window_sec(..., now_fn=time.time)` | injected callable | fake clock literals |
| S4 | `_publish_untracked_status` / `_publish_untracked_fill` | private methods behind unchanged `_handle_untracked_notification` | existing recon tests + new coupling test |

Existing signatures that must NOT change (tests call them directly):
`_handle_order_notification`, `_handle_untracked_notification`,
`_plant_poll_loop`, `_order_status_from_event` (kept as one-line delegate).

## Phases

### P1 — Pure classifier hoist (green refactor; tests already exist)

1. Move bodies of `_order_type_from_event`, `_tif_from_event`,
   `_trigger_type_from_event`, `_order_status_from_event`,
   `_is_benign_bare_complete` to module level next to the closed-set maps
   they read (`_RITHMIC_PRICE_TYPE_TO_ORDER_TYPE`, `_TRIGGERABLE_ORDER_TYPES`).
2. Methods remain as one-line delegates (back-compat for
   `tests/test_exec_recon.py` calls).
3. No new tests — the recon/outcome suites already pin every mapping row.
   This slice must never go red.

Verify: `uv run pytest tests/test_exec_recon.py tests/test_execution_outcomes.py -q`

### P2 — Emit guards, red-first (one slice per rule)

Each rule gets ONE failing test against the new method before extraction:

1. **LAP-42** — `_emit_accepted(order, cid, vid, ts)`: order SUBMITTED →
   `generate_order_accepted` recorded; order ACCEPTED → nothing emitted,
   debug log. Test first on the not-yet-existing method (red), then extract
   the branch (green).
2. **UPDATED terms** — `_resolve_updated_terms(order, action) -> (qty, price,
   trigger)`: action value wins; else order field; else None. Four-case table
   test (action+order / action-only / order-only / neither), red-first.
3. **#3812 guard** — `_emit_triggered_guarded(order, ...)`: STOP_LIMIT emits;
   STOP_MARKET suppressed; duplicate TRIGGER on TRIGGERED order suppressed.
   Red-first.
4. Router keeps plain elif dispatch over `action.kind`; filled branch still
   routes to `_handle_tracked_fill`. No dispatch-table framework.

### P3 — `now_fn` injection (red-first)

1. Failing test: ns-epoch `end` clamps to `_MAX_RECON_SPAN_S` using an
   injected fake clock (today this needs monkeypatching `time.time`).
2. Add `now_fn: Callable[[], float] = time.time` parameter to
   `_recon_window_sec`; callers pass nothing (default). No other signature
   changes.

### P4 — Untracked handler split (coupling pinned first)

1. Red test: status-report publication failure ⇒ fill report is NOT attempted
   (early return survives the split).
2. Split into `_publish_untracked_status(fields, ts_event) -> bool` (False =
   stop) and `_publish_untracked_fill(fields, ts_event)`; handler becomes
   identity/seed checks → status phase → fill phase.
3. Dedup stores stay where they are; bulk-clear policy moves verbatim.

### Out of scope (accepted residue)

- `_poll_iteration` regime split (candidate 4): latch/resync docstring
  documents structural guarantees; extraction risk outweighs smell score.
  Revisit only if it grows past ~25.
- `kind_from_notify` Rust↔Python parity test: separate small change, tracked
  here as follow-up note, not in this diff.
- All parity-signature smells (AGENTS.md hard rule).

## Verification (per phase + whole change)

```bash
uv run ruff check .
uv run ruff format --check .
uv run ty check python/rithmic_nt_connect tests
uv run pytest -q
python scripts/status_progress.py --check   # unaffected, run once at end
qlty smells --all                            # targets below cx 15; no NEW smells
```

No Rust crates touched → cargo gates not required unless `python/mod.rs`
delegates are disturbed (they must not be). No live smokes required
(behavior-preserving); optional post-merge confidence:
`scripts/smoke_lucid_nq.py` with MotiveWave closed.

## Risks

- **Exec event loss**: fills/status must never drop. Mitigation: dedup keys
  and unknown-status policy stay in exactly one home; exec suite green per slice.
- **Delegate drift**: `_order_status_from_event` delegate and module function
  could diverge. Mitigation: delegates are single-expression returns; add one
  assertion-style test that both entries agree on a sample row (cheap, in P1).
- **Red-first discipline slipping into bulk tests**: one rule, one test, one
  extraction per cycle — no batch-writing P2 tests up front.
