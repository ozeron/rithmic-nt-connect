# Plan: Close audit gaps — STATUS over/under-marks + missing e2e harnesses

**Date:** 2026-08-24
**Status:** draft → implementation-ready
**Goal:** `audit everything in project against non implemented functionality, what is actual implemented but marked incorrectly, or lack e2e tests but working` (goal `ses_fcb890d00ffeHb2TNn7qyzSBXT`)
**Origin:** `docs/STATUS.md` 82% (47 done +22 partial) + live audit 2026-08-24 (3 parallel subagents)
**Artifact readiness:** implementation-ready
**Product contract source:** ce-plan-bootstrap (audit as origin)

---

## Problem frame

`docs/STATUS.md` claims 82% implemented but audit found 1 over-mark, 1 under-mark, and 3 “working code, missing e2e” seams that make the scoreboard look better/worse than reality. Closing the gaps is either proving the claim with a live `e2e` harness or downgrading the claim to `[~]` so the board is honest.

* `Phase 5 EXTERNAL bars [x]` claims `1m/15m/1h/1d` done — only `1m` ever got a live payload (`TC-D40[1m]` PASS Lucid 2026-08-14 + Test 2026-08-24, others `SKIP`).
* `Phase 7 Recovery suite; skipped-spec register [ ]` claims not started — half is done (`docs/references/ops-runbook.md:54` skipped-spec table), half not.
* Working code with no committed `e2e`: `Account auto-discovery` (`execution.py:665` `resolved_account`), `examples/nq_four_bar.py` + `live_nq_intraday_sandbox.py` paper path, bracket OMS (wire parity both paths but Nautilus `SubmitOrder` loops legs), and history `15m/1h/1d` subscribe path.

---

## Scope

**In:**
- Fix the two STATUS row marks (one line each) + update At-a-glance rollup via `python scripts/status_progress.py`.
- Add the smallest `e2e` that proves the claim or keep the claim at `[~]` — no new product surface.
- Keep behavior unchanged (gated `RITHMIC_ENABLE_TRADING`, `RITHMIC_BRACKETS=1`, `RITHMIC_ALLOW_LUCID_E2E`).

**Out:**
- L3 MBO, mark/funding/greeks, catalog/Parquet, v2 LiveNode, native TLS — `N/A` stays.
- New bracket OMS (`SubmitOrder` → plant bracket) — separate slice, not this plan.
- Perf/robustness Phase 8.

**Success:** `python scripts/status_progress.py --check` passes, `uv run pytest -q` + `cargo test` green, and each changed STATUS row either has a new `tests/e2e` that passes on Test (or is explicitly `SKIP` with venue reason) or is downgraded to `[~]` with a note.

---

## Requirements traceability

| Req | Origin | How closed |
|---|---|---|
| R1 — EXTERNAL bars honesty | `STATUS.md:137` Phase 5 `[x]` vs `data.py:463-518` + `tests/e2e/test_data_client_live.py:211` `TC-D40` only `1m` PASS | U1: either live-prove `15m/1h/1d` or flip row to `[~]` + note. |
| R2 — Phase 7 split | `STATUS.md:148` `[ ]` vs `ops-runbook.md:54` register done, recovery suite not | U2: split row into `[~]` (register `[x]`, suite `[ ]`) or two rows. |
| R3 — Working code without `e2e` | Capability `Account auto-discovery` `[x]`, Paper `nq_four_bar` `[x]`, Brackets `[~]` | U3: add `tests/e2e` discovery/paper-smoke harness or downgrade note to `manual run only`. Chosen: harness for discovery (U3a), keep paper as manual with explicit `SKIP` note (U3b), brackets stay `[~]` spike-only (U3c). |

---

## Decisions

**D1 — Keep paper path `[x]` but label `e2e` gap explicitly.** `examples/nq_four_bar.py:1` and `live_nq_intraday_sandbox.py:68` are correct and have a manual Lucid run (`2026-08-13 NQU6 +2 INTERNAL`), but no `tests/e2e` harness. Rather than downgrading to `[~]`, add a one-line `STATUS` note `e2e: manual smoke only, no committed harness` — preserves `[x]` for code while honest about coverage.

**D2 — EXTERNAL bars: downgrade, not live-prove.** Live-proving `15m/1h/1d` needs an off-hours Lucid window and may still `SKIP` (venue rollover). Cheaper to flip Phase 5 row to `[~]` with note `1m proven, 15m/1h/1d register but no live payload`. If a later Test window yields payload, flip back to `[x]` in one line.

**D3 — Phase 7: split, not new code.** No new recovery `e2e` in this slice; just make the row honest. One row becomes two or one `[~]` with parenthetical `(register [x], suite [ ])`.

**D4 — Discovery `e2e`: reuse `live_session`, not a new TradingNode.** `config.py:357` `has_account` + `execution.py:665` `resolved_account()` + `gateway_wire.py:76` already unit-tested; the missing piece is a live assertion that `resolved_account()` returns the env triple on Test. A 10-line `tests/e2e/test_discovery_live.py` using `live_session` (no trading gate) suffices — no new `ExecHarness`.

---

## Implementation units

### U1 — Fix Phase 5 row over-mark
*Goal:* Make `Phase 5` honest — `[x]` → `[~]`.
*Files:* `docs/STATUS.md:136-138` (Phase 5 EXTERNAL bars row), `docs/STATUS.md:26` At-a-glance (regenerated via `python scripts/status_progress.py`)
*Change:* Flip `Phase 5 [x] Live EXTERNAL time bars…` to `[~]` and add note `1m live-proven Lucid+Test, 15m/1h/1d wired but no live payload (SKIP)`.
*Verification:* `python scripts/status_progress.py --check` `OK`, `tests/e2e/test_data_client_live.py:211` `TC-D40[1m]` `PASS` or `SKIP` (venue), never `FAIL`.
*Depends:* none.

### U2 — Fix Phase 7 under-mark (split register / suite)
*Goal:* Make `Phase 7` honest — `[ ]` → `[~]` (register done, suite not).
*Files:* `docs/STATUS.md:147-148` (Phase 7 rows)
*Change:* Replace ` [ ] Recovery suite; skipped-spec register` with ` [~] Recovery suite [ ] + skipped-spec register [x] (ops-runbook.md:54)` or two rows. Keep `Phase 7 Exit [~]` as is. Regenerate rollup.
*Verification:* `python scripts/status_progress.py --check` `OK`, `uv run ruff check .` pass.
*Depends:* U1 (same file, avoid merge conflict).

### U3a — Add discovery live `e2e` (closes “working but no e2e”)
*Goal:* Prove `Account auto-discovery` `[x]` with a committed live test.
*Files:* `tests/e2e/test_discovery_live.py` (new, 20 lines) — **test file**, `docs/STATUS.md:198` note link
*Change:* `live_session` → `live_session.resolved_account()` is not `None`, matches `RITHMIC_ACCOUNT_ID`/`FCM`/`IB` env triple when set, else auto-discovered account has `account_id`. Marked `pytest.mark.live`, skips without `RITHMIC_TEST_DOTENV`.
*Test scenarios:*
- `test_resolved_account_matches_env` — `live_session.resolved_account()` returns dict with `account_id` and `fcm_id`/`ib_id` matching `explicit_test_env()` when triple set.
- `test_resolved_account_auto_discovers` — when env triple unset, `resolved_account()` still returns one `account_id` (single-account case).
*Verification:* `RITHMIC_TEST_DOTENV=.env uv run pytest tests/e2e/test_discovery_live.py -v` → `2 passed` on Test, `SKIP` in CI without creds (expected, not failure).
*Depends:* none.

### U3b — Clarify paper path note (no new harness)
*Goal:* Keep `Paper path [x]` but make `e2e` gap explicit in docs.
*Files:* `docs/STATUS.md:222-232` Paper path section
*Change:* Add parenthetical to `Live Lucid run` row: `manual smoke 2026-08-13, no committed e2e harness (examples run via scripts/smoke_*)`.
*Verification:* `python scripts/status_progress.py --check` `OK` (docs-only).
*Depends:* U1/U2 (same file).

---

## Test plan (per unit)

*U1:* `uv run pytest tests/e2e/test_data_client_live.py -k D40 -v` → `1m` PASS or `SKIP` (venue), never `FAIL`. `python scripts/status_progress.py --check` → `OK`.
*U2:* `python scripts/status_progress.py --check` → `OK`; `uv run ruff check .` still pass.
*U3a:* `RITHMIC_TEST_DOTENV=.env uv run pytest tests/e2e/test_discovery_live.py -v` → `2 passed` on Test, `SKIP` in CI (no creds is expected, not a failure). No trading gate, so no `RITHMIC_ENABLE_TRADING` needed.

---

## Dependencies / sequencing

`U1` + `U2` + `U3b` all touch `docs/STATUS.md` — do them in one commit or rebase. `U3a` is independent file, can land first. Order: `U3a` → `U1/U2/U3b` together. All depend on `main` `2e5fcea` (BBO/logging flake fix).

---

## Risks

* EXTERNAL `15m/1h/1d` live payload may never arrive on Test off-hours — hence downgrade not live-prove (D2). Re-flip is one line if later proven.
* Discovery `e2e` will `SKIP` in CI and on multi-account envs without selector — document as venue-limited, not failure (same as `TC-D10` book skip).
* `STATUS.md` rollup will move `Phase marks: Done 15→14 or 15→16` depending on flip — run `status_progress.py` not hand-edit At-a-glance.

---

## Verification contract

* `python scripts/status_progress.py --check` → `OK` on every docs-only unit.
* `uv run ruff check .` + `uv run ruff format --check .` + `cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect` + `uv run pytest -q` → green (U1-U3b docs units) or `2 passed` / `SKIP` for live `U3a` on Test.
* Each STATUS row flip has either a new `tests/e2e` that `PASS` on Test or an explicit `SKIP` with venue reason, never `FAIL`.

## Definition of Done

Plan is done when `docs/STATUS.md` At-a-glance is regenerated, all four rows are honest (`Phase 5 [~]`, `Phase 7 [~]`, paper note, discovery note), and `U3a` `test_discovery_live.py` exists with 2 scenarios passing on Test or `SKIP` in CI without `RITHMIC_TEST_DOTENV`.

---

## Out of scope / deferred

* Bracket OMS (`SubmitOrder` → plant bracket) stays `[~]` spike-only — full OMS is a separate slice.
* L3 MBO, mark/funding/greeks, catalog, v2 LiveNode, native TLS — `N/A`.
* Perf Phase 8. Re-proving `15m/1h/1d` EXTERNAL bars is deferred to a later PR if a live window yields payload.
