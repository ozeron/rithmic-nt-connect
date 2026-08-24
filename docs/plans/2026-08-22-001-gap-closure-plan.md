---
title: "Gap-closure plan: close remaining STATUS partials and not-started marks"
date: 2026-08-22
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
execution: code
origin: session — post-merge review of docs/STATUS.md scoreboard (44 done / 24 partial / 2 not-started of 81) after #31 (stop acceptance + gates) and #32 (gateway refactor) landed; Alex asked to inventory what is left and plan closure slice by slice.
baseline: STATUS.md At-a-glance 2026-08-22 — 2 hard `[ ]` (Phase 2 definition updates, Phase 7 recovery suite), 24 `[~]` clustered below, 11 N/A excluded.
related: docs/STATUS.md, docs/references/ops-runbook.md, docs/plans/2026-08-14-001-exec-honesty-account-discovery-dryrun-plan.md, docs/plans/2026-08-21-003-refactor-exec-classifiers-emit-guards-tdd-plan.md
---

# Gap-closure plan

## Goal Capsule

Close the remaining open marks in `docs/STATUS.md` in evidence-cheapest-first
order. Two kinds of work are left:

1. **Evidence runs** — code exists and is unit-tested; only a live proof run
   plus a STATUS mark flip is missing (slices P0–P2).
2. **Real code work** — recovery suite + skipped-spec register (P3) and
   command-failure retry taxonomy (P4). Offline, no creds needed.

**Stop when:** every `[~]` that can be closed has evidence recorded in
STATUS.md notes and its mark flipped; every genuinely parked item is listed
in the Parked register with its blocker named; scoreboard regenerated via
`python scripts/status_progress.py --check`.

## Constraints (every live slice)

- One Rithmic login per system: MotiveWave / R|Trader closed for LucidTrading
  runs; test-account runs use the repo `.env` (`RITHMIC_TEST_DOTENV=.env`).
- Trading off unless explicitly gated; far prices only (BUY below / SELL above
  market for limits; stops derived resting-side per `_derive_trigger`).
- Never `cancel_all_orders` to clean up a smoke order — identity-tagged
  baskets only (`_our_baskets` attribution).
- Every STATUS edit ends with `python scripts/status_progress.py` +
  `--check` so the At-a-glance table cannot drift.

## Slices

### P0 — Cheap evidence runs (bars D + reconnect B)

Goal: one LucidTrading session shape, no production code changes — but the
proofs must land as **mechanism-only e2e tests** in `tests/e2e/`
(auto-skip without creds per existing conftest pattern), not one-off
scripts (AGENTS.md safety rule).

1. **15m/1h/1d EXTERNAL bars live proof.** Parametrize TC-D40
   (`tests/e2e/test_data_client_live.py`) over 1m/15m/1h/1d.
   - Assertion bar: subscription ack + **first bar payload received**
     (in-progress bar counts; do not wait for period rollover).
   - Skip policy: a venue that rejects or never delivers a period is
     recorded as a skipped-spec entry (period + reason + date), not a
     failure. 1m stays the anchor case; its assertions do not weaken.
   - Done when: all four periods pass-or-registered on LucidTrading;
     Phase 5 bullet flips `[x]`; capability row updated.
2. **Ticker resync live proof (close-out 4).** New e2e test
   (`tests/e2e/test_reconnect_live.py`): subscribe ticker+book+1m bars,
   force disconnect **client-side** (drop the session/socket we own),
   reconnect, assert `replay_subscription_intent` re-issues all three
   intents and last-trade resumes within the poll window. Gateway-restart
   variant only if the direct variant passes and time allows — otherwise
   register as follow-up.
   - Done when: proof recorded; Convention "Reconnect restores intent" +
     Phase 3 exit note cite the test; close-out 4 flips `[x]`.

Verify: `RITHMIC_TEST_DOTENV=.env uv run pytest tests/e2e -v -k "tc_d40 or reconnect"`.

### P1 — Exec cluster on the test account (A)

Goal: turn "best-effort" recon language into venue-proven language where the
test account allows it. All proofs go through the **adapter path**
(`tests/e2e/` harness: `OrderDriver` + `ExecHarness` on a real
`TradingNode` with Rithmic exec registered) — not the raw session poll in
`verify_order_dry_run.py`, whose drain is not the code under claim.

1. **Drain-working-orders proof** (new e2e test, live-marked). Place one far
   working LIMIT via OrderDriver → run the exec client's
   `generate_order_status_reports` / `_load_orders_events` drain → assert
   the venue row matches by client order id / basket identity → cancel via
   the node → re-drain and assert it clears. Both drains recorded in the
   report artifact; "clears" keeps best-effort semantics (empty = "no
   working orders seen") and says so.
2. **P5 live canary + MY043 regression watch.** Same harness: place →
   accepted → cancel → canceled, then issue status/fill report queries over
   the window. Capture engine logs and assert **no** WARN regressions:
   no `InvalidStateTrigger` from the bulk drain, no SUBMITTED-branch
   fill-reconciliation warning (`report.avg_px was None`) — the exact MY043
   paths fixed 2026-08-21.
3. **`cancel_all_orders` honesty decision.** Either prove plant-wide scope
   in a test-account run and document it in the runbook, or mark N/A for
   this line in the honesty claim. Do not leave it ambiguous.
4. *(Stretch, only if time allows)* stop-limit / trailing-stop single-shot
   place+cancel to widen the "Order types" row evidence beyond plain limit.

Production `app_name` conformance stays parked (needs Rithmic conformance,
not code).

Done when: STATUS close-out 6 note records dates + artifacts; "Order status
reports" and "Fill reports" rows updated from best-effort wording where
evidence supports it.

### P2 — Brackets accept/survival proof (C)

Goal: prove what the spike already wires, on **both** connect modes — the
AGENTS.md direct ↔ gateway parity rule applies to live capability claims.

0. **Pre-step (offline).** Verify restore capability exists per mode before
   booking any live run: direct reconnect re-issues bracket subscription
   intent; gateway `RestorePlan` carries the brackets bit (it does per
   STATUS). If a path lacks restore, that is new parity work first — do not
   paper over it with a one-sided proof.
1. Run `scripts/spike_bracket_order.py` on the test account
   (`RITHMIC_CONNECT_MODE=direct` then `=gateway`, far trigger): assert
   parent + bracket children accepted in each mode.
2. Survival half: with brackets resting, drop/restart the order plant per
   mode and assert bracket notifications resume (children still working at
   venue) via the restored intent.
3. If either half fails on venue behavior, file the finding as a gap row
   instead of force-flipping the mark.

Done when: capability row "Brackets / OCO" carries accept + survival dates
**per mode**, or an explicit deferral entry naming the deferred mode and why.

### P3 — Recovery suite + skipped-spec register (offline)

Goal: the last hard `[ ]` in Phase 7.

1. **Recovery suite.** Consolidate the scattered crash/reconnect/replay cases
   into one runnable suite under `tests/` (no creds): transport e2e
   reconnect-restore cases, order-plant state-machine transition table,
   drain/re-arm barrier property tests, gateway RestorePlan intent bits.
   Mostly organization + a runner target; add missing cases only where the
   register below names one.
2. **Skipped-spec register.** New section in
   `docs/references/ops-runbook.md` (single home — STATUS only links it)
   listing every known skip with reason: TC-D10 (Lucid L2 permission
   denied), TC-D31/D41/D42 transient-empty history, Test-plant silent
   ticker (why TC-D54 was dropped), A4/OQ1 unsourced constraint, any P0.1
   period the venue refuses. Each entry: what would unblock it.

Done when: Phase 7 "Recovery suite; skipped-spec register" flips `[x]`;
register lives somewhere CI-adjacent reviewers check.

### P4 — Retry taxonomy (offline)

Goal: close the Phase 1 `[~]` ("no command-failure classes yet").

Define closed-set failure classes at the plant boundary (auth, permission,
transient-transport, reject-with-reason, unknown) and map existing error
strings onto them; state-changing RPCs must not auto-retry across classes
where retry is unsafe (place/modify/cancel already follow three-outcome
semantics — align, don't duplicate). Wire through direct + gateway parity
per AGENTS.md hard rule, with framing tests.

Done when: Phase 1 second bullet flips `[x]`; conventions table unaffected
(three-outcome row already `[x]`).

## Disposition of every remaining `[~]`

Each open partial mark maps to exactly one slice or the parked register —
nothing is left unowned.

| STATUS mark | Disposition |
| --- | --- |
| Phase 1 retry taxonomy | P4 |
| Phase 2 exit `[~]` | Parked (definition updates not advertised) |
| Phase 3 exit | Closes with P0.2 (close-out 4) |
| Phase 3 book row | Partial by design; see parked L2 depth |
| Phase 4 PnL bullet | Parked (soft-fail PnL is deliberate; auto-discovery done) |
| Phase 5 EXTERNAL bars | P0.1 |
| Phase 7 exit | Closes when close-outs 1–6 have evidence (P0–P2) |
| Phase 9 recovery docs thin | P3 register + runbook updates land with each slice |
| Capability: account modes | Parked (needs FCM/IB triple for full claim; discovery proven) |
| Capability: EXTERNAL bars | P0.1 |
| Capability: order types | P1 core (limit evidence) + stretch item; production app_name parked |
| Capability: brackets/OCO | P2 |
| Capability: account/positions PnL | Parked (same as Phase 4 bullet) |
| Capability: submit/cancel/modify | P1 |
| Capability: status reports | P1.1 |
| Capability: fill reports | Live-stream-only semantics stand; wording updated in P1 |
| Capability: reconnect/resubscribe | P0.2 |
| Capability: gateway shared login | Residual is Lucid-proof of restore — covered by P0.2/P2 gateway variants |
| Close-out 1 incremental book | Parked (Lucid permission) |
| Close-out 2 execution honesty | P1 (cancel_all decision + drain proof) |
| Close-out 6 exec hardening | P1 (P5 canary); A4 parked on OQ1 |
| Convention: subscribe intent vs confirm | Closes with P0.1/P0.2 proofs |
| Convention: reconnect restores intent | P0.2 |
| Convention: never drop exec events | Suppression-with-log is the designed behavior; wording tightened in P1, no venue case can force an unparseable event — park after review |
| Convention: no empty-list-as-empty | Best-effort drain semantics are permanent; P1.1 records the venue evidence that narrows it |

## Parked register (not planned; blockers named)

| Item | Blocker |
| --- | --- |
| L2 incremental book depth | Lucid denies book permission (TC-D10); summary-only L2 is the advertised surface |
| A4 client-order-id validation | OQ1: Rithmic `user_tag` length/format constraint unsourced |
| Production `app_name` conformance | Needs Rithmic conformance process, not repo work |
| Definition updates stream | Not advertised; stays `[ ]` unless Alex advertises it |
| Native TLS remote | N/A by decision |

## Execution order and rationale

P0 → P1 → P2 → P3 → P4. Evidence runs first while login windows are fresh;
offline suites after, since they have no scheduling constraint. Each slice
lands as its own PR with its own STATUS/scoreboard update; no slice may flip
a mark without the artifact or test named in this plan.

## Execution status (2026-08-24)

Live evidence on Rithmic Test (markets open). Code still on #33/#34/#35 pending
Alex re-review / merge.

| Slice | Code | Evidence |
| --- | --- | --- |
| P0.1 bars | done — TC-D40 parametrized 1m/15m/1h/1d (#33) | **PARTIAL:** Test `D40[1m] PASSED` 2026-08-24; `15m/1h/1d` SKIPPED (no payload in 65s — register). Full four-period close still wants Lucid or longer waits. |
| P0.2 resync | done + `[8]` idempotent replay (#33) | **DONE** 2026-08-24 — `test_reconnect_live.py` PASSED on Test (ticker+book+bars after `resync_ticker_session`) |
| P1 drain E88 | done (#35) | **DONE** 2026-08-24 — far LIMIT drain by identity + post-cancel clear |
| P1 canary E89 | done + canary residual align (#35) | **DONE** 2026-08-24 — no `avg_px was None`; ≤1 engine-residual `InvalidStateTrigger` |
| P1 cancel_all decision | **DONE** — N/A-for-line (#35) | none needed |
| P2 pre-check | **DONE** — direct bracket intent restore (#34) | none needed |
| P2 vehicle | done + post-redial nudge + always-cleanup (#34) | **DONE** 2026-08-24 — accept+survive+cleanup on **direct** and **gateway** (far LIMIT, `scripts/spike_bracket_order.py`) |

Historical blockers (2026-08-22 EOD) that cleared on 2026-08-24:

1. Test order routing — orders now reach exchange (`OPEN_PENDING` → `SENT_TO_EXCH`); keep CME-band prices.
2. Test MD/bars — 1m EXTERNAL + last_trade/bbo flow during market hours; slower bar periods still skip.
3. Resync `[8] already exists` — history-plant bars survive `reset_ticker`; replay treats `[8]` as success.

Still open:

1. LucidTrading env for full P0.1 (15m/1h/1d) and Lucid-hosted proofs — `RITHMIC_ALLOW_LUCID_E2E=1`, MotiveWave closed.
2. Reviews/merges of #33/#34/#35.
