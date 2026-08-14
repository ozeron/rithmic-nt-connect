---
title: "Close-outs 2 / 3 / 5 — execution honesty, account discovery, Lucid dry-run record"
date: 2026-08-14
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
product_contract_source: docs/STATUS.md close-outs 2, 3, 5 + docs/plans/2026-08-13-001-account-auto-discovery-plan.md
execution: code
origin: docs/STATUS.md
supersedes_partial: docs/plans/2026-08-13-001-account-auto-discovery-plan.md
reviewed: gap-fix 2026-08-14
---

# Close-outs 2 / 3 / 5 — execution honesty, account discovery, Lucid dry-run record

## Goal Capsule

Make the Nautilus execution path **convention-honest**, remove the manual FCM/IB/account triple as a hard gate (direct **and** gateway), and **record** a LucidTrading order-plant dry-run as conformance evidence — without enabling live place.

Authority (highest first): `docs/references/nautilus-adapter-conventions.md` (exec outcomes / tracked vs external) → `docs/STATUS.md` close-outs 2–3–5 → this plan → older account-discovery plan (design absorbed; do not fork a second account story).

Stop when: convention marks for three outcome classes / tracked-vs-external / fill dedup / never-drop are `[x]` or explicitly deferred with reason; operators can connect order/PnL with user+password+system+gateway only (multi-account needs selector) in both `connect_mode`s; STATUS close-out 5 cites a dated Lucid dry-run artifact; `cargo test` + `pytest -q` green; no `--live-place`; place/cancel/modify are not blindly retried.

## Product Contract

### Problem

STATUS advertises gated submit/cancel/modify, but the live exec client still:

- emits **venue `OrderRejected`** for pre-send submit local denies and for many post-send transport failures (gateway path only partially treats unknown; direct mode almost always rejects);
- uses local `*_rejected` correctly for modify/cancel gates today, but post-send modify/cancel still reject on non-gateway exceptions;
- **drops** untracked order notifications (debug/warning return — no reports);
- has **no fill dedup** by venue trade id;
- refuses order/PnL plants without a full env triple even though Rithmic returns that routing identity after login;
- `rithmic-gateway` starts `PlantSet::MARKET_DATA` when the env triple is empty, so gateway mode cannot discover accounts on ensure today;
- has a dry-run harness and an old `artifacts/order-dry-run.json`, but Phase 7 still says the Lucid run is **not recorded** in STATUS.

### Requirements

- R1. **Submit** pre-send local failures (trading disabled, plant not ready, map/route errors, parent gateway trading off) emit **`generate_order_denied`**, never `OrderRejected` / never `OrderSubmitted`.
- R1b. **Modify/cancel** pre-send local failures keep command-scoped **`generate_order_modify_rejected` / `generate_order_cancel_rejected`** (conventions: matching reject attributable to that command — not `OrderDenied`).
- R2. After a state-changing send has been attempted (`OrderSubmitted` for place; after `to_thread` invoke starts for modify/cancel), **any** exception is **unknown** (in flight): warn + leave state; never terminal reject/cancel-reject/modify-reject from that evidence. Definitive venue results come only from structured notifications. Do not inventory every PyO3/`RuntimeError` shape — default unknown.
- R3. Tracked notifications continue to emit typed domain events. Untracked but parseable notifications emit reports via **`_send_order_status_report` / `_send_fill_report`**; never invent strategy/client identity. Unparseable untracked → slim error-log and treat as **suppressed** (STATUS note); not a typed event.
- R4. Fills are deduped before `generate_order_filled` / fill reports using a stable key from `trade_id_from_fill_fields` (and account/instrument when the venue id is not globally unique).
- R5. Fill query stays `VenueQueryUnavailable`; cache-backed order status reports stay honest (not a venue snapshot).
- R6. Order/PnL plants work with user + password + system + gateway only in **direct and gateway** modes; resolution rules: (a) full FCM/IB/account triple override, (b) `account_id` selector only (FCM/IB from login-info / account list), or (c) auto-pick when exactly one account. Multi-account with no selector → clear error listing `account_id`s.
- R7. LucidTrading dry-run (`scripts/verify_order_dry_run.py --seconds 5`, no `--live-place`) is recorded in STATUS (date + artifact path + pass criteria). May run with an explicit triple **before** discovery lands; stronger artifact (resolved ids without env triple) after U3/U4. Live place remains gated on authorized `app_name`.
- R8. Place / cancel / modify are not blindly retried (verify no retry wrapper; document if any idempotent read-only retry remains).
- R9. `cancel_all_orders` stays plant-wide / log-on-failure; **out of honesty claim** (do not advertise as convention-complete).

### Scope boundary

In scope: Python `execution.py` / `_orders.py` honesty; Rust `rithmic-plants` account resolve + ensure paths; gateway parent ensure/fingerprint/PlantSet so discovery works under `connect_mode=gateway`; PyO3 surface for resolved account; config/docs/scripts for optional triple + selector; dry-run harness + STATUS evidence.

Out of scope: live place / authorized `app_name`; brackets/OCO; fill snapshot API; ticker resync Lucid proof (close-out 4); EXTERNAL bar Lucid proof; incremental L2; gateway remote TLS; rewriting `cancel_all_orders` semantics; persisting discovered ids into `.env`.

### Acceptance examples

- Submit pre-send: `enable_trading=False` → `OrderDenied`, never submitted.
- Modify pre-send: trading disabled → `OrderModifyRejected` (not deny).
- Post-send place/modify/cancel: any exception (gateway timeout, channel lag, bare `RuntimeError`) → warning, in-flight, no terminal reject from that path.
- Untracked fill with basket + symbol + fill fields → `_send_fill_report`; duplicate dedup key suppressed.
- Unparseable untracked → error-log only (suppressed); STATUS notes the case.
- Empty FCM/IB/ACCOUNT, single Lucid account, `connect_mode=direct` → order plant connects; dry-run prints resolved ids.
- Same with `connect_mode=gateway` (parent discovers on ensure_order / shared cache).
- `RITHMIC_ACCOUNT_ID` selector with multi-account list → pins that row; FCM/IB from list/login-info.
- Two accounts, no selector → config error listing ids.
- STATUS close-out 5 checked with dated artifact under `artifacts/`.

## Planning Contract

### Key technical decisions

| ID | Decision | Rationale |
| --- | --- | --- |
| KTD1 | `generate_order_denied` for **submit** pre-send only; modify/cancel keep local `*_rejected` | Matches conventions three-class table |
| KTD2 | After send attempt: **all** exceptions → unknown (warn + return). No post-send `OrderRejected` / modify-reject / cancel-reject from exception handlers | Prefer false-unknown; venue reject only from notification classifier; direct mode has no reliable “not transmitted” proof after `to_thread` |
| KTD3 | Untracked → build reports and call `_send_order_status_report` / `_send_fill_report`; else slim error-log = suppressed | Named 1.231 APIs; inventing strategy is forbidden |
| KTD4 | Dedup key = `trade_id_from_fill_fields(...)` optionally prefixed with account + instrument; set on exec client; **retain across disconnect** (bounded LRU) so reconnect snapshot replays stay idempotent; mark only after successful emit | Reuses existing helper; conventions require extra dimensions when venue id is weak |
| KTD5 | `RithmicSession::resolve_account` + cache is the wire owner; gateway `ensure_order` / PnL paths must call it; update fingerprint / PlantSet assumptions so empty env triple does not permanently strand MARKET_DATA-only | R6 fails in gateway without parent participation |
| KTD6 | Honesty (U1–U2) first; discovery (U3–U4) next; dry-run record (U5) **anytime** creds allow (triple OK early; discovery-free stronger later) | Close-out 5 is evidence, not blocked on 3 |
| KTD7 | Close-out 5 is ops evidence + STATUS stamp | Harness exists; gap is recording |
| KTD8 | PnL: attempt resolve + subscribe when soft path desired — do not gate solely on `has_account()`; keep `soft_fail_pnl` | R6 includes PnL readiness without manual triple |
| KTD9 | Reconnect: reuse cached resolved account unless login identity changed; never leave bootstrap handle LIVE | From 001 risks; required for plant drop / gateway restore |
| KTD10 | Tests: pure helpers + thin client with mock `WireSession` / monkeypatched methods; avoid full `TradingNode` for U1/U2 | Almost no exec-client tests today |

### Current gaps (code breadcrumbs)

- `python/rithmic_nt_connect/execution.py`: submit pre-send still `generate_order_rejected`; post-send only `_is_unknown_gateway` (place/modify/cancel); `_handle_order_notification` returns on unknown client id; no `_seen_trade_ids`; PnL gated on `has_account()`; `_set_account_id` already used on PnL/order fields.
- `python/rithmic_nt_connect/errors.py`: `CHANNEL_ERRORS` unused in submit path (superseded by KTD2 all-unknown after send).
- `python/rithmic_nt_connect/_orders.py`: `trade_id_from_fill_fields` exists; not used for dedup set.
- `crates/rithmic-plants/src/session.rs`: `ensure_order_plant` / PnL require `config.account()`.
- `crates/rithmic-gateway/src/bin/rithmic_gateway.rs`: empty triple → `PlantSet::MARKET_DATA`; fingerprint account fields empty.
- `scripts/verify_order_dry_run.py`: exits 2 without `has_account()`; needs `RITHMIC_CONNECT_MODE`.
- `artifacts/order-dry-run.json`: prior Lucid dry-run exists but STATUS Phase 7 unmarked.
- `tests/`: no outcome/dedup coverage for `RithmicExecutionClient` (only `test_exec_readonly.py` alias check).

### Assumptions

- Lucid login available for U5; MotiveWave closed; one login session.
- Single-account Lucid users are the common auto-pick case.
- `rithmic-rs` login-info + `get_account_list` usable without a real account on the login request (per 001).
- `_send_order_status_report` / `_send_fill_report` remain the correct 1.231 injection points for external reports.

### Open questions

- Deferred: instrument resolve for untracked when only symbol/exchange present — best-effort from cache; skip (suppress) if missing.
- Deferred: persist discovered account to `.env`.
- Deferred: whether gateway fingerprint should advertise resolved account to new clients after discovery (prefer yes once cached; implement in U3 gateway slice).

### Sequencing

```
U1 submit deny + post-send unknown (place/modify/cancel)
    → U2 untracked reports + fill dedup
        → U3 Rust resolve_account + ensure_* (+ gateway parent)
            → U4 Python/config/exec connect + PnL + scripts
U5 Lucid dry-run record + STATUS   ← parallel anytime creds exist
```

U1–U2 need no live creds. U5 may run with an explicit triple before U3/U4; re-run optional after discovery for a stronger artifact.

## Implementation Units

### U1. Submit deny + post-send unknown (place / modify / cancel)

Goal: three evidence classes on command paths; modify/cancel local rejects unchanged in kind.

Files: `python/rithmic_nt_connect/execution.py`, `tests/test_execution_outcomes.py` (new; mock session)

Approach:

- Submit pre-send local failures → `generate_order_denied` (trading disabled, plant policy, map/route, parent `trading_enabled is False`). `_submit_order_list` inherits the same per-order path.
- Modify/cancel pre-send local failures → keep `generate_order_modify_rejected` / `generate_order_cancel_rejected`.
- Replace post-send handlers: after send attempted, **any** `Exception` → warn + return (unknown). Remove “else reject” branches for place/modify/cancel.
- Do **not** change `cancel_all_orders` beyond leaving it log-only (R9).
- Spot-check: no blind retry around place/cancel/modify (R8).

Test scenarios:

- Trading disabled submit → denied, no submitted.
- Map error before submit → denied.
- Trading disabled modify → modify_rejected (not denied).
- After submitted, `GatewayError(timeout)` / `ChannelLaggedError` / bare `RuntimeError` → no rejected; tag mapping retained.
- After modify/cancel send, same exception classes → no modify/cancel rejected from exception path.
- Venue notification reject still → `generate_order_rejected`.

### U2. Untracked reports + fill dedup

Goal: parseable untracked → reports; fills deduped; unparseable → suppressed log.

Files: `python/rithmic_nt_connect/execution.py`, `python/rithmic_nt_connect/_orders.py` (helpers only if needed), `tests/test_execution_outcomes.py` / `tests/test_orders.py`

Approach:

- When `_resolve_client_order_id` is None: if fields support status/fill report construction (instrument in cache or resolvable, venue id, required fill fields), call `_send_order_status_report` / `_send_fill_report`; else slim error-log (suppressed).
- Dedup: key from `trade_id_from_fill_fields` + account/instrument when available; skip duplicate on tracked fill and report paths; insert key only after successful emit.
- Keep `generate_fill_reports` raising `VenueQueryUnavailable`.

Test scenarios:

- Untracked accept/fill with basket + symbol → report sent; no typed strategy event.
- Duplicate dedup key → second fill suppressed.
- Tracked fill still generates `order_filled` once.
- Unparseable untracked → no report, error-log (assert no crash).

### U3. Rust account auto-discovery (+ gateway)

Goal: `ensure_order_plant` / `ensure_pnl_plant` resolve when triple absent; gateway parent can ensure order after discovery.

Files: `crates/rithmic-plants/src/session.rs`, `crates/rithmic-plants/src/config.rs`, `crates/rithmic-plants/tests/…`, `crates/rithmic-gateway/src/bin/rithmic_gateway.rs` and/or `server/dispatch.rs` (ensure_order path), `crates/rithmic-nt-connect/src/python/mod.rs`

Approach:

- Follow `docs/plans/2026-08-13-001-account-auto-discovery-plan.md` sequence (bootstrap → login → account list → pick → replace handle → cache).
- Selection: full triple short-circuit; else `account_id` selector; else single-account auto-pick; else error listing ids.
- Prefer account-list row fcm/ib; reconnect reuses cache unless login identity changed; never leave bootstrap handle LIVE.
- Gateway: empty env triple must not permanently prevent order ensure — discovery on `ensure_order_plant` (lazy) even if initial `PlantSet` was MARKET_DATA; update fingerprint account fields after resolve when safe for handshake expectations.

Test scenarios:

- Unit: one account → pick; two + no selector → error; selector match/miss; explicit triple skips discovery.
- Unit/integration: ensure_order after empty config account succeeds when list returns one row.
- Gateway-focused: ensure_order with empty parent env account fields triggers resolve (mock or harness).

### U4. Python surface + exec connect + PnL + harness

Goal: Nautilus and scripts use discovery; PnL not gated solely on `has_account()`; docs mark triple optional.

Files: `python/rithmic_nt_connect/config.py`, `python/rithmic_nt_connect/execution.py`, `scripts/verify_order_dry_run.py`, `.env.example`, `docs/references/ops-runbook.md`, `docs/STATUS.md`, `AGENTS.md` if identity checklist changes

Approach:

- `has_account()` = fully specified override; selector-only fields remain partial.
- Exec `_connect`: drop hard `enable_trading` triple gate; after ensure, `_set_account_id(AccountId(f"{VENUE}-{resolved}"))` (same pattern as PnL path).
- PnL: attempt subscribe after resolve (or shared ensure); keep `soft_fail_pnl`.
- Dry-run: remove fail-on-missing-triple; print resolved account; still refuse `--live-place` without gates; require `RITHMIC_CONNECT_MODE`.

Test scenarios:

- Exec connect with empty triple + mocked session resolve → `_set_account_id` called / `account_id` set.
- PnL path attempted without `has_account()` when soft_fail allows.
- Dry-run no longer returns 2 solely for missing triple.

### U5. Record Lucid order dry-run

Goal: STATUS close-out 5 evidence (may run early).

Files: `artifacts/` (new dated JSON), `docs/STATUS.md`, optionally `docs/references/ops-runbook.md` one-liner

Approach:

- MotiveWave closed, `RITHMIC_CONNECT_MODE` set, creds loaded: `python scripts/verify_order_dry_run.py --seconds 5 --out artifacts/order-dry-run-YYYYMMDD.json` (no `--live-place`).
- Minimum pass: exit 0; `mode=dry_run`; `placed=false`; order plant subscribe succeeded (explicit triple OK).
- Stronger pass (after U3/U4): resolved account fields present with empty FCM/IB/ACCOUNT env.
- Update STATUS close-out 5 + Phase 7 order dry-run row with date and artifact path.

Test scenarios: live ops checklist only (exit 2 if no creds OK for CI).

## Verification Contract

```bash
cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect
uv run pytest -q
# U5 anytime with Lucid creds (MotiveWave closed):
uv run python scripts/verify_order_dry_run.py --seconds 5 --out artifacts/order-dry-run-$(date +%Y%m%d).json
```

No `--live-place`. No `cancel_all_orders` for cleanup.

## Definition of Done

- Global: R1–R9 satisfied; STATUS matrix / convention marks / close-outs 2, 3, 5 updated; AGENTS checklist rows that apply can be marked; tests green; no blind place/cancel/modify retry.
- U1: submit deny + post-send all-unknown covered; modify/cancel local reject kinds preserved; `cancel_all` unchanged/out of claim.
- U2: `_send_*_report` path covered; dedup via `trade_id_from_fill_fields` (+ account/instrument); unparseable → suppressed log.
- U3: resolve_account unit coverage; ensure_* without triple; gateway ensure path participates.
- U4: docs/env/scripts aligned; exec + PnL connect without env triple; `_set_account_id` from resolved id.
- U5: dated artifact + STATUS citation; `placed=false`.

## Appendix

### Relationship to prior plan

`docs/plans/2026-08-13-001-account-auto-discovery-plan.md` remains the detailed discovery design note. This plan is the **execution umbrella** for STATUS close-outs 2+3+5; implement U3/U4 per 001’s sequence and risks table rather than inventing a second discovery protocol. Gateway participation is an umbrella addition required for R6 under `connect_mode=gateway`.

### Gap-fix changelog (2026-08-14)

- Split deny vs modify/cancel local reject (R1/R1b, KTD1).
- Post-send: all exceptions unknown (KTD2); direct-mode inventory dropped.
- Named `_send_order_status_report` / `_send_fill_report` (KTD3).
- Dedup reuses `trade_id_from_fill_fields` + account/instrument (KTD4).
- Gateway discovery + fingerprint/PlantSet (KTD5, U3).
- PnL not solely `has_account()` (KTD8); reconnect cache (KTD9).
- Test strategy mock session (KTD10).
- U5 parallel / early-record allowed (KTD6).
- R8 no-retry; R9 `cancel_all` out of claim; `_submit_order_list` inherits submit path.
- Suppressed vs never-drop clarified for unparseable untracked.

### Safety

- One Rithmic login; trading off unless `enable_trading` / `RITHMIC_ENABLE_TRADING=1`.
- Dry-run record must not place.
- Secrets stay out of reports, artifacts event slim-views, and logs.
