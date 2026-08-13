# Plan: Auto-discover Rithmic FCM / IB / account for Nautilus

Date: 2026-08-13  
Repo: `rithmic-nt-connect`  
Status: proposed  
Related: Phase 2 order plant (`ensure_order_plant`), async_rithmic OrderPlant login-info pattern

## Goal

Nautilus live trading / PnL should work with **user + password + system + gateway** only.  
`RITHMIC_FCM_ID` / `RITHMIC_IB_ID` / `RITHMIC_ACCOUNT_ID` become **optional overrides**, not required secrets the operator must look up in R|Trader.

## Problem

Today `SessionConfig.has_account()` and Rust `ensure_order_plant` refuse to connect the order/PnL plants without the full triple. Those IDs are not credentials — they are **routing identity** that Rithmic already returns after order-plant login:

| Source (async_rithmic / rithmic-rs) | Template | Fields |
| --- | --- | --- |
| `get_login_info` (auto on login) | 300 → 301 | `fcm_id`, `ib_id`, `user_type` |
| `get_account_list` | 302 → 303 | `account_id`, `fcm_id`, `ib_id`, … |

`rithmic-rs` 3.x already does login-info during `RithmicOrderPlantHandle::login` and exposes `get_account_list()`. Our wrapper currently **requires the triple before** calling `get_handle` / `login`, so we never use that discovery path.

## Decision (recommended)

**Optional override everywhere:**

1. If config/env supplies a complete triple → use it (tests, multi-account pin, CI).
2. Else on first order/PnL plant need → connect, login, discover, cache resolved `RithmicAccount` on the session.
3. If multiple accounts and no `account_id` override → fail with a clear list of `account_id`s (same UX as async_rithmic).
4. Partial env (e.g. only `ACCOUNT_ID`) is allowed as a **selector**; FCM/IB still come from login-info / account list.

## Design

### Discovery sequence (Rust session)

```
ensure_order_plant / ensure_pnl_plant
  │
  ├─ if config.account() is Some → current path (unchanged)
  │
  └─ else resolve_account():
       1. RithmicOrderPlant::connect(config)   // credentials only
       2. get_handle(bootstrap_placeholder)  // login wire ignores account fields
       3. handle.login()                     // sets LoginScope from ResponseLoginInfo
       4. handle.get_account_list()
       5. pick account:
            - if config.account_id set → must match one row
            - elif exactly one account → use it
            - else → Config error listing account_ids
       6. fcm/ib := login_scope or account-list row (prefer row if present)
       7. get_handle(real_account); keep that handle for trading/PnL
       8. store resolved account on Session (and optionally surface to Python)
```

**Bootstrap account:** `RithmicAccount::new("", "", "")` (or a dedicated `_BOOTSTRAP` sentinel). Safe because `login()` does not put handle account on the login request; only later place/subscribe calls do. After resolution, **replace** the handle so subscription filters and order payloads carry the real triple.

### Nautilus wiring

| Layer | Change |
| --- | --- |
| `SessionConfig` | Keep optional fields; `has_account()` means “fully specified”; add `account_id` selector semantics |
| Rust `Session` | `resolve_account()` + cache; `ensure_order_plant` / `ensure_pnl_plant` call it |
| PyO3 | Expose `resolved_account()` / `account_id`/`fcm_id`/`ib_id` after connect; discovery triggers on order/PnL ensure |
| `LiveExecClient._connect` | Remove hard `has_account()` gate for `enable_trading`; let session discover; set Nautilus `AccountId` from resolved id |
| PnL path | Same discovery (order plant first is fine; share cached account for PnL handle) |
| Scripts | `verify_order_dry_run.py` / smoke: drop “triple required” fail when creds present |
| Docs / `.env.example` | Mark FCM/IB/ACCOUNT as optional; document multi-account selector |

### Multi-account

- Default: single account → auto-pick.
- Multi: require `RITHMIC_ACCOUNT_ID` (or config) **only as selector** — still no need for FCM/IB in env.
- Optional later: Nautilus config field `account_id: str | None` on `RithmicExecClientConfig` mirroring env.

### Explicit overrides (keep)

Env / config still wins when all three are set. Useful for:

- Deterministic unit tests without wire discovery
- Pinning when login-info is unscoped / admin edge cases
- Operators who already know the triple

## Out of scope

- Changing `rithmic-rs` public API (bootstrap + re-handle is enough)
- Auto-writing discovered IDs back into `.env`
- Placing live orders as part of discovery
- System/gateway discovery (`npm run systems`) — separate from account routing

## Risks

| Risk | Mitigation |
| --- | --- |
| Placeholder handle used for a real place/subscribe | Never leave bootstrap handle LIVE; resolve before `subscribe_order_updates` / place |
| Login-info missing → unscoped account list | Prefer account-list row’s fcm/ib; if still incomplete, error with “set RITHMIC_* overrides” |
| Multi-account silent pick | Never auto-pick when `len > 1` |
| PnL connect before order discovery | Share one `resolve_account()`; either plant may trigger it (order plant is natural owner) |
| Session reconnect | Re-resolve or reuse cached account; prefer cache unless login identity changed |

## Test plan

1. **Unit:** resolve with mocked account-list (1 account → pick; 2 accounts + no selector → error; selector match/miss).
2. **Unit:** explicit triple short-circuits discovery.
3. **Integration / dry-run (creds):** connect order plant with **empty** FCM/IB/ACCOUNT in env; assert dry-run prints resolved ids and completes subscribe without place.
4. **Exec client:** `enable_trading=True` without account env → connect path discovers and sets `AccountId`; no place.
5. Regression: existing tests that pass explicit account still pass.

## Implementation units (suggested order)

1. Rust: `resolve_account` + cache on `Session`; relax `ensure_order_plant` / `ensure_pnl_plant`.
2. Parse `ResponseAccountList` / login-info into DTO; PyO3 getters.
3. Python config/docs/scripts: optional triple; multi-account selector messaging.
4. `LiveExecClient`: discover-on-connect; set Nautilus account from resolution.
5. Tests + dry-run verification on Test or Lucid (no `--live-place`).

## Success criteria

Operator can set only:

```ini
RITHMIC_USER=...
RITHMIC_PASSWORD=...
RITHMIC_SYSTEM_NAME=LucidTrading
RITHMIC_GATEWAY=wss://rprotocol.rithmic.com:443
RITHMIC_ENABLE_TRADING=1   # still gated separately
```

…and Nautilus exec client connects order/PnL plants without manual FCM/IB lookup. Multi-account users add only `RITHMIC_ACCOUNT_ID=...`.
