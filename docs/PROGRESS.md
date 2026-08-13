# rithmic-connect — functionality checklist

Last updated: 2026-08-13  
Repo: https://github.com/ozeron/rithmic-connect  

Legend: **Done (main)** · **In PR** · **Partial** · **Not started** · **Out of scope / other repo**

---

## Shipping status (PRs)

| PR | Topic | State |
|----|--------|--------|
| #1 | Phase 1 adapter (MD + read-only PnL) | **Merged** |
| #3 | Positions, depth tests, live NQ example | **Merged** |
| #4–#6 | CI + history bars + verify | **Merged** |
| (next) | Phase 2 order routing | **In progress** (`feat/phase2-order-routing`) |

---

## Core / wire (Rust + PyO3)

| Item | Status | Notes |
|------|--------|--------|
| LucidTrading session config / env mapping | **Done (main)** | MY046-compatible |
| Ticker / history / PnL plants | **Done (main)** | |
| History ticks + time bars (`*_all`, sorted) | **Done (main)** | |
| Front month + reference data | **Done (main)** | |
| Order plant connect / subscribe / place / cancel / modify | **In PR** | Phase 2; gated by account |
| Depth-by-order | **Not started** | Entitlement-dependent |
| Reconnect + auto-resubscribe | **Not started** | |

---

## Nautilus adapter (1.231.x)

| Item | Status | Notes |
|------|--------|--------|
| Data client (ticks/quotes/book/history request) | **Done (main)** | |
| Instrument provider | **Done (main)** | |
| Read-only exec (account/PnL) | **Done (main)** | `enable_trading=False` default |
| Trading exec (submit/cancel/modify + fills) | **In PR** | `RITHMIC_ENABLE_TRADING` / `enable_trading=True` |
| Live venue EXTERNAL bars | **Not started** | History/request only |
| Catalog / Parquet downloader | **Not started** | Lake owns long history |

---

## Verify / quality

| Item | Status | Notes |
|------|--------|--------|
| LucidTrading MD smoke | **Done (main)** | |
| Live↔history verify | **Done (main)** | |
| Order-plant dry-run harness | **In PR** | `scripts/verify_order_dry_run.py` — **do not** `--live-place` yet |
| Unit tests (cargo + pytest) | **Done / expanding** | |

---

## Suggested next priorities

1. Merge Phase 2 PR after dry-run verify on LucidTrading (no live place).
2. Confirm conformance / `app_name` before any `--live-place`.
3. Optional: brackets / OCO, symbology flag, reconnect.
