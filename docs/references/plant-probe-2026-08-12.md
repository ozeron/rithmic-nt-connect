# Live plant probe — 2026-08-12

Probe run during brainstorming against MY046 LucidTrading credentials via `async_rithmic` 1.6.5.
**No orders were placed.** Secrets redacted.

## Connection

| Plant | Connected |
| --- | --- |
| ticker | yes |
| order | yes |
| pnl | yes |
| history | yes |

System: `LucidTrading`  
Gateway: `wss://rprotocol.rithmic.com:443`

## Account / PnL (Phase 1 best-effort — confirmed)

- `list_accounts` → 1 account (currency USD; auto-liquidate enabled)
- `subscribe_to_pnl_updates` → account PnL snapshot received (balance/margin/day PnL fields present)
- `list_positions` → returned instrument PnL snapshot sample including `MNQU6` / CME Future
- `get_account_rms` → OK (1 entry)

## Implication

Phase 1 can include **best-effort account/positions** on this plant without waiting for order-routing conformance.
Order **plant reachability** ≠ order **submit authorization**; Phase 2 still gates live trading.
