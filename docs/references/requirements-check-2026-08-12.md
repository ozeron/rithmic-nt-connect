# Requirements check — 2026-08-12

Review of `docs/plans/2026-08-12-001-feature-rithmic-nt-adapter-plan.md` Product Contract against Nautilus 1.231 adapter APIs and `rithmic-rs` 3.x.

## Findings applied into the plan

| ID | Severity | Finding | Resolution |
| --- | --- | --- | --- |
| RQ-1 | **P0** | R7 said a data client would surface account/positions into Nautilus account pathways. On 1.231.x, account state is published via `ExecutionClient.generate_account_state` (exec path), not `LiveDataClient._handle_data`. | Amended R2/R7/F4/AE3: Phase 1 adds a **read-only** `LiveExecutionClient` that never submits orders (still governed by R9). |
| RQ-2 | P1 | R2 only mentioned data factories; account path needs an exec factory registration on `TradingNode`. | R2 updated to require both factories when account path is enabled. |
| RQ-3 | P2 | “Full DOM” underspecified vs `rithmic-rs` (OrderBook summary vs depth-by-order). | KTD + R6 clarified: Phase 1 maps LastTrade+BBO+OrderBook summary first; depth-by-order when entitled. |
| RQ-4 | P2 | LucidTrading is not `rithmic-rs` default Live system (`Rithmic 01`). | Planning assumes `RITHMIC_LIVE_SYSTEM_NAME=LucidTrading` (or config equivalent). |
| RQ-5 | Info | `rithmic-rs` covers ticker/history/pnl for Phase 1; order plant unused. Protos not redistributed (generated code in crate). | Closes Q1 favorably; keep R12. |

## Residual (non-blocking)

- Exact Cython type construction path (`from_dict` vs pyo3 capsule) left to implementation unit verification.
- Live smoke remains optional/credentials-gated in CI.
