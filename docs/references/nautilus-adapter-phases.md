# Nautilus adapter phases

Extracted from the upstream
[Adapters developer guide](https://nautilustrader.io/docs/latest/developer_guide/adapters/)
(`origin/develop`). Phases organize **dependencies**, not release gates.

- A market-data-only adapter omits execution.
- An adapter may finish one product before starting another.
- Keep the capability matrix current throughout; do not wait for the docs phase.
- Omit phases and steps that do not apply.

This file is the phase **pattern**. Current marks for this repo live in
[`../STATUS.md`](../STATUS.md).
Conventions (how to implement each phase) live in
[`nautilus-adapter-conventions.md`](nautilus-adapter-conventions.md).

---

## Phase 0: Define scope

| Step | Component | Work |
| --- | --- | --- |
| 0.1 | Capability matrix | Products, environments, account modes, data types, order types, reports in scope |
| 0.2 | Venue constraints | Restrictions, unsupported capabilities, testnet / environment differences |
| 0.3 | Protocol boundaries | Separate product APIs, public vs private, transports |
| 0.4 | Initial slice | Smallest end-to-end path |
| 0.5 | Repository wiring | Add the crate to each build surface that owns it; project only what you expose |

**Exit:** Integration docs have an initial capability matrix, known gaps, and a test plan.

## Phase 1: Build the protocol core

| Step | Component | Work |
| --- | --- | --- |
| 1.1–1.4 | HTTP (if any) | Error types, client, models, deterministic parse |
| 1.5–1.8 | Stream / WS (or plants) | Errors, lifecycle, auth, heartbeat, frames, decode-once, route by typed identity |
| 1.9 | Protocol tests | Fixtures, canonical requests, signing vectors if any, mock or controlled exchange |

**Exit:** Crate compiles; protocol fixtures parse; applicable signing vectors pass; mock or controlled auth works.

## Phase 2: Implement instruments

| Step | Component | Work |
| --- | --- | --- |
| 2.1 | Parsing | Every supported family: identity, precision, currency, contract fields |
| 2.2 | Loading | Load, filter, cache, emit at each parsing boundary that needs context |
| 2.3 | Symbol mapping | Bidirectional venue symbol ↔ `InstrumentId`; never collapse distinct instruments |
| 2.4 | Updates | Fresh requests and any supported definition / status updates |

**Exit:** Fixtures cover every advertised family; invalid definitions fail clearly; complete Nautilus instruments emitted.

## Phase 3: Implement market data

Start with **one public stream and one instrument** before fan-out.

| Step | Component | Work |
| --- | --- | --- |
| 3.1 | Live streams | Subscribe and unsubscribe each advertised type; preserve subscription intent |
| 3.2 | Historical requests | Bars, trades, quotes, or book snapshots with correlation and freshness |
| 3.3 | Data client | Requests, subscriptions, lifecycle, complete domain events |
| 3.4 | Order book | Snapshot, incremental, sequence, clear, batch boundaries |
| 3.5 | Stream recovery | Malformed input, gaps, unsubscribe, disconnect, reconnect, subscription replay |

**Exit:** Unit / mock-transport tests prove complete domain events for the advertised matrix.

## Phase 4: Implement execution

Establish account state and reconciliation **before** order flow.

| Step | Component | Work |
| --- | --- | --- |
| 4.1 | Account bootstrap | Identity, initial account state, private subscriptions, connected readiness |
| 4.2 | Reconciliation reports | Order, fill, position, mass-status at startup and on demand |
| 4.3 | Submit | Supported market / limit with deterministic local validation |
| 4.4 | Modify / cancel | Including cancel-replace venue semantics when that is how the venue works |
| 4.5 | Execution client | Commands, lifecycle, tracked vs external routing, ordered events |
| 4.6 | Outcome recovery | Unknown outcomes, fill dedup, resolve via stream / query / reconcile |

**Exit:** Mock tests cover supported commands, definitive rejection, uncertain transmission, duplicates / OOO, startup reconciliation.

## Phase 5: Optional venue capabilities

Only after the base lifecycle is stable.

| Step | Component | Work |
| --- | --- | --- |
| 5.1 | Advanced orders | Conditional, stop, TP/SL, trailing, etc. |
| 5.2 | Batch / mass | Per-order results; do not treat a whole-request fail as every child failed |
| 5.3 | Venue-specific data | Funding, greeks, liquidations, depth-by-order — separate slices |
| 5.4 | Splits | Split clients only when protocol, auth, quota, or recovery requires it |
| 5.5 | Proof | Fixtures, functional tests, acceptance, documented limitations |

**Exit:** Each optional capability is independently testable and does not weaken base paths.

## Phase 6: Factories and projection

| Step | Component | Work |
| --- | --- | --- |
| 6.1 | Configs | Typed data and exec configs, defaults, env fallback, secret redaction |
| 6.2 | Factories | Required clock / cache inputs for the **supported runtime seam** |
| 6.3 | Registration | In-tree PyO3 registry **or** out-of-tree `TradingNode` factories |
| 6.4 | Python package | Public package and boundary tests for exposed capabilities |
| 6.5 | Stubs | Only when the support line generates `.pyi` from Rust |

**Exit:** Factory / boundary tests pass; package imports resolve; generated output matches inputs when applicable.

## Phase 7: Prove conformance

| Step | Component | Work |
| --- | --- | --- |
| 7.1 | Unit | Parsers, serializers, symbols, signatures, state, malformed input |
| 7.2 | Integration | Public boundaries against deterministic mock transports |
| 7.3 | Python boundary | Imports, config, factories, conversion, representative async calls |
| 7.4 | Acceptance | Applicable `DataTester` / `ExecTester` on testnet or a controlled account |
| 7.5 | Recovery | Disconnect, reconnect, shutdown, rate limits, state recovery |
| 7.6 | Spec gaps | Every skipped spec case with a venue or capability reason |

**Exit:** Applicable [data](https://nautilustrader.io/docs/latest/developer_guide/spec_data_testing/) and [execution](https://nautilustrader.io/docs/latest/developer_guide/spec_exec_testing/) specs pass; advertised capabilities have evidence.

## Phase 8: Performance and robustness

| Step | Component | Work |
| --- | --- | --- |
| 8.1 | Canonical benches | Confirmed end-to-end data / exec hot paths |
| 8.2 | Microbenches | Signing / hash / codec only when they matter |
| 8.3 | Fuzz | Untrusted parse, decode, normalize, sign, encode |
| 8.4 | Invariants | Stronger than panic freedom |

**Exit:** Suites run with representative fixtures and documented invariants. Omit categories the adapter does not use.

## Phase 9: Documentation and operations

| Step | Component | Work |
| --- | --- | --- |
| 9.1 | Capability matrix | Reconcile every claim and exception with the tested implementation |
| 9.2 | Integration guide | Credentials, config, limits, reconciliation, env differences, gaps |
| 9.3 | Testers | Safe defaults (dry-run / no live place) |
| 9.4 | Operations | Recovery, troubleshooting, venue behavior operators must know |
| 9.5 | Verify | Links, examples, generated output if any |

**Exit:** A user can configure, test, operate, and diagnose without reading source.

---

## Applying this repo (out-of-tree 1.231.x)

| Upstream step | Here |
| --- | --- |
| 0.5 / 6.2–6.5 in-tree wiring | **N/A** — not a Nautilus monorepo crate. Seam is `TradingNode.add_*_client_factory`. |
| Phase 1 HTTP | **N/A** — Rithmic plants, not REST HMAC. |
| Phase 1 WebSocket | Map to ticker / history / PnL / order **plants**. |
| Phase 6 PyO3 registry / `make py-stubs` | **N/A**. Our PyO3 is `rithmic_nt_connect._lib`. |
| Phase 8 | Defer until advertised hot paths are closed. |

Do not invent a second phase numbering. Status marks use **0–9** as above.
