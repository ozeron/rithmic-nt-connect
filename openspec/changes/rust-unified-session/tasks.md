## 1. Session trait and DirectSession (no behavior change)

- [x] 1.1 Inventory every `WireSession` method and every `#[pymethods]` entry in `rithmic-nt-connect/src/python/mod.rs`; record the list as the seed of the RPC table
- [x] 1.2 Define the `Session` trait (async RPCs, timeout-bounded `poll_*`) in `rithmic-plants` and the separate `HistoryReplay` trait for direct-only fill/order history
- [x] 1.3 Implement `DirectSession` as a wrapper over `RithmicSession`, keeping the process-wide singleton and flock release-on-last-holder behavior
- [x] 1.4 Add the single `PlantEvent` → dict conversion in the extension and route all direct polls through it
- [x] 1.5 Rebase the PyO3 `Session` class on `Box<dyn Session>`; keep `block_on` inside the extension and release the GIL on every blocking call
- [x] 1.6 Verify no behavior change: `cargo clippy -D warnings`, `cargo test`, `pytest -q`, `ty check`, `ruff` all green

## 2. Generic gateway server, fake plant, real-binary harness

- [x] 2.1 Make `GatewayState` and dispatch hold the session as the `Session` trait object instead of `RithmicSession`
- [x] 2.2 Implement `FakeSession` (scripted events, injectable plant drop, injectable error codes, recorded calls)
- [x] 2.3 Add the `rithmic-gateway-fake` bin target behind `required-features = ["testing"]`; confirm no env switch on the production binary selects a fake plant
- [x] 2.4 Build the integration harness: spawn the fake binary in a temp runtime dir, expose a client-agnostic scenario list (subscribe + fan-out, trading gates, order/bracket restore after plant drop, ticker reset, overflow, history refusal under live ticker, transport drop)
- [x] 2.5 Run the harness against the existing Python `GatewayClient` and fix or record any current-behavior surprises before porting
- [x] 2.6 Add a packaging test asserting the wheel contains `rithmic-gateway` and not `rithmic-gateway-fake`

## 3. RPC table and parity tests

- [x] 3.1 Add `RPC_TABLE` (trait method, proto `Body` variant, kind OneShot/SubscriptionIntent/IntentRelease, gate None/Trading/CancelAll)
- [x] 3.2 Test: every `session.proto` request variant and every trait method appears in the table
- [x] 3.3 Test: every SubscriptionIntent row maps to a `RestorePlan` field and a `restore_intents` re-issue (including order and bracket on the order-plant stream)
- [x] 3.4 Test: every Trading-gated row is denied under `ParentGates` with trading off; cancel-all gate independent of trading
- [x] 3.5 Test: Python `WireSession` protocol method set equals the trait's method set (via `_lib.pyi`)
- [x] 3.6 Retire the proto-string-grep parity tests in `framing.rs` once 3.2–3.4 cover them

## 4. Rust GatewaySession

- [x] 4.1 Port attach-only: dial existing socket, flock check, bounded wait for a starting parent, `parent_not_running` error (from `attach.py`, `flock.py`; reuse `singleton`, `runtime_dir`, `env_keys`). Prerequisite: `simplify-gateway-modes` landed
- [x] 4.2 Confirm no spawn or binary-resolution code is ported (it stays in the Python launcher only)
- [x] 4.3 Port handshake: auth token only, fingerprint match, scopes, read-only `trading_enabled` / `cancel_all_enabled`
- [x] 4.4 Implement the single reader task with pending-reply slots and bounded per-channel queues (md, history, pnl, order); surface an overflow disconnect as a transport error (the parent closes the connection; no overflow frame)
- [x] 4.5 Implement single-flight re-dial with transport generation; unknown-outcome error for in-flight state-changing RPCs; at most one retry for read-only RPCs
- [x] 4.6 Implement every trait RPC over the proto; add `pb::Event` → `PlantEvent` conversion (inverse of `convert.rs`)
- [x] 4.7 Implement client-side chunked `load_time_bars_range` with per-slice timeout, bounded slice retry, merge, sort, dedup
- [x] 4.8 Port the behavior of `test_gateway_mux.py`, `test_gateway_runtime.py`, `test_transport_recovery.py`, `test_gateway_attach.py` as Rust tests against the fake binary
- [x] 4.9 Test: identical scripted event stream through Direct and Gateway yields equal dicts
- [x] 4.10 Test: concurrent `poll_event(timeout)` and RPC from multiple threads do not deadlock (GIL released)

## 5. Switch-over behind a temporary selector

- [x] 5.1 Add `RITHMIC_GATEWAY_CLIENT=rust|python` to `create_session` (default `python`); route `rust` through the PyO3 class in gateway mode
- [x] 5.2 Run the shared harness against both the Rust client and the Python lake client; both pass the same scenarios
- [ ] 5.3 Live verification on Rithmic Test, gateway mode, `rust` selector, no live place, MotiveWave / R|Trader closed: `scripts/smoke_gateway_shared_ticks.py`, `tests/e2e/test_reconnect_live.py`, `tests/e2e/test_rms_commission_live.py`. **Partial (2026-10-13):** verified live with the Rust client: attach to a harness-started parent, handshake, typed `trading_disabled` gate, typed Rithmic errors through the order plant (`[13] permission denied` on RMS and order updates, identical for the Python client), recovery from a real parent restart (`tests/e2e/test_gateway_live.py`), `parent_not_running`. NOT verified: tick delivery and the reconnect smoke — the Rithmic Test account has no reference data and fails the history-ready probe even in direct mode. Needs a market-data account.
- [x] 5.4 Flip the default to `rust`. **Decision (2026-10-13, operator):** flip on offline + partial live evidence (5.3 stays open); the `python` selector was NOT kept for a release — 6.1 removed it in the same series (nothing was released with the selector, so no consumer depends on it)

## 6. Remove the Python gateway path from the adapter

- [x] 6.1 Delete `python/rithmic_nt_connect/gateway_wire.py` and the selector; make `create_session` use the PyO3 class for both modes
- [x] 6.2 Remove adapter imports of `rithmic_gateway`; add the "imports without protobuf" test
- [x] 6.3 Confirm no `dual`, spawn, or retired-knob code remains (removed by `simplify-gateway-modes`)
- [x] 6.4 Confirm `python/rithmic_gateway` still imports without Nautilus or the extension and its tests still pass against the harness
- [x] 6.5 Update `_lib.pyi`, `docs/references/gateway-remote.md`, and `scripts/build_wheel.sh` expectations

## 7. Docs and close-out

- [x] 7.1 Rewrite the AGENTS.md "Direct ↔ gateway plant-surface parity" section around the trait and RPC table (keep the RPC-kinds learning)
- [x] 7.2 Update `docs/STATUS.md` session-gateway row and marks; fix the existing scoreboard drift; run `python scripts/status_progress.py` and `--check`
- [x] 7.3 Run the full verification list from AGENTS.md (fmt, clippy, cargo test, ruff, ty, pytest, qlty, status check) and record results. **2026-10-13:** cargo fmt/clippy (workspace, `python`, `testing`) clean; cargo tests all pass; ruff, ruff format, ty clean; pytest 657 passed / 91 skipped; qlty clean; status check OK; `openspec validate --strict` valid
- [x] 7.4 File the follow-up change for moving Nautilus-independent exec logic to Rust (state machine, drain pipeline, dedup, commission, exposure, bar hygiene) with its test-porting requirements
