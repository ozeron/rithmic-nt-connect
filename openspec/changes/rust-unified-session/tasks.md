## 1. Session trait and DirectSession (no behavior change)

- [x] 1.1 Inventory every `WireSession` method and every `#[pymethods]` entry in `rithmic-nt-connect/src/python/mod.rs`; record the list as the seed of the RPC table
- [x] 1.2 Define the `Session` trait (async RPCs, timeout-bounded `poll_*`) in `rithmic-plants` and the separate `HistoryReplay` trait for direct-only fill/order history
- [x] 1.3 Implement `DirectSession` as a wrapper over `RithmicSession`, keeping the process-wide singleton and flock release-on-last-holder behavior
- [x] 1.4 Add the single `PlantEvent` → dict conversion in the extension and route all direct polls through it
- [x] 1.5 Rebase the PyO3 `Session` class on `Box<dyn Session>`; keep `block_on` inside the extension and release the GIL on every blocking call
- [x] 1.6 Verify no behavior change: `cargo clippy -D warnings`, `cargo test`, `pytest -q`, `ty check`, `ruff` all green

## 2. Generic gateway server, fake plant, real-binary harness

- [ ] 2.1 Make `GatewayState` and dispatch hold the session as the `Session` trait object instead of `RithmicSession`
- [ ] 2.2 Implement `FakeSession` (scripted events, injectable plant drop, injectable error codes, recorded calls)
- [ ] 2.3 Add the `rithmic-gateway-fake` bin target behind `required-features = ["testing"]`; confirm no env switch on the production binary selects a fake plant
- [ ] 2.4 Build the integration harness: spawn the fake binary in a temp runtime dir, expose a client-agnostic scenario list (subscribe + fan-out, trading gates, order/bracket restore after plant drop, ticker reset, overflow, history refusal under live ticker, transport drop)
- [ ] 2.5 Run the harness against the existing Python `GatewayClient` and fix or record any current-behavior surprises before porting
- [ ] 2.6 Add a packaging test asserting the wheel contains `rithmic-gateway` and not `rithmic-gateway-fake`

## 3. RPC table and parity tests

- [ ] 3.1 Add `RPC_TABLE` (trait method, proto `Body` variant, kind OneShot/SubscriptionIntent, gate None/Trading/CancelAll)
- [ ] 3.2 Test: every `session.proto` request variant and every trait method appears in the table
- [ ] 3.3 Test: every SubscriptionIntent row maps to a `RestorePlan` field and a `restore_intents` re-issue (including order and bracket on the order-plant stream)
- [ ] 3.4 Test: every Trading-gated row is denied under `ParentGates` with trading off; cancel-all gate independent of trading
- [ ] 3.5 Test: Python `WireSession` protocol method set equals the trait's method set (via `_lib.pyi`)
- [ ] 3.6 Retire the proto-string-grep parity tests in `framing.rs` once 3.2–3.4 cover them

## 4. Rust GatewaySession

- [ ] 4.1 Port attach-only: dial existing socket, flock check, bounded wait for a starting parent, `parent_not_running` error (from `attach.py`, `flock.py`; reuse `singleton`, `runtime_dir`, `env_keys`). Prerequisite: `simplify-gateway-modes` landed
- [ ] 4.2 Confirm no spawn or binary-resolution code is ported (it stays in the Python launcher only)
- [ ] 4.3 Port handshake: auth token only, fingerprint match, scopes, read-only `trading_enabled` / `cancel_all_enabled`
- [ ] 4.4 Implement the single reader task with pending-reply slots and bounded per-channel queues (md, history, pnl, order); surface fan-out overflow as an error
- [ ] 4.5 Implement single-flight re-dial with transport generation; unknown-outcome error for in-flight state-changing RPCs; at most one retry for read-only RPCs
- [ ] 4.6 Implement every trait RPC over the proto; add `pb::Event` → `PlantEvent` conversion (inverse of `convert.rs`)
- [ ] 4.7 Implement client-side chunked `load_time_bars_range` with per-slice timeout, bounded slice retry, merge, sort, dedup
- [ ] 4.8 Port the behavior of `test_gateway_mux.py`, `test_gateway_runtime.py`, `test_transport_recovery.py`, `test_gateway_attach.py` as Rust tests against the fake binary
- [ ] 4.9 Test: identical scripted event stream through Direct and Gateway yields equal dicts
- [ ] 4.10 Test: concurrent `poll_event(timeout)` and RPC from multiple threads do not deadlock (GIL released)

## 5. Switch-over behind a temporary selector

- [ ] 5.1 Add `RITHMIC_GATEWAY_CLIENT=rust|python` to `create_session` (default `python`); route `rust` through the PyO3 class in gateway mode
- [ ] 5.2 Run the shared harness against both the Rust client and the Python lake client; both pass the same scenarios
- [ ] 5.3 Live verification on Rithmic Test, gateway mode, `rust` selector, no live place, MotiveWave / R|Trader closed: `scripts/smoke_gateway_shared_ticks.py`, `tests/e2e/test_reconnect_live.py`, `tests/e2e/test_rms_commission_live.py`
- [ ] 5.4 Flip the default to `rust`; keep `python` selector for one release

## 6. Remove the Python gateway path from the adapter

- [ ] 6.1 Delete `python/rithmic_nt_connect/gateway_wire.py` and the selector; make `create_session` use the PyO3 class for both modes
- [ ] 6.2 Remove adapter imports of `rithmic_gateway`; add the "imports without protobuf" test
- [ ] 6.3 Confirm no `dual`, spawn, or retired-knob code remains (removed by `simplify-gateway-modes`)
- [ ] 6.4 Confirm `python/rithmic_gateway` still imports without Nautilus or the extension and its tests still pass against the harness
- [ ] 6.5 Update `_lib.pyi`, `docs/references/gateway-remote.md`, and `scripts/build_wheel.sh` expectations

## 7. Docs and close-out

- [ ] 7.1 Rewrite the AGENTS.md "Direct ↔ gateway plant-surface parity" section around the trait and RPC table (keep the RPC-kinds learning)
- [ ] 7.2 Update `docs/STATUS.md` session-gateway row and marks; fix the existing scoreboard drift; run `python scripts/status_progress.py` and `--check`
- [ ] 7.3 Run the full verification list from AGENTS.md (fmt, clippy, cargo test, ruff, ty, pytest, qlty, status check) and record results
- [ ] 7.4 File the follow-up change for moving Nautilus-independent exec logic to Rust (state machine, drain pipeline, dedup, commission, exposure, bar hygiene) with its test-porting requirements
