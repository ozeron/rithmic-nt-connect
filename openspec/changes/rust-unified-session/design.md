## Context

Today the plant surface is defined four times and kept in sync by hand:

```
Python data/exec clients
  └─ WireSession (typing.Protocol, python/rithmic_nt_connect/session.py)
       ├─ direct : PyO3 `Session` (nt-connect/src/python/mod.rs) → RithmicSession (rithmic-plants)
       └─ gateway: GatewayWireSession (gateway_wire.py) → GatewayClient (rithmic_gateway/client.py, pure Python)
                     → unix socket + session.proto → rithmic-gateway server (dispatch/*.rs) → RithmicSession
```

Constraints that shape the design:

- Rithmic allows one login per credential. The gateway parent owns the login; direct is a process-wide singleton per credential fingerprint guarded by a flock. Both modes stay.
- 1.231.x v1 `TradingNode` seam: Nautilus-typed clients stay in Python. The extension returns plain dict events and Python converts at one boundary (`_convert.py`).
- `block_on` lives only inside the extension; Python calls through `asyncio.to_thread`.
- `python/rithmic_gateway` is advertised as a Nautilus-free, maturin-free client for market-data-lake. The adapter currently shares it; the lake must keep working.
- The wire protocol (`session.proto`, 566 lines) and the server's gates / fan-out / `RestorePlan` semantics are correct and live-proven. This change must not alter them.
- The gateway server dispatch is currently concrete over `RithmicSession`, so it cannot run against a stub plant. That is why there is no real-binary integration test today.

## Goals / Non-Goals

**Goals:**
- One Rust definition of the plant surface; adding or changing an RPC without implementing it in both modes fails to compile.
- Move the gateway client (attach-only, flock check, handshake, mux, transport recovery, range loads) into Rust behind that definition.
- Keep the Python `WireSession` protocol and everything above it unchanged.
- Make the gateway server runnable over a stub plant so Python ↔ real gateway binary ↔ stub plant is testable without credentials.
- Stop the adapter depending on Python protobuf gencode.

**Non-Goals:**
- Moving Nautilus-independent execution logic (order-plant lifecycle state machine, drain/recovery pipeline, fill dedup, commission, exposure, bar hygiene) to Rust. That is a follow-up change, gated on porting its property/mutation tests.
- Changing `session.proto`, server gates, fan-out, or restore semantics.
- Retiring the pure-Python `rithmic_gateway` lake client.
- Native TLS / remote gateway (already N/A).
- Gateway support for fill/order history (stays direct-only, deferred per STATUS).

## Decisions

### 1. Trait lives in `rithmic-plants`; `GatewaySession` lives in `rithmic-gateway`

`rithmic-gateway` already depends on `rithmic-plants`, and `rithmic-plants` must not depend on the gateway. So the `Session` trait and `DirectSession` (a thin wrapper over `RithmicSession`) sit in `rithmic-plants`; the client implementation is a new `client` module in the `rithmic-gateway` lib, which already owns `pb`, `framing`, `singleton` (flock), `runtime_dir`, and `env_keys`.

Alternatives: a new `rithmic-gateway-client` crate (cleaner link graph, but needs the prost output split into its own proto crate; defer until the extension's binary size or link time hurts); trait in a third `rithmic-session` crate (no current need).

### 2. Native async trait, dynamic dispatch at the PyO3 edge only

The trait uses `async fn` for RPCs and sync, timeout-bounded `poll_*` methods for events. The PyO3 class holds a `Box<dyn Session>` behind one mutex, using the `async_trait` crate for the object-safe form (decided). RPC rate is far below where vtable cost matters.

Alternatives: an `enum Backend { Direct, Gateway }` with macro-generated delegation (no boxing, but the delegation macro becomes a second hand-maintained list — the thing we are removing); generic `PySession<S>` (cannot be a single `#[pyclass]`).

### 3. One event type: `PlantEvent` in, identical dict out

`GatewaySession` converts `pb::Event` → `rithmic_plants::dto::PlantEvent` (the inverse of today's `rithmic-gateway/src/convert.rs`), and PyO3 converts `PlantEvent` → dict in exactly one place for both backends. This removes the separate Python `_event_to_dict` / `_normalize_history_tick` path, a latent source of mode-dependent dict shapes.

### 4. Server becomes generic over the trait; stub plant is a test-only binary

`GatewayState` holds the session as the trait object instead of `RithmicSession`. A `FakeSession` (scripted events, injectable plant drop, recorded calls) implements the trait. A separate bin target `rithmic-gateway-fake`, `required-features = ["testing"]`, serves over `FakeSession`. It is never built by `build_wheel.sh` and the production binary has no env switch to select a fake plant (a runtime fake-mode flag on the shipped binary would be a safety hazard).

Alternative: in-process server in a test thread (faster, but would not cover attach/flock/process-exit behavior, which are exactly what we are porting).

### 5. Parity beyond the compiler: one RPC table

The trait guarantees both *client-side* implementations cover every method. It does not guarantee the server dispatch handles every proto request, nor that each RPC has the right kind. A `RPC_TABLE` const lists, per RPC: trait method, proto `Body` variant, kind (`OneShot` | `SubscriptionIntent`), gate (`None` | `Trading` | `CancelAll`). Tests assert: every `Body` request variant is in the table; every trait method is in the table; every `SubscriptionIntent` row maps to a `RestorePlan` field (`note_*`/`forget_*`) and a `restore_intents` re-issue; every `Trading` row is denied under `ParentGates` with trading off. This replaces the proto-string-grep tests in `framing.rs` and the matching Python guards, and encodes the "RPC kinds" learning from AGENTS.md.

### 6. Direct-only surfaces are a separate trait

Fill history / order history replay is deferred for gateway. It moves to a `HistoryReplay` trait implemented only by `DirectSession`. The PyO3 class exposes those methods only when the backend implements it, and raises a typed "unsupported in gateway mode" error otherwise. `tests/test_history_direct_only.py` keeps asserting the book/runners cannot reach it in gateway mode.

### 7. Client structure: one socket per process, one reader task

Per credential fingerprint a process-local runtime owns one unix socket and one tokio reader task that routes inbound frames to: pending RPC replies (by in-flight slot), and bounded per-channel event queues (md, history, pnl, order). `poll_*` drains the channel queue with a timeout, GIL released. This is the Python `mux` mode ported.

The Python client's `dual` mode (one socket per factory, a rollback knob) is removed by `simplify-gateway-modes`, which lands first. It was a gateway-internal client topology, not a third `connect_mode`: `direct` and `gateway` both remain fully supported, and `dual` was never a one-login problem (several client sockets to one parent share one login). The Rust client implements `mux` only.

Alternative: one task per channel with separate sockets (more parent connections, no benefit).

### 8. Transport recovery stays single-flight with a generation counter

On socket loss the runtime performs one re-dial (single-flight, shared by all waiters), bumps `transport_generation`, and surfaces the generation to the adapter so order-plant re-arm and MD replay (adapter-owned, unchanged) can key off it. The parent's `restore_intents` re-issues subscriptions on plant reconnect as today. In-flight state-changing RPCs at the moment of drop return an unknown-outcome error and are never retried by the client (AGENTS.md: state-changing place/cancel/modify is not blindly retried; post-send failure is unknown, not rejected). Read-only RPCs may be retried once after re-dial.

### 9. Attach-only semantics (after `simplify-gateway-modes`)

Dial the existing socket; if the credential flock is held but the socket is not yet accepting, wait up to the attach timeout; otherwise fail with `parent_not_running`. The client never spawns the parent, so there is no spawn, binary resolution, or cold-start race to port (binary resolution stays in the Python launcher only). The handshake verifies the parent's `Fingerprint` (system / URL / user / env) so Live/Demo/Test cannot mix. Gateway-mode clients hold no password. Parent idle-exit is parent-side and unchanged.

### 10. Strangler migration with a temporary switch

Phase the work so each step is shippable and reversible:

1. Trait + `DirectSession` + `HistoryReplay`; PyO3 class over the trait. No behavior change; existing tests green.
2. Server generic over trait; `FakeSession`; fake binary; real-binary integration harness. Run the harness against the **existing Python client first** to pin current behavior.
3. `GatewaySession` in Rust. Temporary `RITHMIC_GATEWAY_CLIENT=rust|python` selector in `create_session` (default `python` until step 4 evidence).
4. Run the same harness against the Rust client, plus the live shared-ticks smoke on Rithmic Test; flip default to `rust`.
5. Delete `gateway_wire.py`, the selector, and adapter imports of `rithmic_gateway`. The lake client package stays and keeps being exercised by the same harness.

Rollback at any step before 5 is the selector; after 5 it is a revert.

## Risks / Trade-offs

- [Transport recovery / attach races regress when reimplemented] → Port the behavior encoded in `test_transport_recovery.py`, `test_gateway_attach.py`, `test_gateway_mux.py`, `test_gateway_runtime.py` as Rust tests against the fake binary before deleting the Python path; run both clients through one harness.
- [Event dict shape drifts between backends] → Single `PlantEvent` → dict conversion (decision 3) plus a test that feeds the same scripted event stream through both backends and compares dicts.
- [GIL deadlock or starvation in blocking polls] → Every blocking extension call releases the GIL; a test calls `poll_event(timeout)` from several threads while another thread issues RPCs.
- [`FakeSession` drifts from real plant behavior and gives false confidence] → It models only what the gateway layer depends on (events, drops, error codes); live smokes remain the authority and stay in the verification list.
- [Fake binary shipped or selected in production] → Separate bin, `required-features = ["testing"]`, excluded from wheel scripts; add a packaging test asserting the wheel contains no fake binary.
- [Lake client and Rust client diverge on the shared proto] → Same integration harness runs both; proto stays the single contract; any proto change updates both or is blocked by the harness.
- [Extension binary grows / link time rises from linking server code with the client] → Accepted for now (decision 1); revisit with a split proto crate if it matters.

## Migration Plan

Follow decision 10. Verification gates per AGENTS.md before each step lands: `cargo fmt/clippy -D warnings`, `cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect`, `ruff`, `ty`, `pytest -q`, `status_progress.py --check`. Step 4 additionally runs `scripts/smoke_gateway_shared_ticks.py` and `tests/e2e/test_reconnect_live.py` / `test_rms_commission_live.py` in gateway mode on Rithmic Test (one login; no MotiveWave / R|Trader open; no live place). `docs/STATUS.md` marks and AGENTS.md parity section are updated in step 5, and the scoreboard regenerated.

## Open Questions

- Packaging: should the venue layer ship as its own Nautilus-free Rust-backed library that the Nautilus adapter depends on (and the lake can use), instead of the extension living inside the adapter wheel? This decides whether `python/rithmic_gateway` is deleted or kept as the lake client. Pros/cons were discussed; undecided.
- Keep `PlantEvent` as the cross-mode event type, or promote it to a versioned public type now that two crates produce it? (Becomes mandatory if the library is split out.)

Resolved: `dual` gateway client mode is dropped outright (direct and gateway both stay); `async_trait` is acceptable.
