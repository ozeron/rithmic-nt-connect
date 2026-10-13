## Why

Direct and gateway are two hand-built stacks that only meet at the Python `WireSession` protocol. Every plant RPC is implemented in six places (`plants/session.rs`, PyO3 `mod.rs`, gateway `dispatch/*.rs`, `rithmic_gateway/client.py`, `gateway_wire.py`, `session.py`), and "wire it on both paths" is a convention enforced by proto greps and review (AGENTS.md "Direct ↔ gateway plant-surface parity"). Parity drift has already cost us once (bracket updates shipped as a bare subscribe with no fan-out or restore). The gateway client is also ~3k lines of Python (client, mux, runtime, transport recovery, spawn/attach) that re-implements venue-adjacent policy outside the type system, and forces the adapter to pin protoc 5.29.6 against Nautilus's `protobuf==5.29.6`.

Both modes must stay (the gateway exists because Rithmic allows one login per credential). The fix is to make one Rust trait the single definition of the plant surface so the compiler enforces parity.

## What Changes

- Introduce a Rust `Session` trait in `rithmic-plants` (or a small new crate) covering every plant surface the Python `WireSession` protocol exposes today, with two implementations:
  - `DirectSession`: in-process plants (the current `RithmicSession` + PyO3 plumbing, behind the trait).
  - `GatewaySession`: a Rust client for the gateway unix socket, speaking the existing `session.proto` and reusing the prost types the server already generates.
- Port the gateway client policy to Rust: handshake, attach-only parent attach (bounded wait for an already-starting parent; the client never spawns, per `simplify-gateway-modes`), shared-credential flock check, single-socket mux with typed event fan-out (mux is the only topology), single-flight transport re-dial with generation, and client-side chunked `load_time_bars_range`.
- Depends on `simplify-gateway-modes` landing first: no spawn, no `dual`, no password in gateway-mode clients.
- Expose both implementations through the one existing PyO3 `Session` class, selected by explicit `connect_mode`. The Python `WireSession` protocol is preserved so the data/exec clients and their fake-session tests do not change.
- Remove the adapter's use of the pure-Python gateway client: delete `gateway_wire.py` and route `create_session` through the PyO3 class for both modes. Python no longer speaks protobuf in the adapter.
- Add a real-binary integration test: Python adapter session ↔ real `rithmic-gateway` binary ↔ stub plant, covering subscribe fan-out, order/bracket restore after a plant drop, gates, and transport re-dial.
- Replace the proto-grep parity tests with a single RPC table (name, request fields, one-shot vs subscription intent, gate kind) that the trait, dispatch, and Python facade are checked against.
- **BREAKING (internal)**: `rithmic_nt_connect.gateway_wire` is removed; adapter code that imports `rithmic_gateway.GatewayClient` directly must use the PyO3 session.

Explicitly out of scope (follow-up change): moving the Nautilus-independent execution logic (order-plant lifecycle state machine, drain/recovery pipeline, fill dedup, commission, exposure, bar hygiene) from Python into Rust.

## Capabilities

### New Capabilities
- `session-backend`: the Rust `Session` trait as the single definition of the plant surface (ticker/book, history, time bars, PnL, order, brackets, RMS, account resolution, ticker reset), with `DirectSession` and `GatewaySession` implementations, the one-shot-vs-subscription-intent classification, and compile-time parity between modes.
- `gateway-client`: the Rust gateway client behavior — attach-only/flock, handshake and scope gating, mux with typed fan-out, transport recovery, restore-after-reconnect semantics as seen by the client, and chunked history range loads.
- `adapter-wire-facade`: the PyO3/Python boundary — one `Session` class for both `connect_mode`s, preserved `WireSession` protocol, `block_on` kept inside the extension, no protobuf dependency in the adapter.

### Modified Capabilities
<!-- None: openspec/specs/ is empty. The existing behavior is captured as new specs by this change. -->

## Impact

- **Rust**: `crates/rithmic-plants` (trait, `DirectSession`), `crates/rithmic-nt-connect/src/python/mod.rs` (one class over the trait), new gateway-client module (likely in `rithmic-gateway` or a new `rithmic-gateway-client` crate sharing the prost output), `crates/rithmic-gateway/tests` (real-binary integration test).
- **Python**: delete `python/rithmic_nt_connect/gateway_wire.py`; adapter stops importing `rithmic_gateway` for sessions; `session.py` factory simplified; `_lib.pyi` updated.
- **Lake consumer**: `python/rithmic_gateway` is documented as a Nautilus-free, maturin-free client for market-data-lake. It is kept as-is for the lake (proto remains the shared contract) and is not used by the adapter after this change; its retirement is a separate decision (see design open questions).
- **Packaging/dependencies**: adapter no longer needs the protoc 5.29.6 gencode pin at runtime; wheel build (`scripts/build_wheel.sh`) must keep bundling the `rithmic-gateway` binary.
- **Docs**: `docs/STATUS.md` (session gateway row, capability marks, scoreboard regenerated), AGENTS.md parity section rewritten around the trait, `docs/references/gateway-remote.md`.
- **Risk**: transport recovery, mux, and attach-wait behavior are reimplemented; their Python tests (`test_gateway_mux.py`, `test_gateway_attach.py`, `test_transport_recovery.py`, `test_gateway_runtime.py`) define the behavior that must be ported with them.
