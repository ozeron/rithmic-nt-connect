> **Depends on `unify-session-host`**, which lands first. That change moves the session registry, holders, flock, gates and recovery events into `SessionHost` and removes `reset_ticker` / `is_direct`. Once it lands, group 1 here shrinks to deleting the Python wrappers, and the names follow it (`Venue` / `RithmicVenue` / `FakeVenue`, `Session` / `LocalSession` / `RemoteSession`).

## Why

Adding one plant RPC touches about 12 files today. Some of those copies are real behaviour: `DirectSession`, `FakeSession`, `GatewaySession` and the server handlers each do something different per method. Most are mechanical:

- `_FlockedDirectSession` repeats about 250 lines of forwards (`session.py`), only because a `__getattr__` delegate is invisible to type checkers.
- The Python `WireSession` / `TickerSession` / `PnlSession` / `OrderSession` protocols are a hand-written list of the same methods.
- The PyO3 `#[pymethods]` forwards and `_lib.pyi` repeat the trait one more time.
- Records cross PyO3 as `PyDict`, built by about 240 `set_*` calls (`python/mod.rs:126-480`). Python then reads them by string key at about 140 sites that `ty` cannot check.

Also, argument names drift between layers for the same RPC (`start_sec` / `start_ssboe` / `start_time_sec`, `start` / `start_date`), and the surface carries two event models (`poll_*` and `next_events`).

The first goal is to delete what is purely mechanical. Generating code is limited to the one place where it is uniform: the PyO3 forwards and their stub.

## What Changes

- **BREAKING (internal): delete the Python session wrappers.** `_FlockedDirectSession`, `_RustGatewaySession`, `_SESSION_CACHE`, `ensure_connected` and `_connect_once` go away. The extension takes over their behaviour:
  - the credential flock
  - the direct process-wide singleton per credential fingerprint
  - the holder count (the last holder disconnects and frees the flock)
  - idempotent `connect`
  - fan-out of transport events to several listeners, each cancellable on its own

  `create_session` returns `_lib.Session`, and `WireSession` becomes an alias of it. The hand-written Protocols are deleted.
- **BREAKING (internal): one event model.** Remove `poll_event`, `poll_history_event`, `poll_pnl_event` and `poll_order_event` from the `Session` trait, the gateway client, PyO3 and the stub. Every consumer uses `next_events(stream, max, timeout)`.
- **One argument name per RPC.** The trait, PyO3, the stub and the Rust structs use the proto field names (for example `start_time_sec`, `start_date`). The proto is not changed.
- **BREAKING (internal): typed records.** The existing `dto.rs` structs become frozen pyclasses through `cfg_attr` behind the `python` feature, which only `rithmic-nt-connect` enables. They replace the `PyDict` builders. Python reads attributes. A dict-access shim keeps the migration green, and it is removed inside this change.
- **Generate only the uniform layer.** A small `macro_rules!` inside `rithmic-nt-connect` lists the simple RPCs once. From that list it emits the PyO3 forwards and the `Session` section of `_lib.pyi`. `RPC_TABLE` stays the hand-written declaration of kind, gate, lane and restore, and its coverage tests stay. The trait, the gateway client and the server handlers stay hand-written.

Not in this change (next steps in `design.md`):

- splitting the trait by capability, and moving `reset_ticker` to a direct-only hook
- per-plant actors in `rithmic-plants`
- splitting `RithmicExecutionClient` / `data.py`
- retiring or freezing the lake `GatewayClient`

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `session-backend`:
  - The plant surface no longer contains the `poll_*` methods.
  - Argument names match the proto.
  - The direct singleton, flock and holders are owned by the extension.
- `adapter-wire-facade`:
  - Python uses the extension `Session` directly, with no wrapper.
  - `WireSession` is an alias of it.
  - Transport listeners are fanned out by the extension.
  - Records are typed, frozen objects instead of dicts.

## Impact

- **Rust:**
  - `crates/rithmic-plants/src/{backend.rs, direct.rs, fake.rs, dto.rs}`: `poll_*` removed, names unified, `cfg_attr(pyclass)` added
  - `crates/rithmic-gateway/src/client/`: `poll_*` removed
  - `crates/rithmic-nt-connect/src/python/`:
    - adds the session registry and the holders
    - adds the listener set on the existing delivery thread
    - adds the forwards macro and the stub generator
    - deletes the dict builders
- **Python:**
  - `session.py` shrinks to the factory and config mapping
  - `_lib.pyi`: the `Session` section is generated
  - `data.py` and `execution.py` stop using `ensure_connected` and `poll_*`
  - the record readers move from keys to attributes (`_convert`, `_orders`, `order_rows`, `notification_intake`, `recon`, `inventory`, `verify`, …)
  - `wire_types.py` re-exports
- **Tests:**
  - a Python-constructible fake-backed `Session` (`testing` feature) for the singleton and holder tests that today poke wrapper internals (about 25 tests in `test_session_factory.py` / `test_rust_gateway_session.py`)
  - `WireSessionStub` builds DTOs
  - about 20 test files that hand-build event dicts
  - the FFI throughput benchmark re-baselined
- **Unchanged:** `session.proto` and the wire protocol, the lake `rithmic_gateway` package, gateway parent behaviour, gates, outcome classes, and `RPC_TABLE` semantics.
- **Docs:** the AGENTS.md "When you add or change a plant RPC" list, `crates/README.md`, and an amendment to ADR 0002.
