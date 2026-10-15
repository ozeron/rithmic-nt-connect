## Context

> **Rebased onto `unify-session-host` (2026-10-15).** The extension-owned registry, holders and flock (D2), multi-listener session events (D3, now `subscribe_session_events`) and `Session.fake` landed there. This change keeps D1 (delete the wrappers, which are now forwarding only), D4 (`next_events` only), D5 (one argument name), D6 (typed records) and D7 (generated forwards and stub). Type names in the text below predate the rename: `DirectSession` → `RithmicVenue`, `FakeSession` → `FakeVenue`, `GatewaySession` → `RemoteSession`.

An independent review (2026-10-15) checked the first draft of this change against the code. It found that only part of the "13 copies" is mechanical:

- **Real behaviour (stays hand-written):**
  - `DirectSession`, `FakeSession` and `GatewaySession`. For example, `GatewaySession` arms queues, builds `IntentKey`s and picks a history flag and response extractor per method (`client/mod.rs:439, 596-640, 714, 746`).
  - The server read handlers, which each take their own locks, refuse under live market data, and pick their own error codes (`orders.rs:50-56`, `history.rs:63-67`, `info.rs:40`). One-shot commands already share a helper (`orders.rs:268 one_shot`).
- **Mechanical (removed or generated here):**
  - `_FlockedDirectSession`'s ~250 lines of forwards, which exist only because `__getattr__` is invisible to type checkers.
  - The three Python Protocols plus `WireSession`.
  - The PyO3 forwards (`python/mod.rs:817-1342`) and `_lib.pyi`.
  - The ~240 `PyDict` `set_*` builders (`python/mod.rs:126-480`).

The wrappers also carry behaviour that has to move into the extension:

- **`_FlockedDirectSession`:**
  - the flock, taken before the session is built
  - the `_SESSION_CACHE` singleton, keyed by a fingerprint without the password
  - the holder count (the last holder disconnects, frees the flock and evicts the entry)
  - idempotent `connect`, which swallows `AlreadyConnectedError` under a lock
  - `request_plants(execution)` on a cache hit
  - a refcount-blind `reset_ticker`
- **`_RustGatewaySession`:** one extension registration fanned out to many listeners through a weak relay, cancelled when the last listener leaves and on `disconnect`.

The extension already delivers transport events on a dedicated `std::thread` (`python/mod.rs:765`). A listener may block on the shared runtime, so it must never run on a runtime worker.

## Goals / Non-Goals

**Goals:**
- Python holds `_lib.Session` directly, and `WireSession` is an alias of it.
- One event model (`next_events`), and one argument name per RPC across all layers.
- Records are typed, frozen objects with today's field values.
- Python-side code that only lists or forwards methods is deleted. On the Rust side, only the uniform PyO3 layer is generated.
- No wire, gate, outcome or gateway-parent behaviour change.

**Non-Goals:**
- Generating the trait, `RPC_TABLE`, `GatewaySession` or server dispatch. Their per-method differences are behaviour, not repetition.
- Changing `session.proto`.
- Splitting the trait, plant internals or the Nautilus clients (see Next steps).
- Typed venue enums.

## Decisions

### D1. Delete the wrappers and alias `WireSession` (biggest win, done first)

`create_session` returns `_lib.Session`. `session.py` keeps `WireSession = _lib.Session` (under `TYPE_CHECKING`, so importing the module doesn't need the extension). `TickerSession` / `PnlSession` / `OrderSession` are deleted; clients type against `WireSession`.

`WireSessionStub` stays as a test double, and `test_wire_session_parity.py` checks it against `_lib.pyi`.

*Alternative considered:* keep the Protocols for the narrower clients. That would keep a hand-written list of the same methods, which is exactly what this change removes. The capability split in step 4 is the right place for narrow types.

### D2. Direct singleton and holders in the extension

`rithmic-nt-connect` gains:
- `static DIRECT: Mutex<HashMap<Fingerprint, Weak<SharedDirect>>>`
- `SharedDirect { session: DirectSession, lock: SessionLock, connect_gate: tokio::Mutex<()> }`
- each direct `PySession` holds a `Holder { shared: Arc<SharedDirect>, released: AtomicBool }`

Behaviour:
- **Construction:** registry hit → new holder on the existing `SharedDirect`. Miss → take the flock, then build. A busy flock raises the session-lock error. Construction does no plant I/O.
- **`request_plants`:** unchanged, and stays a separate call. `create_session(plants="execution")` calls it after construction, which is what the Python code does today on a cache hit.
- **`connect`:** runs under the connect gate with the GIL released. `AlreadyConnected` → `Ok`.
- **`disconnect`:** `released.swap(true)` guards against double release. The last live holder disconnects the plants with the GIL released, then drops the flock and evicts the entry.
- **`Drop` of an unreleased holder:** never blocks and never touches the venue. If it was the last holder, it evicts the entry and drops the `SharedDirect`, which drops the flock. Plant tasks end when their handles drop. Explicit `disconnect` stays the contract.
- **`reset_ticker`:** stays refcount-blind (`direct.rs` already is).

Gateway sessions are not cached here. The Rust client runtime already shares one socket per fingerprint.

### D3. Transport listeners in the extension, on the existing delivery thread

`subscribe_transport_events(listener) -> TransportSubscription` keeps its current signature and gains multiple listeners: a `ListenerSet` per shared gateway client, with an id per listener.

- **Delivery:** the existing dedicated thread calls every live listener in registration order, with the GIL taken there.
- **Lifetime:** listeners are held strongly until `cancel()`, matching today. The Rust registration is cancelled when the set empties and on the last holder's `disconnect`. There is no WeakMethod.
- **Event types:** the transport event types become pyclasses and `transport.py` re-exports them, so ADR 0002 still holds. `event_from_rust` is deleted.

### D4. One event model: delete `poll_*` (pulled forward from step 4)

`next_events(stream, max, timeout)` already exists in the trait and both backends, and the data client and `PlantPoller` already prefer it. The work:

- Remove `poll_event`, `poll_history_event`, `poll_pnl_event` and `poll_order_event` from the trait, `DirectSession`, `FakeSession`, `GatewaySession`, PyO3, the stub and `WireSessionStub`.
- Move the three remaining callers to `next_events`: `PlantPoller`'s fallback, `inventory.py:265` and `verify.py:99`.

This takes four methods off the surface before anything is generated.

### D5. One argument name per RPC (pulled forward)

Rename the trait, PyO3 and stub arguments to the proto field names, for example `load_orders(start_time_sec, end_time_sec)` and `load_fill_history(start_date, finish_date)`. The proto is unchanged.

A test compares the PyO3 signatures (`inspect.signature` on the built extension) with the proto request message fields, using the `session_pb2` descriptors in test-only code. A mismatch fails.

### D6. Typed records: `cfg_attr` on the existing DTOs

`rithmic-plants` gains an optional `python` feature with an optional `pyo3` dependency. Each struct in `dto.rs` gets `#[cfg_attr(feature = "python", pyo3::pyclass(frozen, get_all, eq, module = "rithmic_nt_connect._lib"))]`, plus a `#[new]` kwargs constructor under the same `cfg_attr` for test fakes.

Only `rithmic-nt-connect` enables the feature. The gateway server and `rithmic-loadtest` build exactly as today, so there is no second struct list and no `From` mirror.

- **Events:** `PlantEvent` (an enum) is returned as its variant's struct. Each event struct gets a `type` class attribute (today's dict `"type"` string), so string routing keeps working until step 4.
- **Shim:** a read-only `__getitem__` / `get` / `__contains__` over field names (one small helper macro in `rithmic-nt-connect`) keeps the migration green. It is removed in the last group. `ty` then catches any leftover key access, so no grep test is needed.

*Watch:* `get_all` clones `String` fields on every access. The benchmark gets an access-pattern case as well as the crossing case.

### D7. Generate only the PyO3 forwards and the stub

A single `macro_rules! session_forwards` in `rithmic-nt-connect` lists the uniform RPCs, about 30 of the 40 methods. These are methods whose PyO3 body is "release the GIL, `block_on(inner.method(args))`, map the error and convert the result". An entry is just the signature:

```rust
session_forwards! {
    fn subscribe(symbol: &str, exchange: &str) -> ();
    fn load_ticks(symbol: &str, exchange: &str, start_time_sec: i32, end_time_sec: i32) -> Vec<HistoryTickDto>;
    fn cancel_order(basket_id: &str) -> ();
    // …
}
```

It expands to two things from the same tokens:
1. one `#[pymethods] impl PySession { … }` block with all the forwards
2. a `const SESSION_PYI: &str` built with `concat!`/`stringify!`

A test writes `SESSION_PYI` into the `Session` section of `_lib.pyi` when `UPDATE_STUBS=1` is set, and otherwise fails on a diff.

The hand-written methods stay in a second `#[pymethods]` block: the constructor, gates, transport, `next_events`, the three struct-argument commands and `resolved_account`. That needs PyO3 `multiple-pymethods`, which uses `inventory` link-time registration. If that is unwanted in the wheel, the fallback is to put the hand-written methods inside the macro invocation as pass-through items, so there is still one block. This is decided in group 5 after measuring.

*Alternatives considered:*
- **A cross-crate `for_each_rpc!` declaration** that also generates the trait, table, gateway client and server routes. The review showed those layers need 5+ extra per-RPC columns (intent key, queue arm, history flag, extractor, locks, error codes). The declaration would turn into a DSL, the line count would come out about even, and it would be harder to debug. Rejected.
- **A proc-macro.** That means a new crate for one list. `macro_rules` is enough when the entries are uniform.

### What stays hand-written, and why that's fine

`RPC_TABLE` is already a declarative table (kind, gate, lane, restore), and `tests/rpc_table.rs` checks it against the proto and the trait. The trait, the backends and the server handlers differ per method by design, and the compiler already enforces trait ↔ implementation parity. After this change, adding a uniform RPC touches:

1. the proto message
2. the trait method
3. `DirectSession`
4. `FakeSession`
5. `GatewaySession`
6. the server handler
7. the `RPC_TABLE` row
8. one `session_forwards!` line, followed by regenerating the stubs

Steps 1–7 carry actual decisions, and step 8 is one line. That is down from about 12 files, and none of the remaining edits is copying.

## Risks / Trade-offs

- **[Tests poke wrapper internals]** About 25 tests in `test_session_factory.py` / `test_rust_gateway_session.py` touch `_connect_gate`, `_inner` and `_listeners`. → Expose a `testing`-feature constructor (`Session.fake(fingerprint=…)`) backed by `FakeSession`, and rewrite those tests against behaviour (logins opened, flock held, listener calls) instead of internals.
- **[Drop at interpreter shutdown]** → Drop never blocks and never takes the GIL beyond what PyO3 already holds. Explicit `disconnect` stays the documented path, and a test covers the dropped-holder case.
- **[Listener re-entrancy]** → Listeners stay on the dedicated thread (unchanged), so a listener calling a blocking session method is safe.
- **[Renamed arguments break keyword callers]** → The callers are internal and found by `ty` plus the generated stub. Scripts under `scripts/` and `examples/` are included in the sweep.
- **[DTO migration churn]** → The shim keeps every commit green, the migration goes module by module, and the shim is removed last.
- **[FFI throughput]** → A baseline is recorded before group 4, and both crossing and access are measured.
- **[Mixed-version gateway]** → There is no wire change.

## Migration Plan

Each group in `tasks.md` is one PR and leaves the tree green. The order is:

1. Extension-owned singleton, holders and listeners; delete the wrappers; alias `WireSession`.
2. Delete `poll_*`.
3. Unify argument names.
4. Typed DTOs with the shim.
5. Generate the forwards and stub.
6. Migrate Python to attributes.
7. Remove the shim and update the docs.

Rollback is a revert per PR.

## Open Questions

- Should the three struct-argument commands take a typed request pyclass instead of kwargs? Deferred to step 4.
- Is `multiple-pymethods` acceptable in the wheel, or should the single-block fallback be used? Decided in group 5.

## Next steps (out of scope)

4. **Split `Session` by capability.** Replace the single trait with `Lifecycle`, `MarketData`, `History`, `Orders` and `Account`, and group the table, lanes and gates by capability. Move `reset_ticker` off the shared surface into a direct-only recovery hook. Consider typed venue enums in `rithmic-exec` (side, price type, duration, notify kind), with `PlaceOrder` carrying them instead of strings, and typed per-stream delivery replacing string `type` routing.
5. **Per-plant actors in `rithmic-plants`.** Replace the `RithmicSession` (`&mut self`) / `Handles` / `*_on(handle)` layering and `DirectSession`'s lock set and mirrored `order_cache` / `account_cache` with one task per plant. Each task owns its handle and takes commands over a channel.
6. **Split the Nautilus clients.** Break `RithmicExecutionClient` into four components, with the client as a thin composition over them:
   - `OrderCommands`
   - `OrderIntake`
   - `AccountState`
   - `Reconciliation`

   In `data.py`, move the converters into `_convert`, and give `BookAssembler` and the bar guards their own module (or move them into `rithmic-exec` where the rules are pure).
- **Lake `GatewayClient`.** With prebuilt wheels shipping, decide whether the lake can depend on the wheel. If yes, retire `python/rithmic_gateway/client.py` and `history_window.py`. If not, freeze them at read-only scope.
