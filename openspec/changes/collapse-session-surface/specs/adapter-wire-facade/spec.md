## MODIFIED Requirements

### Requirement: One PyO3 session class for both modes
The extension SHALL expose a single `Session` class that fronts either backend according to `connect_mode`. `create_session` SHALL return that extension object directly, with no Python wrapper class around it. `WireSession` SHALL be an alias of the extension `Session` type, not a hand-maintained protocol. The PyO3 forwards for uniform RPCs and the `Session` section of `_lib.pyi` SHALL be generated from one list in the extension. A test SHALL fail when the checked-in stub differs from a fresh generation.

Transport-event subscription SHALL accept multiple listeners per session, each cancellable on its own. Listeners SHALL be invoked on the extension's dedicated delivery thread, never on a runtime worker. They SHALL be held until cancelled, and every listener SHALL be cancelled when the session's last holder disconnects.

#### Scenario: Data client unchanged across modes
- **WHEN** `RithmicDataClient` runs against a direct session and against a gateway session
- **THEN** it SHALL call the same methods with the same arguments and receive the same record types

#### Scenario: Existing Python fake sessions
- **WHEN** unit tests inject `WireSessionStub` or other fakes implementing `WireSession`
- **THEN** they SHALL keep passing, with the fakes building records through the DTO constructors

#### Scenario: No Python session wrapper
- **WHEN** the change is complete
- **THEN** `_FlockedDirectSession`, `_RustGatewaySession` and `ensure_connected` SHALL NOT exist, and `create_session` SHALL return an instance of `rithmic_nt_connect._lib.Session`

#### Scenario: Stale generated stub
- **WHEN** an entry is added to the extension's forward list and `_lib.pyi` is not regenerated
- **THEN** the stub-freshness test SHALL fail

#### Scenario: Two clients listen for transport events
- **WHEN** the data and exec clients both subscribe to transport events on one gateway session and the data client cancels its subscription
- **THEN** the exec client SHALL keep receiving `down` / `up` / `recovery_failed` events

#### Scenario: Listener may block on the session
- **WHEN** a transport listener makes a blocking session call
- **THEN** the call SHALL complete without deadlocking the runtime, because the listener runs on the delivery thread

### Requirement: Blocking stays inside the extension
Blocking operations SHALL run on the extension's runtime with the GIL released. The extension SHALL be the only place that blocks on the runtime, and Python callers SHALL invoke blocking methods through `asyncio.to_thread`, so the asyncio loop never performs a nested blocking wait.

#### Scenario: Concurrent event wait and RPC
- **WHEN** one thread blocks in `next_events(stream, max, timeout)` while another thread issues an RPC
- **THEN** both SHALL make progress and neither SHALL deadlock on the GIL

#### Scenario: Event loop stays responsive
- **WHEN** a slow RPC is in flight from an async client
- **THEN** other asyncio tasks SHALL continue to run

## REMOVED Requirements

### Requirement: Records are plain dicts with one shape
**Reason**: Dict records lose types at the FFI boundary, so `ty` cannot check the roughly 140 read sites. They are replaced by typed DTO records.
**Migration**: Read DTO attributes (`event.price`) instead of keys (`event["price"]`). During this change the DTOs also answer `event["price"]` / `event.get("price")`, and that shim is removed before the change is archived. Test fakes build records with the DTO constructors.

## ADDED Requirements

### Requirement: Records are typed with one shape
Event batches and reads SHALL return frozen extension objects: the existing Rust DTO structs exposed as pyclasses. An empty `next_events` batch SHALL mean a timeout. The class and field values SHALL be identical in both modes for the same plant event, and the field values SHALL keep the types and units the dict records carried. Venue enumerations unknown to the closed-set mapping SHALL still be rejected at the single convert boundary.

#### Scenario: Wait timeout
- **WHEN** no event arrives within the timeout
- **THEN** `next_events` SHALL return an empty batch

#### Scenario: Unknown closed-set value
- **WHEN** an event carries a venue enumeration value outside the known set
- **THEN** conversion SHALL reject it and SHALL NOT emit a Nautilus object with an invented value

#### Scenario: Records are immutable
- **WHEN** Python code assigns to a field of a received DTO
- **THEN** the assignment SHALL raise

#### Scenario: Same event, same record in both modes
- **WHEN** one scripted plant event is delivered through a direct and a gateway session
- **THEN** both SHALL yield DTOs of the same class with equal fields

#### Scenario: Boundary throughput does not regress
- **WHEN** the FFI throughput benchmark runs with `RITHMIC_PERF=1`, covering both crossing and field access
- **THEN** events per second SHALL be no lower than the dict baseline recorded before the change
