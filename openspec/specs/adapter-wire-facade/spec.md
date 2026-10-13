# adapter-wire-facade Specification

## Purpose
TBD - created by syncing change `rust-unified-session`. Update Purpose after archive. Scope: the PyO3/Python boundary: one `Session` class for both modes, no protobuf in the adapter.

## Requirements

### Requirement: One PyO3 session class for both modes
The extension SHALL expose a single `Session` class that fronts either backend according to `connect_mode`, and the Python `WireSession` protocol (method names, arguments, return shapes) SHALL remain unchanged so data and exec clients and their fake-session tests do not change.

#### Scenario: Data client unchanged across modes
- **WHEN** `RithmicDataClient` runs against a direct session and against a gateway session
- **THEN** it SHALL call the same methods with the same arguments and receive the same record shapes

#### Scenario: Existing Python fake sessions
- **WHEN** the existing unit tests inject Python fake sessions implementing `WireSession`
- **THEN** they SHALL continue to pass without modification

### Requirement: Adapter does not depend on Python protobuf
The adapter's session creation path SHALL NOT import `rithmic_gateway` or protobuf gencode, so the adapter imports on any protobuf runtime that Nautilus permits.

#### Scenario: Import without protobuf
- **WHEN** `rithmic_nt_connect` is imported in an environment where `protobuf` is not installed
- **THEN** import and `create_session` in both modes SHALL succeed up to the point of opening a connection

#### Scenario: Removed gateway_wire module
- **WHEN** the change is complete
- **THEN** `rithmic_nt_connect.gateway_wire` SHALL NOT exist and no adapter module SHALL reference it

### Requirement: Blocking stays inside the extension
Blocking operations SHALL run on the extension's runtime with the GIL released, the extension SHALL be the only place that blocks on the runtime, and Python callers SHALL invoke blocking methods through `asyncio.to_thread` so the asyncio loop never performs a nested blocking wait.

#### Scenario: Concurrent poll and RPC
- **WHEN** one thread blocks in `poll_event(timeout)` while another thread issues an RPC
- **THEN** both SHALL make progress and neither SHALL deadlock on the GIL

#### Scenario: Event loop stays responsive
- **WHEN** a slow RPC is in flight from an async client
- **THEN** other asyncio tasks SHALL continue to run

### Requirement: Records are plain dicts with one shape
Event polls SHALL return plain Python dicts (or `None` on timeout) whose shape is identical in both modes, and venue enumerations unknown to the closed-set mapping SHALL be rejected at the single convert boundary rather than passed through.

#### Scenario: Poll timeout
- **WHEN** no event arrives within the timeout
- **THEN** the poll SHALL return `None`

#### Scenario: Unknown closed-set value
- **WHEN** an event carries a venue enumeration value outside the known set
- **THEN** conversion SHALL reject it and SHALL NOT emit a Nautilus object with an invented value

### Requirement: Secrets never leak
Neither backend SHALL include the Rithmic password in any `repr`, error message, or log line, and credentials SHALL be resolved at session construction, not in request methods.

#### Scenario: repr of a session and its config
- **WHEN** `repr` is taken of a session or its configuration
- **THEN** the password SHALL NOT appear

#### Scenario: Error from a failed login
- **WHEN** login fails in either mode
- **THEN** the error text SHALL NOT contain the password

### Requirement: Live, Demo, and Test endpoints cannot mix
A gateway-mode session SHALL refuse a Test or Demo system without an explicit URL, using the same environment derivation as direct mode.

#### Scenario: Test system without URL in gateway mode
- **WHEN** `connect_mode` is `gateway`, the system name is a Test or Demo system, and no URL is set
- **THEN** creation SHALL fail rather than default to the production URL

### Requirement: Partial connect tears down
If connect fails partway in either mode, the session SHALL close everything it started and SHALL leave no held flock, open socket, or running plant.

#### Scenario: Order-plant subscribe fails
- **WHEN** the order-plant subscription fails after ticker and history plants started
- **THEN** all started plants SHALL be torn down and the credential flock SHALL be released

### Requirement: Packaging and lake client unaffected
The wheel SHALL bundle the `rithmic-gateway` binary and SHALL NOT bundle any fake-plant binary. The pure-Python `rithmic_gateway` package SHALL remain importable without Nautilus or the extension.

#### Scenario: Wheel contents
- **WHEN** the wheel is built by `scripts/build_wheel.sh`
- **THEN** it SHALL contain the `rithmic-gateway` binary and SHALL NOT contain `rithmic-gateway-fake`

#### Scenario: Lake import
- **WHEN** `rithmic_gateway.GatewayClient` is imported in an environment with neither Nautilus nor the extension installed
- **THEN** the import SHALL succeed
