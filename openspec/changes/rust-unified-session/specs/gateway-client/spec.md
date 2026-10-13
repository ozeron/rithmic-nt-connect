## ADDED Requirements

### Requirement: Attach to an explicitly started gateway parent
The Rust gateway client SHALL connect to an existing parent unix socket and SHALL NOT spawn any process (see `simplify-gateway-modes`, which this change depends on). When the credential flock is held but the socket is not yet accepting, the client SHALL wait up to a bounded attach timeout; otherwise it SHALL fail with `parent_not_running`.

#### Scenario: Parent running
- **WHEN** a parent is listening and holds the credential flock
- **THEN** the client SHALL attach and complete the handshake

#### Scenario: Parent still starting
- **WHEN** the flock is held and the socket begins accepting within the attach timeout
- **THEN** the client SHALL attach successfully

#### Scenario: No parent
- **WHEN** no parent is reachable and no flock is held
- **THEN** connect SHALL fail with `parent_not_running` naming the listen path, and no process SHALL be started

### Requirement: Handshake verifies identity and advertises scopes
The client SHALL authenticate with an auth token only, SHALL NOT transmit the Rithmic password, and SHALL reject a parent whose fingerprint (system, URL, user) does not match the client's configuration. The client SHALL expose the parent's advertised scopes, `trading_enabled`, and `cancel_all_enabled` as read-only values.

#### Scenario: Fingerprint mismatch
- **WHEN** the parent was started for a different system or URL than the client's configuration
- **THEN** the handshake SHALL fail and no RPC SHALL be issued

#### Scenario: Password never on the wire
- **WHEN** a handshake frame is captured
- **THEN** it SHALL contain no password field or value

#### Scenario: Client cannot elevate trading
- **WHEN** the parent reports trading disabled
- **THEN** no client configuration SHALL cause the client to report or obtain trading enabled

### Requirement: One socket per process with typed event routing
For a given credential fingerprint, a process SHALL use one gateway socket and one reader that routes inbound frames to pending RPC replies and to separate bounded queues for market-data, history, PnL, and order events. A busy or slow channel SHALL NOT block another channel's delivery.

#### Scenario: Data and exec share the socket
- **WHEN** the data factory and exec factory both create gateway sessions in one process
- **THEN** a single socket connection to the parent SHALL be used

#### Scenario: Order events during heavy tick flow
- **WHEN** market-data events are arriving faster than they are consumed
- **THEN** order events SHALL still be delivered to the order queue

#### Scenario: Consumer lags past capacity
- **WHEN** the parent signals fan-out overflow for this client
- **THEN** the client SHALL surface an explicit overflow error and SHALL NOT silently drop events

### Requirement: Single-flight transport recovery with generation
On socket loss the client SHALL perform one shared re-dial for all waiters, SHALL increment a transport generation, and SHALL expose the generation so the adapter can re-arm order-plant state and replay market-data intent. Subscription restore after plant reconnect SHALL remain the parent's responsibility.

#### Scenario: Concurrent callers during a drop
- **WHEN** several threads issue RPCs while the socket is down
- **THEN** exactly one re-dial SHALL occur and the generation SHALL increase by one

#### Scenario: Read-only RPC in flight at drop
- **WHEN** a read-only RPC was in flight when the socket dropped
- **THEN** the client MAY retry it once after the re-dial

#### Scenario: State-changing RPC in flight at drop
- **WHEN** a place, cancel, or modify RPC was written and the socket dropped before the reply
- **THEN** the client SHALL return an unknown-outcome error and SHALL NOT retry

#### Scenario: Re-dial fails
- **WHEN** the re-dial cannot reach the parent within its bounded window
- **THEN** pending and subsequent RPCs SHALL fail with a transport error rather than hang

### Requirement: Requests always complete
Every RPC and every bounded poll SHALL return data, an empty result, or an error within its timeout and SHALL NOT hang.

#### Scenario: Parent stops answering
- **WHEN** the parent accepts a request and never replies
- **THEN** the call SHALL fail with a timeout error after its configured timeout

### Requirement: Client-side chunked history range loads
The client SHALL split a wide time-bar window into calendar slices, fetch each with its own timeout, and return the merged result sorted and deduplicated, so a single RPC never holds the parent's session mutex for a whole multi-month window.

#### Scenario: Multi-month 1m window
- **WHEN** a multi-month 1-minute bar window is requested
- **THEN** it SHALL be fetched as multiple slice RPCs and the merged output SHALL be strictly ordered by time with no duplicate bars

#### Scenario: Slice fails transiently
- **WHEN** a slice returns a transient timeout
- **THEN** the client SHALL retry that slice a bounded number of times before failing the whole request

### Requirement: Parent-enforced history refusal is surfaced
The client SHALL surface the parent's refusal of history loads while a live ticker intent is active as a typed error and SHALL NOT attempt to bypass it.

#### Scenario: History while ticker live
- **WHEN** `load_ticks` is called while the parent has a live ticker intent
- **THEN** the client SHALL return the parent's denial error unchanged

### Requirement: Lifecycle and cleanup
Disconnecting the last client in a process SHALL close the socket and release client-held resources. Parent idle-exit behavior SHALL remain parent-side. A parent SHALL NOT be terminated by client disconnect.

#### Scenario: Last client disconnects
- **WHEN** the last gateway session in a process disconnects
- **THEN** the process SHALL hold no open gateway socket and no flock

#### Scenario: Partial connect failure
- **WHEN** connect fails after the socket was opened but before the session is ready
- **THEN** the socket SHALL be closed and no client-held resource SHALL leak

### Requirement: Protocol compatibility with the lake client
The Rust client and the pure-Python `rithmic_gateway` client SHALL both be exercised by the same integration harness against the same gateway binary, with `session.proto` as the single contract.

#### Scenario: Both clients pass the shared harness
- **WHEN** the integration harness runs against the fake-plant gateway binary
- **THEN** the same scenarios (subscribe, fan-out, gates, restore, re-dial) SHALL pass for the Rust client and the Python client
