# gateway-client Specification

## Purpose
The Rust gateway client: attach to an explicitly started parent, share one socket per process with typed event routing, recover the transport with a generation counter, classify outcomes, and load history in chunks.
## Requirements

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
For a given credential fingerprint, a process SHALL use one gateway socket and one reader that routes inbound frames to pending RPC replies and to separate bounded queues for market-data, history, PnL, and order events. A queue SHALL receive events only after its channel is armed by the first subscribe or poll of that channel, and events SHALL be shared between queues by reference rather than deep-copied. A busy or slow channel SHALL NOT block another channel's delivery. A gap event SHALL be delivered to the affected channel's queue and surfaced to the adapter.

#### Scenario: Data and exec share the socket
- **WHEN** the data factory and exec factory both create gateway sessions in one process
- **THEN** a single socket connection to the parent SHALL be used

#### Scenario: Order events during heavy tick flow
- **WHEN** market-data events are arriving faster than they are consumed
- **THEN** order events SHALL still be delivered to the order queue

#### Scenario: Unarmed channel receives nothing
- **WHEN** the exec facade has never polled or subscribed the market channel
- **THEN** no market event SHALL be queued or copied for it

#### Scenario: Subscribe then immediate event
- **WHEN** a channel is armed by subscribe and an event arrives before the first poll
- **THEN** the event SHALL be queued, not dropped

#### Scenario: Fan-out queue overflows
- **WHEN** a client's fan-out queue falls further behind than its capacity
- **THEN** the parent SHALL send that client a gap event carrying the exact number of skipped frames and SHALL keep the connection open

#### Scenario: Client socket stops draining
- **WHEN** a client's socket accepts no data for longer than the parent's write timeout
- **THEN** the parent SHALL close the connection, and the client SHALL surface a transport error on the next call or poll, SHALL NOT silently continue, and SHALL re-attach through the normal recovery path

#### Scenario: Gap event surfaced
- **WHEN** the parent sends a gap event for the ticker stream
- **THEN** the client SHALL deliver it to the market queue and the adapter SHALL run its ticker resync

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
The Rust client and the pure-Python single-connection `rithmic_gateway.GatewayClient` SHALL both be exercised by the same integration harness against the same gateway binary, with `session.proto` as the single contract. The Python package SHALL NOT contain a multiplexing runtime; sharing one socket across sessions is the Rust client's responsibility.

#### Scenario: Both clients pass the shared harness
- **WHEN** the integration harness runs against the fake-plant gateway binary
- **THEN** the same scenarios (subscribe, fan-out, gates, restore, re-dial) SHALL pass for the Rust client and the Python `GatewayClient`

#### Scenario: Python package imports without a mux
- **WHEN** `rithmic_gateway` is imported
- **THEN** it SHALL expose `GatewayClient` and SHALL NOT expose `GatewayMux`, `MuxGatewayClient`, `GatewayRuntimeRegistry`, or `create_gateway_client`

#### Scenario: Wide history window is sequential
- **WHEN** `load_time_bars_range` splits a window into slices
- **THEN** the slices SHALL be fetched sequentially over the one connection and the merged result SHALL be ordered and deduplicated

### Requirement: Slow RPCs do not block other RPCs on the socket
The gateway server SHALL read requests on a dedicated per-connection reader, process state-changing and subscription commands in arrival order on a command lane, and run slow read RPCs (history loads) on a separate bounded lane. A slow RPC SHALL NOT delay a command, an event write or the reading of the next frame. A frame read SHALL be cancel-safe.

#### Scenario: Cancel behind a history load
- **WHEN** a client issues `load_time_bars` and then `cancel_order` on the same socket
- **THEN** the cancel reply SHALL arrive without waiting for the history reply

#### Scenario: Command order preserved
- **WHEN** a client issues place then cancel for the same order on one socket
- **THEN** the server SHALL process them in that order

#### Scenario: Partial frame during event write
- **WHEN** an outbound event is written while a request frame is partly received
- **THEN** the request SHALL be decoded intact with no stream desync

### Requirement: A plant reconnect keeps clients attached
When the parent rebuilds its plants it SHALL restore every remembered subscription intent itself and SHALL keep attached clients connected, sending each a `TransportResumed` event instead of closing the connection. A client SHALL NOT reset the shared ticker plant or re-issue market-data subscriptions in response; it MAY re-read its account state (working-order drain, a fresh PnL subscription) before trading resumes. A market-data intent that cannot be restored after a bounded number of passes SHALL be left degraded without holding the gateway not ready, and a client's next subscribe for that key SHALL retry it. An account-level intent (PnL, orders, brackets) that cannot be restored SHALL cause a full reconnect and keep the gateway not ready.

#### Scenario: Reconnect during a flood
- **WHEN** the plants drop during a 10,000 events/s flood
- **THEN** each attached client SHALL receive `TransportResumed` on its existing connection, the venue SHALL see one subscribe per intent and no unsubscribe, and no event SHALL be lost without a gap

#### Scenario: One intent keeps failing
- **WHEN** a market-data intent fails on every restore pass
- **THEN** the gateway SHALL become ready with the other intents live, and a later subscribe for that key SHALL succeed once the venue accepts it

### Requirement: Intents are refcounted per facade
The Rust client runtime SHALL count each subscription intent (ticker, book, time bars, PnL, order, brackets) per facade. It SHALL send an unsubscribe only when no facade holds the intent any more (1→0). Every subscribe SHALL be sent (the parent treats a repeat as a no-op; a repeat is how a client re-joins a degraded or lost intent). A facade's disconnect or drop SHALL release only that facade's intents, including when the facade is dropped without disconnecting. Requests for one intent SHALL reach the parent in the order they were decided, and a slow request for one intent SHALL NOT hold up requests for another. The runtime key SHALL include `account_id`, `fcm_id` and `ib_id`.

#### Scenario: Two facades share a symbol
- **WHEN** facades A and B both subscribe ES and A unsubscribes
- **THEN** B SHALL keep receiving ES events

#### Scenario: Exec facade stops
- **WHEN** one exec facade calls `disconnect_order_plant` while another facade holds an order intent
- **THEN** the order stream SHALL continue for the other facade

#### Scenario: Different accounts
- **WHEN** two configs differ only in `account_id`
- **THEN** they SHALL NOT share a socket or handshake

### Requirement: Error classes for malformed and failed commands
A `bad_request` reply SHALL map to `NotSent`. For one-shot commands, any parent error other than a venue reject or not-sent SHALL map to `OutcomeUnknown`.

#### Scenario: Undecodable request
- **WHEN** the parent cannot decode a request frame
- **THEN** the client SHALL receive `NotSent` for that request id and the connection SHALL stay up

#### Scenario: Unclassified plant error on cancel
- **WHEN** a cancel fails with a plant error that is neither a reject nor not-sent
- **THEN** the client SHALL return `OutcomeUnknown`

### Requirement: History retries do not compound
A history slice SHALL be retried by the plant layer only. The client SHALL add at most one read retry, so one slice never costs more than two client attempts.

#### Scenario: Transport error on a slice
- **WHEN** a bar slice fails with a transport error
- **THEN** the client SHALL issue at most two attempts for that slice

### Requirement: The pure-Python client keeps frames aligned
The pure-Python `GatewayClient` SHALL buffer partial reads so a non-blocking poll never discards bytes. It SHALL bound pending replies, report a send failure as not sent, and report a failure after the request was written as unknown.

#### Scenario: Partial frame on a non-blocking poll
- **WHEN** a poll with `timeout_ms=0` reads half a frame
- **THEN** the next poll SHALL complete that frame and decode it
