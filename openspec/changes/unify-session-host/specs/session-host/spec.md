## ADDED Requirements

### Requirement: One session host per credential, used by both modes
The system SHALL run every Rithmic login through a `SessionHost`. A process SHALL hold at most one host per credential fingerprint (user, system, URL, env, account triple, endpoint variants; never the password). Direct mode SHALL attach sessions to a host in the adapter's process. Gateway mode SHALL attach sessions to the parent's host over the socket. Both modes SHALL use the same `SessionHost` implementation for intents, fan-out, gates and recovery.

#### Scenario: Data and exec clients share one host in direct mode
- **WHEN** the data and exec clients start in direct mode with the same credentials
- **THEN** exactly one host and one Rithmic login SHALL exist in the process, and each client SHALL hold its own attached session

#### Scenario: Same behaviour through either attachment
- **WHEN** the shared scenario suite runs against a `LocalSession` and against a `RemoteSession` attached to the same kind of host
- **THEN** every scenario SHALL produce the same results, events and errors in both

#### Scenario: Flock before login
- **WHEN** a host is started while another process holds the credential flock
- **THEN** starting SHALL fail with a session-lock error before any plant is opened

### Requirement: Sessions are independent attachments
Each attached session SHALL have its own event queues and its own set of intents on the host. A repeated subscribe from the same session SHALL be idempotent, and SHALL re-join the intent if it is degraded. `connect` on a session SHALL ensure the host is connected: it SHALL succeed when the host is already up, and concurrent callers SHALL cause at most one plant connect. `disconnect_order_plant` SHALL release only this session's order and bracket intents. This session's order commands SHALL then fail as not-sent until it ensures or subscribes again, while other sessions keep trading. Detaching a session, by `disconnect` or by dropping it, SHALL release only that session's intents and queues. The host SHALL leave the venue for an intent only when no attached session still holds it. Dropping a session SHALL NOT block on venue I/O. When the last session detaches, the host SHALL disconnect the venue and release the flock.

#### Scenario: Two sessions share a symbol
- **WHEN** sessions A and B subscribe ES on one host and A unsubscribes
- **THEN** B SHALL keep receiving ES events and the venue SHALL see no unsubscribe

#### Scenario: Repeated subscribe
- **WHEN** a session subscribes ES twice and unsubscribes once
- **THEN** the session SHALL no longer hold ES, and the venue SHALL be left only if no other session holds it

#### Scenario: One session stops trading
- **WHEN** session A calls `disconnect_order_plant` while session B holds an order intent
- **THEN** A's `place_order` SHALL fail as not-sent, and B SHALL keep receiving order notifications and SHALL be able to place orders

#### Scenario: Dropped session
- **WHEN** a session is dropped without calling `disconnect`
- **THEN** its intents SHALL be released, other sessions SHALL be unaffected, and the drop SHALL return without waiting for the venue

#### Scenario: Last session leaves
- **WHEN** the last attached session disconnects
- **THEN** the venue SHALL be disconnected and another process SHALL be able to take the credential flock

#### Scenario: Slow consumer
- **WHEN** one session stops draining its queue
- **THEN** that session SHALL receive a typed gap and other sessions SHALL keep receiving every event

### Requirement: The host applies RPC kinds from one table
The host SHALL apply each RPC according to its row in `RPC_TABLE`:
- A one-shot command SHALL apply its gate, call the venue once, map errors, and register no intent.
- A subscription intent SHALL apply its gate, note the intent for the calling session, call the venue only on the host-wide 0→1 transition, and roll back on failure.
- An intent release SHALL drop the calling session's count and call the venue only on the host-wide 1→0 transition.
- A read SHALL apply its gate and call the venue.

The table SHALL live in `rithmic-host`. Its coverage tests (proto variants, trait methods, restore plan, lanes) SHALL keep passing.

#### Scenario: Subscription intent without a restore entry
- **WHEN** an RPC is classified as a subscription intent but has no restore-plan field
- **THEN** the RPC-table test SHALL fail

#### Scenario: One-shot command after reconnect
- **WHEN** a place, cancel or modify was issued before a plant reconnect
- **THEN** nothing SHALL be re-issued for it after the reconnect

### Requirement: Gates are host state with one call to open them
Trading and cancel-all gates SHALL be state of the host, closed by default and independent of each other. A gateway parent SHALL set them from its own environment at start. `Session.open_gates(trading, cancel_all)` SHALL be the only way a consumer requests gates:
- On a session attached in-process to a host the adapter owns, it SHALL open the requested gates (it SHALL never close a gate) and return the effective gates.
- On a `RemoteSession`, or on a session the gateway server attached for a socket connection, it SHALL NOT change any gate and SHALL return the gates the parent grants.

#### Scenario: Direct exec client enables trading
- **WHEN** the exec client with `enable_trading=True` calls `open_gates(trading=True, cancel_all=False)` on a direct session
- **THEN** `place_order` SHALL be permitted on every session attached to that host

#### Scenario: Gateway client cannot elevate
- **WHEN** a gateway-mode exec client calls `open_gates(trading=True, ...)` on a parent started without `RITHMIC_ENABLE_TRADING`
- **THEN** the gates SHALL stay closed, the call SHALL return `trading=False`, and `place_order` SHALL be denied

#### Scenario: Gate bypass from Rust
- **WHEN** a session on a host with trading closed receives `place_order` directly
- **THEN** the call SHALL fail with a trading-disabled error and nothing SHALL reach the venue

### Requirement: The host owns plant recovery in both modes
The host SHALL detect a plant failure through its event pump and SHALL recover the smallest unit that restores service:
- a ticker or history failure SHALL recreate that plant only, leaving sibling plants untouched
- an order-plant failure SHALL re-seat the order plant
- a login-level failure SHALL reconnect all plants

The host SHALL then re-issue every intent the recovered plants carry. A market-data intent that fails a bounded number of restore passes SHALL be left degraded, and the next subscribe for it SHALL retry it. An account intent (PnL, orders, brackets) that cannot be restored SHALL escalate to a full reconnect. Consumers SHALL NOT reset plants or re-issue intents in response to a host recovery. During recovery, `next_events` SHALL return empty batches rather than a stream error. A consumer SHALL see a stream error only after `RecoveryFailed` or after its session is closed.

#### Scenario: Ticker plant fails while trading
- **WHEN** the ticker plant fails while sessions hold ticker, book, PnL and order intents
- **THEN** only the ticker plant SHALL be recreated, ticker and book intents SHALL be re-issued once each, and the PnL and order plants SHALL not be reconnected

#### Scenario: Consumer stream during recovery
- **WHEN** the ticker plant fails and is recovered while the data client waits in `next_events`
- **THEN** the call SHALL return empty batches (or the events queued before the failure), and SHALL NOT raise

#### Scenario: Direct mode recovers without the adapter
- **WHEN** a plant fails in direct mode
- **THEN** the host SHALL restore it and its intents without any call from the Python adapter

#### Scenario: One market-data intent keeps failing
- **WHEN** a market-data intent fails every restore pass
- **THEN** the other intents SHALL be live, the failing one SHALL be degraded, and a later subscribe for it SHALL succeed once the venue accepts it

### Requirement: Sessions receive the same recovery events in both modes
Every attached session SHALL receive `Interrupted` when the path from it to the venue breaks. It SHALL then receive either `Resumed { generation }`, once its intents are live again, or `RecoveryFailed`, after recovery gives up. The generation SHALL increase by one per completed recovery. A `RemoteSession` SHALL report socket loss and re-attach as the same three events, and SHALL re-issue its own recorded intents before emitting `Resumed`. A one-shot command in flight when the path broke SHALL return an unknown-outcome error and SHALL NOT be retried.

#### Scenario: Plant reconnect seen by a remote session
- **WHEN** the parent's host recovers a plant
- **THEN** each attached `RemoteSession` SHALL emit `Interrupted` then `Resumed` on its existing connection

#### Scenario: Socket loss seen by a remote session
- **WHEN** the socket to the parent drops and re-dials successfully
- **THEN** the `RemoteSession` SHALL re-issue its recorded intents and SHALL emit `Interrupted` then `Resumed`, and its consumer SHALL NOT re-issue market-data intents

#### Scenario: Command in flight at the break
- **WHEN** `place_order` was sent and the path broke before the reply
- **THEN** the call SHALL fail as unknown outcome and SHALL NOT be sent again

### Requirement: The venue is a port with a fake
The host SHALL drive the venue only through the `Venue` trait defined in `rithmic-host`. `RithmicVenue` (in `rithmic-plants`) SHALL be the only production implementation. `FakeVenue` (feature `testing`) SHALL be able to script plant events, fail a plant, reject a request, and record calls. No crate other than `rithmic-plants` SHALL depend on `rithmic-rs`.

#### Scenario: Host tests without a network
- **WHEN** the host test suite runs
- **THEN** it SHALL use `SessionHost<FakeVenue>` and SHALL open no network connection

#### Scenario: Venue SDK does not leak
- **WHEN** the dependency graph of `rithmic-host`, `rithmic-gateway` (library) and `rithmic-nt-connect` is inspected
- **THEN** only `rithmic-plants` SHALL depend on `rithmic-rs` directly

### Requirement: The gateway is a transport around a host
The gateway server SHALL attach one `LocalSession` per accepted connection, and SHALL serve each request by decoding the frame, calling that session and encoding the result. Its lanes SHALL read the `lane` column of `RPC_TABLE`. Handshake, authentication, idle exit and the process singleton SHALL stay in the gateway. The server and `RemoteSession` SHALL accept any bidirectional byte stream, and only the composition roots SHALL dial or listen on unix sockets. The only wire change SHALL be one additive `TransportInterrupted` event; existing messages and their behaviour SHALL NOT change, and clients that do not know the new event SHALL keep working.

#### Scenario: In-memory gateway round trip
- **WHEN** a `RemoteSession` and a server on a `SessionHost<FakeVenue>` are connected through an in-memory duplex stream
- **THEN** the shared scenario suite SHALL pass with no socket file

#### Scenario: Old client ignores the new event
- **WHEN** a client built before this change receives `TransportInterrupted`
- **THEN** it SHALL ignore it and keep serving its subscriptions

#### Scenario: Lake client compatibility
- **WHEN** the pure-Python lake `GatewayClient` talks to the new parent
- **THEN** its existing tests SHALL pass unchanged

### Requirement: Configuration is resolved at the composition roots
Only the PyO3 module and the gateway binary SHALL read environment variables or credentials. Every crate below them SHALL receive configuration as values, and the credential-lock location SHALL be a parameter. The PyO3 `Session` SHALL hold its implementation as a `Session` trait object. With feature `testing`, it SHALL offer `Session.fake(...)`, which attaches to a `SessionHost<FakeVenue>`.

#### Scenario: Python tests on a fake host
- **WHEN** a Python test creates `Session.fake(...)` and runs the data client against it
- **THEN** subscriptions, gaps and recovery events SHALL come from the real host logic, with no Rithmic credentials

#### Scenario: No env reads below the roots
- **WHEN** `rithmic-host`, `rithmic-plants` and the gateway library are searched for environment reads
- **THEN** none SHALL be found outside test code

### Requirement: History refusal under live market data is a root setting
The host SHALL take a history policy at start: `Allow`, or `RefuseUnderLiveMd`. The gateway binary SHALL start its host with `RefuseUnderLiveMd` (today's parent behaviour). The adapter's direct-mode root SHALL start its host with `Allow` (today's direct behaviour). No other code SHALL branch on the connect mode for history.

#### Scenario: Direct history while subscribed
- **WHEN** a direct-mode data client requests bars while subscribed to live ticks for the same symbol
- **THEN** the request SHALL be served, as today

#### Scenario: Gateway history while subscribed
- **WHEN** a gateway client requests history while the parent holds a live ticker intent for the symbol
- **THEN** the request SHALL be refused with `history_denied_live_md`, as today

### Requirement: Test-only surfaces stay out of release builds
`FakeVenue` and `Session.fake` SHALL exist only with the `testing` feature. Release wheels SHALL NOT be built with it.

#### Scenario: Release wheel
- **WHEN** the release wheel is inspected
- **THEN** `rithmic_nt_connect._lib.Session` SHALL have no `fake` attribute

### Requirement: Direct mode keeps its throughput
A `LocalSession` SHALL deliver events to Python at no lower a rate than the pre-change direct path, measured by the FFI throughput benchmark with a baseline recorded before the change.

#### Scenario: Benchmark after switch-over
- **WHEN** `RITHMIC_PERF=1` runs the FFI throughput benchmark in direct mode after group 5
- **THEN** events per second SHALL be no lower than the recorded baseline
