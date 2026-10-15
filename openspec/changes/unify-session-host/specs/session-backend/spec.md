## MODIFIED Requirements

### Requirement: Single Rust definition of the plant surface
The system SHALL define the consumer-facing plant surface as one Rust `Session` trait in `rithmic-host`, and the venue-facing surface as one `Venue` trait in the same crate. `Session` covers: ticker and book subscribe/unsubscribe, front month and reference data, tick and time-bar history, time-bar subscribe/unsubscribe, PnL, order updates, bracket updates, order place/cancel/modify, bracket place/adjust, cancel-all, working-order load, RMS loads, fill and order history, account resolution, plant ensure/disconnect, gates, event delivery and session events. `Session` SHALL have exactly two production implementations: `LocalSession` (attached to a host in the same process) and `RemoteSession` (attached to a parent's host over the socket). `Venue` SHALL have exactly one production implementation, `RithmicVenue`, plus `FakeVenue` for tests.

#### Scenario: New RPC missing from one attachment fails to build
- **WHEN** a method is added to the `Session` trait and only one of `LocalSession` / `RemoteSession` provides it
- **THEN** the workspace SHALL fail to compile

#### Scenario: Python facade method without a trait method
- **WHEN** the Python `WireSession` protocol lists a plant method that has no `Session` trait method
- **THEN** a parity test SHALL fail

#### Scenario: Venue reset is not a consumer call
- **WHEN** the `Session` trait is inspected
- **THEN** it SHALL NOT expose `reset_ticker`; plant recovery SHALL be internal to the host

### Requirement: Explicit backend selection
The system SHALL select a `LocalSession` (`connect_mode=direct`) or a `RemoteSession` (`connect_mode=gateway`) only from an explicit `connect_mode`, and SHALL NOT apply a silent default.

#### Scenario: Missing connect mode
- **WHEN** a session is created with no `connect_mode` and no `RITHMIC_CONNECT_MODE`
- **THEN** creation SHALL fail with a configuration error naming the missing setting

#### Scenario: Gateway mode without gateway address
- **WHEN** `connect_mode` is `gateway` and no gateway address is configured
- **THEN** creation SHALL fail before any socket or plant is opened

### Requirement: Trading gate parity
The system SHALL deny trading RPCs (place, cancel, modify, bracket place/adjust, order and bracket subscribe, working-order load, RMS loads) when trading is disabled, in both modes. The denial SHALL be enforced by the `SessionHost`, so that no caller can bypass it. In gateway mode the gate SHALL be set by the parent, and a client SHALL NOT be able to elevate it. Plant-wide cancel-all SHALL be governed by its own independent gate that does not require trading to be enabled.

#### Scenario: Place with trading disabled
- **WHEN** `place_order` is called with trading disabled in either mode
- **THEN** the call SHALL fail with a trading-disabled error and no order SHALL be sent to the venue

#### Scenario: Direct mode without the Python client
- **WHEN** a `LocalSession` on a host with trading disabled receives `place_order` directly, bypassing the Python exec client
- **THEN** the call SHALL fail with a trading-disabled error

#### Scenario: Cancel-all without its flag
- **WHEN** `cancel_all_orders` is called and the cancel-all gate is off
- **THEN** the call SHALL be denied even if trading is enabled

#### Scenario: Cancel-all with its flag but trading off
- **WHEN** the cancel-all gate is on and trading is disabled
- **THEN** cancel-all SHALL be permitted and place SHALL still be denied

### Requirement: Identical event records from both modes
The system SHALL emit the same event record shape and field values for the same plant event in both modes. The records SHALL be produced by the same host fan-out and a single record conversion.

#### Scenario: Same scripted stream through both attachments
- **WHEN** one scripted sequence of ticker, history, PnL and order events from a `FakeVenue` is delivered through a `LocalSession` and through a `RemoteSession`
- **THEN** the records from both SHALL be equal

### Requirement: Direct is a process-wide singleton per credential
In direct mode, the system SHALL share one `SessionHost`, and so one Rithmic login, among all sessions in the process with the same credential fingerprint. It SHALL release the credential flock only when the last attached session detaches.

#### Scenario: Data and exec factories share one login
- **WHEN** the data factory, exec factory, and a standalone market-data connection create direct sessions with the same credentials
- **THEN** exactly one Rithmic login SHALL be opened

#### Scenario: Last holder releases the flock
- **WHEN** the final attached direct session disconnects
- **THEN** another process SHALL be able to acquire the credential flock

#### Scenario: Execution plants added to a shared host
- **WHEN** an exec factory asks for execution plants on a host first started for market data
- **THEN** the PnL plant SHALL be added to the existing login, not by opening a second login

### Requirement: An explicitly disconnected order plant stays down
After a session calls `disconnect_order_plant`, that session's order commands SHALL fail as not-sent until it ensures the order plant or subscribes to order updates again. Other sessions attached to the same host SHALL keep their order intents and SHALL keep trading. The host SHALL disconnect the order plant only when no attached session holds an order or bracket intent. Overlapping disconnects SHALL keep the disconnect gate closed until the last one finishes.

#### Scenario: Place after explicit disconnect
- **WHEN** a session calls `place_order` after its own `disconnect_order_plant` and before any ensure or subscribe
- **THEN** it SHALL fail with `NotSent` and SHALL NOT reconnect the order plant

#### Scenario: Another session still trades
- **WHEN** session A disconnects the order plant while session B holds an order intent
- **THEN** the order plant SHALL stay connected and B's order commands SHALL succeed

### Requirement: Stream failure is ordered after queued events
A consumer SHALL receive every event queued before a stream failure, close or plant detach before it receives that failure, close or down signal. A plant failure that the host is recovering SHALL NOT reach the consumer as a stream failure. It SHALL be signalled by `Interrupted`, and only `RecoveryFailed` or a closed session SHALL end the consumer's stream with an error.

#### Scenario: Event queued just before failure
- **WHEN** the venue stream queues an order notification and then fails
- **THEN** the consumer SHALL receive the notification, followed by `Interrupted` rather than a stream error

#### Scenario: Recovery gives up
- **WHEN** the host emits `RecoveryFailed` for a plant
- **THEN** the consumer's next take on that stream SHALL return the failure after any queued events
