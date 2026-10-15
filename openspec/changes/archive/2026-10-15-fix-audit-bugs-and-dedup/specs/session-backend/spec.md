## ADDED Requirements

### Requirement: Order commands never wait behind plant I/O
A place, cancel or modify on a live order plant SHALL obtain its order handle without waiting for any other plant's connect, reconnect, ticker reset or history readiness probe. A ticker reset SHALL perform its disconnect and reconnect without holding the session-wide lock.

#### Scenario: Cancel during a ticker reset
- **WHEN** a ticker reset is reconnecting the ticker plant and `cancel_order` is called with the order plant already up
- **THEN** the cancel SHALL be sent without waiting for the ticker reconnect

#### Scenario: Account lookup during a reconnect
- **WHEN** `resolved_account` is called while a plant is reconnecting
- **THEN** it SHALL return the cached account without waiting for the reconnect

### Requirement: A failed subscribe tears down only what it opened
A failed order-updates or bracket-updates subscribe SHALL disconnect the order plant only if that subscribe call opened it. The handle of a plant opened by the call SHALL NOT be published to order operations before the call succeeds, so no order operation can be in flight on a plant being rolled back.

#### Scenario: Re-subscribe on a live shared plant fails
- **WHEN** `subscribe_order_updates` fails on an order plant that was already connected
- **THEN** the order plant, its bracket subscription and in-flight orders SHALL be left intact and the error SHALL be returned

### Requirement: A partial history reply is an error
A history, fill-history or RMS reply that carries an error frame SHALL fail the call even when other frames parsed. Tick and bar slices SHALL classify that error through the transient-retry rule; fill history SHALL fail as reconciliation-unavailable.

#### Scenario: Fill history with a mid-reply error
- **WHEN** a fill-history reply contains some rows and one error frame
- **THEN** the call SHALL fail with `ReconciliationUnavailable` and SHALL NOT return the partial rows

### Requirement: History dedup is limited to slice boundaries
Merged sliced history SHALL remove duplicates only between adjacent slices at their shared boundary (the boundary second, or the boundary day for daily and weekly bars), keeping the earlier slice's copy. Identical rows within one slice SHALL be kept.

#### Scenario: Two identical prints inside a slice
- **WHEN** a slice contains two trades with the same timestamp, price, trade count and volume
- **THEN** both SHALL be returned

#### Scenario: Row repeated at a boundary
- **WHEN** adjacent slices both return a row in their shared boundary second
- **THEN** it SHALL be returned once

### Requirement: Stream failure is ordered after queued events
A consumer SHALL receive every event queued before a stream failure, close or plant detach before it receives that failure, close or down signal.

#### Scenario: Event queued just before failure
- **WHEN** the forwarder queues an order notification and then records a failure
- **THEN** the next take SHALL return the notification, and the following take SHALL return the failure

### Requirement: Order snapshot loads do not interleave
`load_orders` and `load_order_history` SHALL be serialised within one session so neither collects the other's replay rows.

#### Scenario: Concurrent loads
- **WHEN** `load_orders` and `load_order_history` are called concurrently
- **THEN** each SHALL return only rows from its own request

### Requirement: A dead forwarder closes its lane
If a stream forwarder task panics, the lane SHALL be marked failed so consumers receive a typed error instead of waiting forever.

#### Scenario: Conversion panics
- **WHEN** event conversion panics inside the forwarder
- **THEN** the next take on that lane SHALL return a failure

### Requirement: Event time comes from the exchange when present
Trade and BBO events SHALL take `ts_event` from the exchange source time fields when the venue provides them, and from the gateway time fields otherwise. The timestamp arithmetic SHALL exist once, in `rithmic-exec`.

#### Scenario: Trade with source time
- **WHEN** a LastTrade carries `source_ssboe`, `source_usecs` and `source_nsecs`
- **THEN** `ts_event` SHALL equal that source time in nanoseconds

### Requirement: Bracket updates are typed events
A venue bracket-update message SHALL be decoded into a typed bracket-update event with the same shape in direct and gateway mode, never into an untyped `Other`.

#### Scenario: Bracket update through the gateway
- **WHEN** the order plant emits a bracket update and a gateway client holds a bracket intent
- **THEN** the client SHALL receive a bracket-update record with the same fields as direct mode

### Requirement: An explicitly disconnected order plant stays down
After `disconnect_order_plant`, order commands SHALL fail as not-sent until the order plant is ensured or subscribed again. Overlapping disconnects SHALL keep the disconnect gate closed until the last one finishes.

#### Scenario: Place after explicit disconnect
- **WHEN** `place_order` is called after `disconnect_order_plant` and before any ensure or subscribe
- **THEN** it SHALL fail with `NotSent` and SHALL NOT reconnect the order plant

### Requirement: Direct mode is testable offline below the session
A fake Rithmic WebSocket server SHALL be available under the `testing` feature, so the unmodified `RithmicSession` and `DirectSession` can run against it offline. One contract suite SHALL run against both `FakeSession` and `DirectSession` connected to the fake venue.

#### Scenario: Contract suite
- **WHEN** the contract suite runs
- **THEN** trading-gate denial, `NotSent` versus `OutcomeUnknown` classification and order-plant attach SHALL behave identically for both sessions

#### Scenario: History added mid-session is probed
- **WHEN** `request_plants` adds the history plant after connect
- **THEN** the history readiness probe SHALL run before history loads are served
