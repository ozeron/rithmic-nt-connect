## ADDED Requirements

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

## MODIFIED Requirements

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

#### Scenario: Consumer lags past capacity
- **WHEN** the parent drops this client because its outbound queue overflowed (the parent closes the connection; it sends no overflow frame)
- **THEN** the client SHALL surface the closed connection as a transport error on the next call or poll, SHALL NOT silently continue, and SHALL re-attach through the normal recovery path

#### Scenario: Gap event surfaced
- **WHEN** the parent sends a gap event for the ticker stream
- **THEN** the client SHALL deliver it to the market queue and the adapter SHALL run its ticker resync
