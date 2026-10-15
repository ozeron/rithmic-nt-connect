## ADDED Requirements

### Requirement: History holds the live market-data lock only for its check
A history or probe load SHALL check for live ticker, book or time-bar intents and SHALL release the shared live-market-data lock before the load runs. History loads SHALL stay serialised among themselves on a separate lock. A subscribe that arrives while a load is running SHALL proceed without waiting for it.

#### Scenario: Subscribe and place during a history load
- **WHEN** a slow history load is running on a connection and the same connection sends a subscribe and then a place
- **THEN** both SHALL complete without waiting for the history load to finish

#### Scenario: History while live intents exist
- **WHEN** a history load starts while a live ticker intent exists
- **THEN** it SHALL be refused with `history_denied_live_md`

### Requirement: Bar overflow gaps reach the bar consumer
When a client's forwarder for a symbol topic overflows and the client is subscribed to bars on it, the client SHALL receive a history-stream gap in addition to the ticker gap.

#### Scenario: Bars lost to overflow
- **WHEN** a client subscribed to ticker and bars on one symbol falls behind and frames are skipped
- **THEN** it SHALL receive a gap on the market channel and a gap on the history channel

### Requirement: Session-taking reads do not hold up commands
Reference-data, front-month and resolved-account reads SHALL run on the slow lane, so a slow lookup never delays a later place or cancel on the same connection.

#### Scenario: Slow reference-data lookup
- **WHEN** a reference-data read is slow and a place follows on the same socket
- **THEN** the place SHALL be served without waiting for the read

### Requirement: Order peers learn about an order-plant re-seat
When the parent disconnects and resubscribes the order plant while order intents remain, every connection holding an order intent SHALL receive an order-stream gap. The re-seat SHALL NOT be treated as a connection failure that rebuilds every plant.

#### Scenario: Last bracket holder leaves
- **WHEN** the last bracket intent is released while another connection holds an order intent
- **THEN** that connection SHALL receive an order-stream gap after the re-seat completes
