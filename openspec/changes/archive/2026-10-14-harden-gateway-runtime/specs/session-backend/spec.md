## ADDED Requirements

### Requirement: Event streams are awaitable and owned by the consumer
The `Session` trait SHALL provide the consumer with owned, awaitable receivers for the ticker, history, PnL and order streams, obtained under a short lock and consumed without holding any session lock. A consumer SHALL re-obtain its receivers after a reconnect, identified by a stream epoch.

#### Scenario: Consumer awaits without a lock
- **WHEN** the gateway pump waits for the next event
- **THEN** it SHALL hold no session gate and no session mutex while waiting

#### Scenario: Receivers after reconnect
- **WHEN** the plants reconnect and the stream epoch advances
- **THEN** the consumer SHALL obtain fresh receivers before subscriptions are restored, so no restored event is missed

### Requirement: No lock is held across venue I/O
No session-wide gate or mutex SHALL be held while a place, cancel, modify, history load, subscribe or connect round trip is in flight. Such calls SHALL use a cloned plant handle obtained under a short lock, and connect and history SHALL have an explicit deadline.

#### Scenario: Cancel during a history load
- **WHEN** a history load is in flight and `cancel_order` is called
- **THEN** the cancel SHALL be sent without waiting for the history load

#### Scenario: Poll during connect retry
- **WHEN** one plant is retrying its connection
- **THEN** event retrieval from the other plants SHALL continue

#### Scenario: Connect deadline
- **WHEN** a plant cannot connect within its deadline
- **THEN** the connect SHALL fail with a typed error instead of retrying forever

### Requirement: A lag is a typed gap event in both modes
A skipped-message condition on a plant channel SHALL be represented as a typed gap event carrying the stream and the skipped count, delivered through the same event path as data in both modes.

#### Scenario: Lag becomes a gap
- **WHEN** a plant broadcast receiver reports `Lagged(n)`
- **THEN** the session SHALL surface a gap event with `skipped = n` and SHALL NOT log and discard it

## MODIFIED Requirements

### Requirement: Trading gate parity
The system SHALL deny trading RPCs (place, cancel, modify, bracket place/adjust, order and bracket subscribe, working-order load, RMS loads) when trading is disabled, in both modes, and the denial SHALL be enforced inside the Rust session so that no caller can bypass it. In gateway mode the gate SHALL be owned by the parent and a client SHALL NOT be able to elevate it. Plant-wide cancel-all SHALL be governed by its own independent gate (`RITHMIC_GATEWAY_CANCEL_ALL`) that does not require trading to be enabled.

#### Scenario: Place with trading disabled
- **WHEN** `place_order` is called with trading disabled in either mode
- **THEN** the call SHALL fail with a trading-disabled error and no order SHALL be sent to the venue

#### Scenario: Direct mode without the Python client
- **WHEN** a direct-mode Rust session with trading disabled receives `place_order` directly, bypassing the Python exec client
- **THEN** the call SHALL fail with a trading-disabled error

#### Scenario: Cancel-all without its flag
- **WHEN** `cancel_all_orders` is called and the cancel-all gate is off
- **THEN** the call SHALL be denied even if trading is enabled

#### Scenario: Cancel-all with its flag but trading off
- **WHEN** the cancel-all gate is on and trading is disabled
- **THEN** cancel-all SHALL be permitted and place SHALL still be denied

### Requirement: Outcome classes preserved across modes
The system SHALL classify every failed state-changing call as exactly one of: a local deny or not-sent failure (nothing reached the venue), a venue rejection (the venue answered with a definitive reject), or an unknown in-flight outcome (the call may have reached the venue). The class SHALL be assigned where the error originates and SHALL be preserved across the trait, the gateway wire and the Python boundary, in both modes. State-changing place, cancel, and modify RPCs SHALL NOT be retried automatically.

#### Scenario: Socket drops after place was written
- **WHEN** the gateway connection is lost after a place RPC was written and before a reply was read
- **THEN** the call SHALL return an unknown-outcome error and SHALL NOT be re-sent

#### Scenario: Gate denial before send
- **WHEN** a place RPC is denied by the trading gate
- **THEN** the error SHALL be classed as a local deny, not as a venue rejection

#### Scenario: Definitive venue reject
- **WHEN** the venue answers a place with a synchronous reject
- **THEN** the error SHALL be classed as a venue rejection, not as unknown, and the Python client SHALL emit a rejection rather than latch the plant

#### Scenario: Pre-send failure after submit intent
- **WHEN** the order plant cannot be ensured or the session is disconnecting before anything is written
- **THEN** the error SHALL be classed as not-sent, in both modes
