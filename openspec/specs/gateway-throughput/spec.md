# gateway-throughput Specification

## Purpose
TBD - created by archiving change harden-gateway-runtime. Update Purpose after archive.
## Requirements

### Requirement: Event delivery is event-driven
The gateway parent, the direct session and the Python pollers SHALL wake on event arrival and SHALL NOT discover events by sleeping for a fixed interval and re-polling. The delivery path SHALL perform no periodic polling: a Python consumer blocks inside the extension on the event wait, which has a bounded timeout (50 ms) only so the consumer can notice cancellation and a closed session. A consumer with nothing to poll (for example the history stream with no bar subscription) MAY sleep between checks.

#### Scenario: Idle wake-to-deliver latency
- **WHEN** a single event is published after the system has been idle
- **THEN** it SHALL reach a subscribed client with p99 latency of at most 2 ms, independent of any poll interval

#### Scenario: No fixed-interval poll in the delivery path
- **WHEN** the pump, `DirectSession` event retrieval and the Python poll loops are inspected
- **THEN** none SHALL contain a fixed-duration sleep between event retrievals

### Requirement: Sustained and burst throughput budgets
With the fake plant and the real server, client and PyO3 layers, the system SHALL deliver every event without loss at a sustained 50,000 events/s aggregate for 30 s to four attached clients, and SHALL survive a 200,000 events/s burst lasting 1 s with either zero loss or a gap signal whose skipped count equals the actual loss.

#### Scenario: Sustained flood
- **WHEN** 50,000 LastTrade/BBO events/s are published for 30 s to four subscribed clients
- **THEN** each client SHALL receive every event in order and the report SHALL show zero gaps

#### Scenario: Burst beyond buffer
- **WHEN** 200,000 events/s are published for 1 s
- **THEN** every loss SHALL be reported as a gap with an exact skipped count, and no event SHALL be lost without one

### Requirement: Latency budget under load
At a sustained 20,000 events/s, the latency from parent receipt to client delivery SHALL be at most 5 ms at p99 and 20 ms at p99.9.

#### Scenario: Latency at 20k events/s
- **WHEN** 20,000 events/s are published for 30 s and each event carries its creation timestamp
- **THEN** the harness report SHALL show p99 at or below 5 ms and p99.9 at or below 20 ms

### Requirement: Order path is isolated from market data and history
A `place`, `cancel` or `modify` round trip SHALL NOT wait behind a market-data flood, a slow read RPC, or another client's traffic. The budget is absolute because the idle round trip is sub-millisecond, which makes a ratio to idle meaningless; it is set well above the clean-machine result (about 0.5 ms) so that scheduler noise on a busy host does not flip it, and far below the seconds a blocked socket costs.

#### Scenario: Order latency during flood and history load
- **WHEN** a 50,000 events/s flood and a slow read RPC (at least 8 s) run while orders are placed and cancelled on the same socket, with a new place and cancel every 20 ms regardless of earlier ones finishing
- **THEN** order round-trip p99 SHALL be at most 10 ms and the maximum at most 50 ms on a quiet machine, and no flood event SHALL be lost

#### Scenario: Slow consumer does not slow peers
- **WHEN** one client stops reading while three others consume a 20,000 events/s flood
- **THEN** the three peers' throughput SHALL drop by at most 10 percent and the stalled client SHALL be disconnected or told of a gap

### Requirement: No silent loss
Whenever a plant channel, fan-out queue or client queue drops events, a typed gap signal naming the stream and the skipped count SHALL reach every affected consumer in both modes, and the consumer SHALL resynchronize rather than continue as if nothing was lost.

#### Scenario: Plant broadcast lag
- **WHEN** a plant broadcast channel reports it skipped N messages
- **THEN** each client subscribed to that stream SHALL receive a gap event with `skipped = N`

#### Scenario: Gap in direct mode
- **WHEN** the same lag occurs in direct mode
- **THEN** the Python consumer SHALL observe the same typed gap and run its resync

### Requirement: Bounded memory and allocations
The event path from plant receipt to the client socket SHALL perform at most two heap allocations per delivered event (one encoded buffer and one shared reference), SHALL NOT deep-copy an event per consumer, and SHALL keep resident memory growth at or below 5 percent over a 10 minute soak after warmup.

#### Scenario: Allocations per event
- **WHEN** the harness runs with a counting allocator at 20,000 events/s
- **THEN** the report SHALL show at most two allocations per event on the server path

#### Scenario: Soak
- **WHEN** a 10 minute 10,000 events/s soak completes
- **THEN** RSS growth after the first minute SHALL be at most 5 percent

### Requirement: A reproducible load harness enforces the budgets
The repository SHALL contain a synthetic load harness that needs no Rithmic credentials, runs scenarios (flood, mixed orders, slow history, slow consumer, reconnect during flood, direct-mode FFI), and emits a machine-readable report. A fast smoke subset with loose floors SHALL run in CI; the full run and its baseline report SHALL be reproducible from a documented command.

#### Scenario: Report contents
- **WHEN** the harness finishes a scenario
- **THEN** the report SHALL include events sent, events received, gaps and skipped counts, throughput, p50/p99/p999 latency, allocations per event and peak RSS

#### Scenario: CI smoke
- **WHEN** the smoke subset runs in CI
- **THEN** it SHALL fail on any loss without a gap, on any order-isolation violation, or on throughput below the loose floor

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
