## MODIFIED Requirements

### Requirement: Single Rust definition of the plant surface
The system SHALL define the consumer-facing plant surface as one Rust `Session` trait in `rithmic-host`, and the venue-facing surface as one `Venue` trait in the same crate. `Session` covers: ticker and book subscribe/unsubscribe, front month and reference data, tick and time-bar history, time-bar subscribe/unsubscribe, PnL, order updates, bracket updates, order place/cancel/modify, bracket place/adjust, cancel-all, working-order load, RMS loads, fill and order history, account resolution, plant ensure/disconnect, gates, batched event delivery through `next_events`, and session events. `Session` SHALL have exactly two production implementations: `LocalSession` (attached to a host in the same process) and `RemoteSession` (attached to a parent's host over the socket). `Venue` SHALL have exactly one production implementation, `RithmicVenue`, plus `FakeVenue` for tests. Each RPC SHALL use the same argument names in the trait, the PyO3 method and the Python stub, and those names SHALL equal the field names of its `session.proto` request message.

#### Scenario: New RPC missing from one attachment fails to build
- **WHEN** a method is added to the `Session` trait and only one of `LocalSession` / `RemoteSession` provides it
- **THEN** the workspace SHALL fail to compile

#### Scenario: Python facade method without a trait method
- **WHEN** the extension `Session` or its stub lists a plant method that has no `Session` trait method
- **THEN** a parity test SHALL fail

#### Scenario: Venue reset is not a consumer call
- **WHEN** the `Session` trait is inspected
- **THEN** it SHALL NOT expose `reset_ticker`; plant recovery SHALL be internal to the host

#### Scenario: Argument names drift from the proto
- **WHEN** a trait or PyO3 argument name differs from the field name of its proto request message
- **THEN** a parity test SHALL fail

#### Scenario: Single event delivery model
- **WHEN** the `Session` trait, the gateway client, the PyO3 class and the stub are inspected
- **THEN** none SHALL expose `poll_event`, `poll_history_event`, `poll_pnl_event` or `poll_order_event`, and every event consumer SHALL use `next_events`
