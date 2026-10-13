# session-backend Specification

## Purpose
TBD - created by syncing change `rust-unified-session`. Update Purpose after archive. Scope: the Rust `Session` trait as the single definition of the plant surface, with `DirectSession` and `GatewaySession`.

## Requirements

### Requirement: Single Rust definition of the plant surface
The system SHALL define every plant operation the adapter uses (ticker and book subscribe/unsubscribe, front month and reference data, tick and time-bar history, time-bar subscribe/unsubscribe, PnL, order updates, bracket updates, order place/cancel/modify, bracket place/adjust, cancel-all, working-order load, RMS loads, account resolution, ticker-plant reset, plant ensure/disconnect, event polling) as methods of one Rust `Session` trait, and SHALL provide exactly two implementations: `DirectSession` and `GatewaySession`.

#### Scenario: New RPC missing from one mode fails to build
- **WHEN** a method is added to the `Session` trait and only one implementation provides it
- **THEN** the workspace SHALL fail to compile

#### Scenario: Python facade method without a trait method
- **WHEN** the Python `WireSession` protocol lists a plant method that has no `Session` trait method
- **THEN** a parity test SHALL fail

### Requirement: Explicit backend selection
The system SHALL select `DirectSession` or `GatewaySession` only from an explicit `connect_mode` (`direct` or `gateway`) and SHALL NOT apply a silent default.

#### Scenario: Missing connect mode
- **WHEN** a session is created with no `connect_mode` and no `RITHMIC_CONNECT_MODE`
- **THEN** creation SHALL fail with a configuration error naming the missing setting

#### Scenario: Gateway mode without gateway address
- **WHEN** `connect_mode` is `gateway` and no gateway address is configured
- **THEN** creation SHALL fail before any socket or plant is opened

### Requirement: RPC kinds are declared and enforced
The system SHALL classify every RPC in a single RPC table as a one-shot command, a subscription intent, or an intent release (the matching unsubscribe / plant disconnect, which drops the refcount and leaves the venue only on the last peer). A one-shot command SHALL apply the trading gate (if gated), call the plant, and map errors, with no fan-out, no intent bookkeeping, and no restore. A subscription intent SHALL attach fan-out, note a refcounted intent, call the plant only on the 0→1 transition, roll back on failure, and be re-issued after a plant reconnect. Order updates and bracket updates SHALL be subscription intents carried on the order-plant stream.

#### Scenario: Subscription intent missing from restore
- **WHEN** an RPC is classified as a subscription intent but has no corresponding restore-plan field and re-issue
- **THEN** the RPC-table test SHALL fail

#### Scenario: One-shot command registers intent
- **WHEN** a place, cancel, modify, or bracket-adjust RPC is executed
- **THEN** no intent SHALL be noted and nothing SHALL be restored after a reconnect

#### Scenario: Bracket updates survive plant reconnect
- **WHEN** a client has subscribed to bracket updates and the order plant drops and reconnects
- **THEN** the bracket subscription SHALL be re-issued and bracket notifications SHALL continue to reach the client

### Requirement: Every request variant is accounted for
The system SHALL fail a test if any request variant in `session.proto` is absent from the RPC table, or any trait method is absent from the RPC table.

#### Scenario: Proto gains a request without a table row
- **WHEN** a new request variant is added to `session.proto`
- **THEN** the RPC-table test SHALL fail until the table, trait, and server dispatch cover it

### Requirement: Trading gate parity
The system SHALL deny trading RPCs (place, cancel, modify, bracket place/adjust, order and bracket subscribe, working-order load, RMS loads) when trading is disabled, in both modes. In gateway mode the gate SHALL be owned by the parent and a client SHALL NOT be able to elevate it. Plant-wide cancel-all SHALL be governed by its own independent gate (`RITHMIC_GATEWAY_CANCEL_ALL`) that does not require trading to be enabled.

#### Scenario: Place with trading disabled
- **WHEN** `place_order` is called with trading disabled in either mode
- **THEN** the call SHALL fail with a trading-disabled error and no order SHALL be sent to the venue

#### Scenario: Cancel-all without its flag
- **WHEN** `cancel_all_orders` is called and the cancel-all gate is off
- **THEN** the call SHALL be denied even if trading is enabled

#### Scenario: Cancel-all with its flag but trading off
- **WHEN** the cancel-all gate is on and trading is disabled
- **THEN** cancel-all SHALL be permitted and place SHALL still be denied

### Requirement: Outcome classes preserved across modes
The system SHALL treat a failure before send as a local deny and a failure after send (transport loss, timeout, channel error) as an unknown in-flight outcome, in both modes. State-changing place, cancel, and modify RPCs SHALL NOT be retried automatically.

#### Scenario: Socket drops after place was written
- **WHEN** the gateway connection is lost after a place RPC was written and before a reply was read
- **THEN** the call SHALL return an unknown-outcome error and SHALL NOT be re-sent

#### Scenario: Gate denial before send
- **WHEN** a place RPC is denied by the trading gate
- **THEN** the error SHALL be classed as a local deny, not as a venue rejection

### Requirement: Identical event records from both modes
The system SHALL emit the same event record shape and field values for the same plant event regardless of backend, produced by a single `PlantEvent` → record conversion.

#### Scenario: Same scripted stream through both backends
- **WHEN** one scripted sequence of ticker, history, PnL, and order events is delivered through `DirectSession` and through `GatewaySession`
- **THEN** the polled records from both SHALL be equal

### Requirement: Direct is a process-wide singleton per credential
The system SHALL share one in-process login among all `DirectSession` users with the same credential fingerprint and SHALL release the credential flock only when the last holder disconnects.

#### Scenario: Data and exec factories share one login
- **WHEN** the data factory, exec factory, and a standalone market-data connection create direct sessions with the same credentials
- **THEN** exactly one Rithmic login SHALL be opened

#### Scenario: Last holder releases the flock
- **WHEN** the final direct session holder disconnects
- **THEN** another process SHALL be able to acquire the credential flock

### Requirement: Direct-only surfaces are structurally unreachable in gateway mode
The system SHALL expose fill-history and order-history replay through a separate `HistoryReplay` trait implemented only by `DirectSession`, and SHALL reject those calls in gateway mode with a typed unsupported-in-gateway-mode error.

#### Scenario: Fill history requested in gateway mode
- **WHEN** `load_fill_history` is called on a gateway-mode session
- **THEN** the call SHALL fail with the unsupported-in-gateway-mode error and SHALL NOT reach the parent

#### Scenario: Gateway gains a history RPC
- **WHEN** a history-replay RPC is added to the gateway proto without being added to the Direct-only list
- **THEN** `tests/test_history_direct_only.py` SHALL fail
