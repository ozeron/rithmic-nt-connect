## MODIFIED Requirements

### Requirement: Single Rust definition of the plant surface
The system SHALL define every plant operation the adapter uses (ticker and book subscribe/unsubscribe, front month and reference data, tick and time-bar history, time-bar subscribe/unsubscribe, PnL, order updates, bracket updates, order place/cancel/modify, bracket place/adjust, cancel-all, working-order load, RMS loads, fill and order history, account resolution, ticker-plant reset, plant ensure/disconnect, batched event delivery through `next_events`) as methods of one Rust `Session` trait. The system SHALL provide exactly two production implementations, `DirectSession` and `GatewaySession`. Each RPC SHALL use the same argument names in the trait, the PyO3 method and the Python stub, and those names SHALL equal the field names of its `session.proto` request message.

#### Scenario: New RPC missing from one mode fails to build
- **WHEN** a method is added to the `Session` trait and only one implementation provides it
- **THEN** the workspace SHALL fail to compile

#### Scenario: Python facade method without a trait method
- **WHEN** the extension `Session` or its stub lists a plant method that has no `Session` trait method
- **THEN** a parity test SHALL fail

#### Scenario: Argument names drift from the proto
- **WHEN** a trait or PyO3 argument name differs from the field name of its proto request message
- **THEN** a parity test SHALL fail

#### Scenario: Single event delivery model
- **WHEN** the `Session` trait, the gateway client, the PyO3 class and the stub are inspected
- **THEN** none SHALL expose `poll_event`, `poll_history_event`, `poll_pnl_event` or `poll_order_event`, and every event consumer SHALL use `next_events`

### Requirement: Direct is a process-wide singleton per credential
The extension SHALL share one in-process login among all direct `Session` objects with the same credential fingerprint (user, system, URL, env, account triple, endpoint variants; never the password). It SHALL take the credential flock before opening any plant. Each direct `Session` object SHALL be one holder:
- `connect` SHALL be idempotent across holders and SHALL succeed when the shared login is already connected.
- `disconnect` SHALL release only that holder.
- The last holder's `disconnect` SHALL tear the plants down and release the flock.
- Dropping an unreleased holder SHALL release the flock and the registry entry without blocking on venue I/O.

Session construction SHALL NOT perform plant I/O. Adding execution plants to a shared session SHALL be a separate call. No Python-side cache, wrapper or holder count SHALL be required.

#### Scenario: Data and exec factories share one login
- **WHEN** the data factory, exec factory, and a standalone market-data connection create direct sessions with the same credentials
- **THEN** exactly one Rithmic login SHALL be opened

#### Scenario: Last holder releases the flock
- **WHEN** the final direct session holder disconnects
- **THEN** another process SHALL be able to acquire the credential flock

#### Scenario: One holder's disconnect does not close the shared login
- **WHEN** two holders share a direct session and one disconnects
- **THEN** the other holder's subscriptions and order commands SHALL keep working

#### Scenario: Concurrent connect from two holders
- **WHEN** the data and exec clients call `connect` on the shared session in parallel
- **THEN** exactly one plant connect SHALL run and both calls SHALL succeed

#### Scenario: Flock held elsewhere
- **WHEN** a direct session is created while another process holds the credential flock
- **THEN** creation SHALL fail with a session-lock error before any plant is opened

#### Scenario: Execution plants requested on a shared market-data session
- **WHEN** an exec factory asks for execution plants on a session first created for market data
- **THEN** the PnL plant SHALL be added to the existing login through `request_plants`, not by opening a second login, and not inside the constructor

#### Scenario: Dropped holder
- **WHEN** the last holder is garbage-collected without calling `disconnect`
- **THEN** the flock and registry entry SHALL be released, and no blocking venue call SHALL run in the drop path
