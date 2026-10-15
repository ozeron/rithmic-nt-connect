# gateway-parent-lifecycle Specification

## Purpose
Who starts, owns and stops the gateway parent: clients attach only and never spawn it or hold the Rithmic password, the parent is launched explicitly, and only the parent controls its own lifetime.

## Requirements

### Requirement: Gateway clients never spawn the parent
The gateway client library SHALL NOT start a `rithmic-gateway` process under any configuration. It SHALL attach to a parent that was started explicitly by an operator, a service manager, or a test harness.

#### Scenario: No parent running
- **WHEN** a gateway client dials and no parent is listening and no credential flock is held
- **THEN** the client SHALL fail with error code `parent_not_running` and SHALL NOT start any process

#### Scenario: Error message is actionable
- **WHEN** `parent_not_running` is raised
- **THEN** the message SHALL include the resolved listen path and the launcher command to start the parent

#### Scenario: Library has no spawn path
- **WHEN** the `rithmic_gateway` package is inspected
- **THEN** it SHALL expose no function that starts the gateway binary for a client, and `GatewayConfig` SHALL have no spawn-related fields

### Requirement: Bounded wait for an already-starting parent
When the credential flock is held but the socket is not yet accepting, the client SHALL wait up to a bounded attach timeout for the socket and SHALL fail with a distinct error when the flock is released before the socket appears or the timeout elapses.

#### Scenario: Parent coming up
- **WHEN** the flock is held and the socket begins accepting within the attach timeout
- **THEN** the client SHALL attach successfully

#### Scenario: Flock released before socket
- **WHEN** the flock is released while the client is waiting
- **THEN** the client SHALL fail with `parent_flock_held_no_socket` without waiting for the full timeout

#### Scenario: Wrong listen path
- **WHEN** the flock is held but the configured listen path never accepts within the timeout
- **THEN** the client SHALL fail with `listen_path_mismatch` naming the configured path

### Requirement: Gateway-mode clients hold no Rithmic password
In gateway mode the client configuration SHALL NOT require, read, log, or transmit `RITHMIC_PASSWORD` (or any password alias). The parent SHALL be the only process that holds the Rithmic password. In direct mode the password SHALL remain required.

#### Scenario: Gateway config without password
- **WHEN** a gateway-mode `SessionConfig` is built with user, system, URL, and listen settings but no password
- **THEN** construction SHALL succeed

#### Scenario: Direct config without password
- **WHEN** a direct-mode `SessionConfig` is built without a password
- **THEN** construction SHALL fail with a missing-credential error

#### Scenario: Password never on the client socket
- **WHEN** gateway handshake frames and RPC frames are captured from a gateway-mode client
- **THEN** none SHALL contain a password value, and `repr` of the client config SHALL NOT contain one

### Requirement: Explicit parent launcher
The package SHALL provide `python -m rithmic_gateway` and a `rithmic-gateway` console binary installed by the `rithmic-gateway` wheel. The launcher SHALL resolve the binary in the order `RITHMIC_GATEWAY_BIN`, the wheel's own binary, then `PATH`, and replace the launcher process with it, passing the caller's environment unchanged. It SHALL NOT scan cargo `target/` directories.

#### Scenario: Launch from an installed wheel
- **WHEN** `python -m rithmic_gateway` is run in an environment with the wheel installed and parent credentials set
- **THEN** the wheel's binary SHALL run as the parent with that environment

#### Scenario: Binary cannot be found
- **WHEN** no binary can be resolved
- **THEN** the launcher SHALL exit non-zero with a message naming `RITHMIC_GATEWAY_BIN` and the `rithmic-gateway` wheel

#### Scenario: Launcher does not alter lifetime
- **WHEN** the launcher starts the parent
- **THEN** it SHALL NOT set `RITHMIC_GATEWAY_IDLE_EXIT_SEC` or detach the process

### Requirement: Parent lifetime is controlled only by the parent
The parent's exit behavior SHALL be governed solely by its own configuration: with `RITHMIC_GATEWAY_IDLE_EXIT_SEC` unset or `-1` it SHALL run until terminated. A client disconnect SHALL NOT terminate a parent.

#### Scenario: Last client disconnects from a standalone parent
- **WHEN** the last client disconnects from a parent with idle-exit unset
- **THEN** the parent SHALL keep running and accept a new client

### Requirement: Test harnesses may launch a parent outside the library
Smoke scripts and live e2e tests that need a parent SHALL start it through a harness-side helper that is not importable from the shipped `rithmic_gateway` package, waits for the socket and credential flock, and terminates the process on exit.

#### Scenario: Shared-ticks smoke
- **WHEN** `scripts/smoke_gateway_shared_ticks.py` runs
- **THEN** it SHALL start its parent via the harness helper and two consumers SHALL attach without either spawning anything

#### Scenario: Harness cleanup
- **WHEN** the smoke script exits normally or on error
- **THEN** the parent it started SHALL be terminated and the credential flock released

### Requirement: Idle exit cannot miss the last detach
The idle-exit waiter SHALL register for wakeups before reading its state, so a last-peer detach that races the read still arms the exit.

#### Scenario: Detach races the waiter
- **WHEN** the last peer detaches between the waiter's state read and its wait
- **THEN** the parent SHALL still exit after the idle period

### Requirement: A panicking request does not leak a connection's intents
A panic while serving one request SHALL produce an error reply for that request and SHALL NOT stop the connection's command lane. Connection teardown SHALL always release the connection's intents.

#### Scenario: Handler panics
- **WHEN** a command handler panics
- **THEN** the client SHALL receive an `internal` error for that request, later requests SHALL be served, and on disconnect its intents SHALL be released

### Requirement: Unauthenticated connections are bounded
Before the handshake completes, the parent SHALL enforce a read timeout and a small maximum frame size.

#### Scenario: Silent connection
- **WHEN** a connection sends nothing after connecting
- **THEN** the parent SHALL close it after the handshake timeout

### Requirement: Contenders leave the singleton lock file intact
Probing or failing to acquire the singleton lock SHALL NOT modify the lock file; only the holder SHALL write its pid.

#### Scenario: Losing contender
- **WHEN** a second parent fails to acquire the lock
- **THEN** the lock file SHALL still contain the holder's pid
