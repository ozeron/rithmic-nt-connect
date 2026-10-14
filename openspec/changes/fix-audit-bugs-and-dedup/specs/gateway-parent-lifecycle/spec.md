## MODIFIED Requirements

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

## ADDED Requirements

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
