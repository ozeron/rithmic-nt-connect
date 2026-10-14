# supported-modes Specification

## Purpose
The supported connect modes and their guardrails: explicit `direct` or `gateway` selection, one shared socket per process, unix-only transport, fail-fast retired knobs, venue-environment separation, independent trading and cancel-all gates, and explicit bar source.
## Requirements
### Requirement: Connect mode is explicit and limited to direct or gateway
The adapter SHALL accept exactly two connect modes, `direct` and `gateway`, from `connect_mode` or `RITHMIC_CONNECT_MODE`, and SHALL NOT apply a default when neither is set.

#### Scenario: Unset connect mode
- **WHEN** neither `connect_mode` nor `RITHMIC_CONNECT_MODE` is set
- **THEN** configuration SHALL fail with an error naming the missing setting

#### Scenario: Unknown connect mode
- **WHEN** `RITHMIC_CONNECT_MODE` is any value other than `direct` or `gateway`
- **THEN** configuration SHALL fail and SHALL NOT fall back to either mode

### Requirement: Gateway transport is unix only
The gateway SHALL listen on and clients SHALL dial `unix://` endpoints only. `tcp://` and `tls://` listen URLs SHALL be rejected with an error that states remote transport is not implemented.

#### Scenario: TCP listen URL
- **WHEN** the listen URL scheme is `tcp://`
- **THEN** parsing SHALL fail with a not-implemented error referencing the remote-gateway roadmap

#### Scenario: Relative unix URL
- **WHEN** the listen URL is a relative `unix://` path
- **THEN** it SHALL resolve to an absolute path before bind or dial

### Requirement: Retired gateway client knobs fail fast
Removed client behavior SHALL NOT be silently ignored. A retired variable that is set to any non-empty value SHALL raise a configuration error naming the removal, including values that equal the sole remaining behavior. Unset SHALL be the only accepted state.

#### Scenario: Spawn requested
- **WHEN** `RITHMIC_GATEWAY_SPAWN_POLICY` is `if_missing`, `auto`, or `default`, or `RITHMIC_GATEWAY_AUTO_SPAWN` is truthy
- **THEN** configuration SHALL fail with an error stating client spawn was removed and the parent must be started explicitly

#### Scenario: Attach-only value set explicitly
- **WHEN** `RITHMIC_GATEWAY_SPAWN_POLICY` is `never` or `RITHMIC_GATEWAY_AUTO_SPAWN` is `0`, `false`, `no`, or `off`
- **THEN** configuration SHALL fail with an error stating the variable was removed and must be unset, and SHALL NOT emit a deprecation warning instead

#### Scenario: Unrecognized value
- **WHEN** `RITHMIC_GATEWAY_SPAWN_POLICY` is an unrecognized value
- **THEN** configuration SHALL fail with an error naming the removal

#### Scenario: Both client configs agree
- **WHEN** the same retired variable is set and the adapter config or the `rithmic_gateway` config is built
- **THEN** both SHALL fail, each with its own error type and the same removal message

### Requirement: Venue environment cannot be mixed
The system SHALL resolve the venue environment (Live, Demo, Test) from the system name unless `RITHMIC_ENV` overrides it, and SHALL refuse a Test or Demo system that has no explicit URL.

#### Scenario: Test system without URL
- **WHEN** the system name is a Test or Demo system and no URL is configured
- **THEN** configuration SHALL fail rather than default to the production URL

#### Scenario: Gateway fingerprint includes environment
- **WHEN** a client's resolved environment differs from the parent's
- **THEN** the handshake SHALL fail and no RPC SHALL be issued

### Requirement: Trading and cancel-all remain independent parent-owned gates
Trading SHALL be disabled unless `enable_trading` (direct) or the parent's `RITHMIC_ENABLE_TRADING` (gateway) is set. Plant-wide cancel-all SHALL be governed by `RITHMIC_GATEWAY_CANCEL_ALL` independently of trading. A gateway client SHALL NOT be able to elevate either gate.

#### Scenario: Trading off by default
- **WHEN** no trading setting is present
- **THEN** place, cancel, and modify SHALL be denied

#### Scenario: Cancel-all without trading
- **WHEN** `RITHMIC_GATEWAY_CANCEL_ALL` is set and trading is disabled
- **THEN** cancel-all SHALL be permitted and place SHALL still be denied

### Requirement: Bar source stays explicit
The data client SHALL distinguish INTERNAL (aggregated from ticks) and EXTERNAL (venue time bars) bar subscriptions and SHALL NOT advertise an INTERNAL bar as EXTERNAL.

#### Scenario: One-second bars
- **WHEN** a 1-second bar subscription is requested
- **THEN** it SHALL be served as INTERNAL

#### Scenario: One-minute venue bars
- **WHEN** a 1-minute EXTERNAL bar subscription is requested
- **THEN** it SHALL be served from the venue time-bar stream

### Requirement: Gateway sessions share one socket per process
In gateway mode, all gateway sessions in one process for the same credential fingerprint SHALL share a single client socket to the parent. No alternative topology (`dual`) and no topology selector SHALL exist.

#### Scenario: Data and exec share one socket
- **WHEN** the data factory and the exec factory both create gateway sessions in one process
- **THEN** exactly one socket to the parent SHALL be used

#### Scenario: Topology selector set
- **WHEN** `RITHMIC_GATEWAY_CLIENT_MODE` is set to any value, including `dual` and `mux`
- **THEN** configuration SHALL fail with an error stating that the variable was removed and must be unset

