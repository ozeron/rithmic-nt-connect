## RENAMED Requirements

- FROM: `### Requirement: Gateway client topology is mux only`
- TO: `### Requirement: Gateway sessions share one socket per process`

## MODIFIED Requirements

### Requirement: Gateway sessions share one socket per process
In gateway mode, all gateway sessions in one process for the same credential fingerprint SHALL share a single client socket to the parent. No alternative topology (`dual`) and no topology selector SHALL exist.

#### Scenario: Data and exec share one socket
- **WHEN** the data factory and the exec factory both create gateway sessions in one process
- **THEN** exactly one socket to the parent SHALL be used

#### Scenario: Topology selector set
- **WHEN** `RITHMIC_GATEWAY_CLIENT_MODE` is set to any value, including `dual` and `mux`
- **THEN** configuration SHALL fail with an error stating that the variable was removed and must be unset

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
