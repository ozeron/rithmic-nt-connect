## MODIFIED Requirements

### Requirement: Trading and cancel-all remain independent parent-owned gates
Trading and plant-wide cancel-all SHALL be independent gates owned by the `SessionHost`, closed by default. In direct mode the exec client SHALL open them through `Session.open_gates`, from `enable_trading` and `RITHMIC_GATEWAY_CANCEL_ALL`. In gateway mode the parent SHALL set them from `RITHMIC_ENABLE_TRADING` and `RITHMIC_GATEWAY_CANCEL_ALL`, and a client's `open_gates` SHALL NOT elevate either gate. Cancel-all SHALL NOT require trading.

#### Scenario: Trading off by default
- **WHEN** no trading setting is present
- **THEN** place, cancel and modify SHALL be denied

#### Scenario: Cancel-all without trading
- **WHEN** the cancel-all gate is open and trading is disabled
- **THEN** cancel-all SHALL be permitted and place SHALL still be denied

#### Scenario: Same exec code in both modes
- **WHEN** the exec client starts in either mode
- **THEN** it SHALL call `open_gates` once, with no mode branch, and SHALL log a warning if trading was requested but the returned gates deny it
