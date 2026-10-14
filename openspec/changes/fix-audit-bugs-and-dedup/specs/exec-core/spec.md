## ADDED Requirements

### Requirement: Notification classification and coercion rules live in the crate
The notification-to-action classification (`notification_action`), the price sentinel rule (`sentinel_none`) and closed-set enum coercion (`enum_int`) SHALL be implemented once in `rithmic-exec` and called from Python. The former Python implementations SHALL remain only as test oracles.

#### Scenario: Parity with the oracle
- **WHEN** the parity suite runs the Rust rules and the Python oracles over the same inputs
- **THEN** their outputs SHALL be equal

### Requirement: One listed-contract rule
Detecting whether a symbol is a listed contract (root plus month code plus year) SHALL use `rithmic-exec` `product_root` everywhere, including the history readiness probe.

#### Scenario: Two-digit year
- **WHEN** the history probe is given `ESZ26`
- **THEN** it SHALL treat it as a contract and SHALL NOT resolve a front month for it
