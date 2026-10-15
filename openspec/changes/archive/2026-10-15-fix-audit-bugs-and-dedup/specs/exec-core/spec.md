## ADDED Requirements

### Requirement: Notification classification lives in the crate
The notification-to-action classification (which action each kind asks for, and a refusal's reason: the venue text, else report text, else status, else a fixed default) SHALL be implemented once in `rithmic-exec` and called from Python. The former Python implementation SHALL remain only as a test oracle. Coercion of arbitrary Python values (`sentinel_none`, `enum_int`) stays at the Python boundary.

#### Scenario: Parity with the oracle
- **WHEN** the parity suite runs the Rust rules and the Python oracles over the same inputs
- **THEN** their outputs SHALL be equal

### Requirement: One listed-contract rule
Detecting whether a symbol is a listed contract (root plus month code plus year) SHALL use `rithmic-exec` `product_root` everywhere, including the history readiness probe.

#### Scenario: Two-digit year
- **WHEN** the history probe is given `ESZ26`
- **THEN** it SHALL treat it as a contract and SHALL NOT resolve a front month for it
