## MODIFIED Requirements

### Requirement: Protocol compatibility with the lake client
The Rust client and the pure-Python single-connection `rithmic_gateway.GatewayClient` SHALL both be exercised by the same integration harness against the same gateway binary, with `session.proto` as the single contract. The Python package SHALL NOT contain a multiplexing runtime; sharing one socket across sessions is the Rust client's responsibility.

#### Scenario: Both clients pass the shared harness
- **WHEN** the integration harness runs against the fake-plant gateway binary
- **THEN** the same scenarios (subscribe, fan-out, gates, restore, re-dial) SHALL pass for the Rust client and the Python `GatewayClient`

#### Scenario: Python package imports without a mux
- **WHEN** `rithmic_gateway` is imported
- **THEN** it SHALL expose `GatewayClient` and SHALL NOT expose `GatewayMux`, `MuxGatewayClient`, `GatewayRuntimeRegistry`, or `create_gateway_client`

#### Scenario: Wide history window is sequential
- **WHEN** `load_time_bars_range` splits a window into slices
- **THEN** the slices SHALL be fetched sequentially over the one connection and the merged result SHALL be ordered and deduplicated
