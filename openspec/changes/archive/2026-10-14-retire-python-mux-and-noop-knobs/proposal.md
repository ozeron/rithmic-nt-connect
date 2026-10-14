## Why

`simplify-gateway-modes` and `rust-unified-session` left two compatibility layers behind: the deprecated no-op knobs (`CLIENT_MODE=mux`, `SPAWN_POLICY=never`, `AUTO_SPAWN=0`) that are accepted with a warning, and the Python mux stack in `rithmic_gateway` (`mux.py`, `runtime.py`, `transport_recovery.py`). The Rust `GatewaySession` is now the only adapter path and already owns single-socket sharing and transport recovery. The leftovers are ~950 lines of code, three duplicated retired-knob tables, and tests that only exercise the dead copy. Retire them now, while the lake is the only external consumer and attach-only is fresh.

## What Changes

- **BREAKING** `RITHMIC_GATEWAY_CLIENT_MODE=mux`, `RITHMIC_GATEWAY_SPAWN_POLICY=never` and `RITHMIC_GATEWAY_AUTO_SPAWN=0|false|no|off` raise a configuration error like the other retired values, instead of warning once and being ignored. Unset is the only valid state. The two copies of the retired-knob table (`rithmic_gateway.config`, `rithmic_nt_connect.config`) collapse into one message each.
- **BREAKING** Remove `rithmic_gateway.mux`, `rithmic_gateway.runtime` (`GatewayMux`, `MuxGatewayClient`, `GatewayRuntimeRegistry`, `create_gateway_client`) and `rithmic_gateway.transport_recovery`, with their tests. `GatewayClient` stays as the lake-facing, single-connection Python client.
- `load_time_bars_range` becomes sequential-only; the `max_workers > 1` peer path that depended on the mux is removed.
- Fill the placeholder `Purpose` text in the six main specs.
- Sweep stale docs (STATUS, runbook, AGENTS, ADR 0001, superseded plans) to match.
- The four skipped behavior fixes (unread facade queues, per-tick timestamp codec, transport-callback unsubscribe, double gate lock in the event pump) moved to `harden-gateway-runtime`.
- Delete verified dead code, one-off probes and duplicate tests found by the 2026-10-14 audit.

## Capabilities

### New Capabilities
<!-- none -->

### Modified Capabilities
- `supported-modes`: the "Gateway client topology is mux only" requirement is renamed to a single shared socket (no "mux" mode vocabulary), and "Retired gateway client knobs fail fast" no longer accepts any retired value as a no-op.
- `gateway-client`: "Protocol compatibility with the lake client" is narrowed to the Rust client and the single-connection Python `GatewayClient`; the Python mux is no longer part of the shared harness.

## Impact

- Python: delete `python/rithmic_gateway/{mux,runtime,transport_recovery}.py`; edit `client.py` (`load_time_bars_range`), `config.py`, `recovery_bridge.py` comments, `rithmic_nt_connect/config.py`.
- Tests: delete `tests/test_gateway_mux.py`, `tests/test_gateway_runtime.py`, `tests/test_transport_recovery.py`; drop the `python-mux` scenario client in `tests/test_gateway_scenarios.py`; rewrite `tests/test_gateway_retired_knobs.py` and the `mux` env in `tests/test_import_isolation.py`.
- Docs: `AGENTS.md`, `docs/STATUS.md`, `docs/references/ops-runbook.md`, ADR 0001, six main-spec Purpose lines.
- External: the market-data-lake owner must confirm it does not call `create_gateway_client` or pass `max_workers > 1` (extends open task 6.4 of `simplify-gateway-modes`).
- Rust: only dead-code deletions; no behavior change.
