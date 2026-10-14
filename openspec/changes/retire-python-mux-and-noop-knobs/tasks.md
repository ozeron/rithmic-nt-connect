## 1. Retire no-op knobs

- [x] 1.1 Make `CLIENT_MODE`, `SPAWN_POLICY`, `AUTO_SPAWN` raise for any non-empty value in `python/rithmic_gateway/config.py`; delete `_warn_deprecated` and the accepted sets
- [x] 1.2 Same in `python/rithmic_nt_connect/config.py`; share one removal message per variable
- [x] 1.3 Rewrite `tests/test_gateway_retired_knobs.py` (every set value raises on both configs, unset passes, tables agree); drop the `mux` env in `tests/test_import_isolation.py`

## 2. Retire the Python mux

- [x] 2.1 Confirm with the market-data-lake owner that `create_gateway_client` and `max_workers > 1` are unused (extends `simplify-gateway-modes` 6.4)
- [x] 2.2 Make `GatewayClient.load_time_bars_range` sequential-only and remove `max_workers`
- [x] 2.3 Delete `python/rithmic_gateway/{mux,runtime,transport_recovery}.py` and `tests/test_gateway_mux.py`, `tests/test_gateway_runtime.py`, `tests/test_transport_recovery.py`
- [x] 2.4 Drop the `python-mux` client from `tests/test_gateway_scenarios.py`; fix comments in `framed_conn.py`, `recovery_bridge.py`, `client.py`
- [x] 2.5 Test that `rithmic_gateway` imports without Nautilus and exposes no mux names

## 3. Skipped behavior fixes (code-only, one commit each)

- [ ] 3.1 Arm facade event queues on first subscribe/poll; `deliver` skips unarmed queues (`crates/rithmic-gateway/src/client/runtime.rs`); tests: unarmed channel receives nothing, subscribe then immediate event arrives
- [ ] 3.2 Compute `ts_event_ns` in the Rust event dict builder; `_convert.py` reads the field; cache `symbol_and_exchange_from_instrument_id`; extend `tests/test_exec_rules_parity.py` for `event_ts_ns`
- [ ] 3.3 `subscribe_transport_events` returns a cancel handle; Python wrapper cancels on last listener and close; update `_lib.pyi`; test the task ends and the session is freed
- [ ] 3.4 Event pump locks once per iteration and publishes outside the gate (`server/pump.rs`, `direct.rs`); reconnect/restore tests still pass

## 4. Docs and specs

- [x] 4.1 Replace the six `TBD` Purpose lines in `openspec/specs/*/spec.md`
- [x] 4.2 Update `AGENTS.md`, `docs/STATUS.md`, `docs/references/ops-runbook.md`, ADR 0001 for removed knobs and mux
- [x] 4.3 Run `python scripts/status_progress.py --check` after STATUS edits

## 5. Verify

- [ ] 5.1 `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect`
- [ ] 5.2 `uv run ruff check .`, `uv run ruff format --check .`, `uv run ty check python/rithmic_nt_connect tests`, `uv run pytest -q`
- [ ] 5.3 Live tick and reconnect verification remains open (needs a market-data account); do not claim it
