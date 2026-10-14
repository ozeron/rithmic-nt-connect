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

## 6. Dead code and duplicate tests (audit 2026-10-14)

- [x] 6.1 Delete verified-unused Python helpers: `flock.read_lock_pid`, `framing.decode_frame`, `providers.nautilus_venue`, `Inventory.adopt_positions`, `_orders.TRUSTWORTHY_*`, the `RithmicReadOnlyExecutionClient` alias, four never-produced `TransportFaultKind` members, the `RITHMIC_GATEWAY_CLIENT` ignored-selector test
- [x] 6.2 Delete verified-unused Rust items: `RithmicSession::recv_event`, `FakeSession::is_connected`, `AccountRow::into_account`, `SubKey::is_internal`, `restore_after_reconnect` / `on_subscribe` / `on_unsubscribe`, `rpc_table::by_method`, `ClientCtx.id`, the `rithmic-nt-connect` `thiserror` dependency and the `pub use rithmic_plants::*` re-export
- [x] 6.3 Delete one-off probes `crates/rithmic-nt-connect/examples/probe_daily_bars.rs` and `scripts/probe_hydrate_under_live_md.py` (AGENTS.md: no one-off probes)
- [x] 6.4 Delete duplicate tests `test_order_book_empty_levels_raises`, `test_emit_accepted_only_while_submitted` (tests/test_notification_intake.py)
- [ ] 6.5 Delete the six duplicated tests in `tests/test_data_transport_recovery.py` that `tests/test_recovery_bridge.py` already covers
- [ ] 6.6 Stale docs: rewrite README, crates/README, scripts/README, AGENTS facade line, STATUS/runbook/exec-matrix facts; delete `docs/plans`, ADR 0001, dated reference notes (in progress)
- [ ] 6.7 Decide: `request_bars_error` registry, Python `GatewayClient` order/PnL methods, `scripts/spike_bracket_order.py` + test (need owner decision)
- [ ] 6.8 Verify then delete the unused `RithmicSession` place/cancel/modify/load wrappers in `session.rs` (~170 lines; only two tests call them)

## 7. Deferred to a separate change (architecture review 2026-10-14)

Not part of this change: gateway pump drains one event per 10 ms (H1), per-connection serial dispatch and non-cancel-safe frame read (H2), session gate / `DirectSession` mutex held across network I/O (H3), direct-mode Rust trading gate (M1), error taxonomy `Rejected`/`NotSent`/`Unknown` (M2), epoch-bump vs `restore_intents` (M3). Tasks 3.1 to 3.4 stay here because they are narrow; H1 and H3 widen 3.4 and should move to the new change.
