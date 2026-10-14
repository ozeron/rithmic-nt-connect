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

## 3. Skipped behavior fixes (moved)

Moved to `harden-gateway-runtime` (tasks 2.x to 5.x): they overlap its event-path, lock and copy work and need its load harness to prove them.

## 4. Docs and specs

- [x] 4.1 Replace the six `TBD` Purpose lines in `openspec/specs/*/spec.md`
- [x] 4.2 Update `AGENTS.md`, `docs/STATUS.md`, `docs/references/ops-runbook.md`, ADR 0001 for removed knobs and mux
- [x] 4.3 Run `python scripts/status_progress.py --check` after STATUS edits

## 5. Verify

- [x] 5.1 `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-nt-connect`
- [x] 5.2 `uv run ruff check .`, `uv run ruff format --check .`, `uv run ty check python/rithmic_nt_connect tests`, `uv run pytest -q`
- [ ] 5.3 Live tick and reconnect verification remains open (needs a market-data account); do not claim it

## 6. Dead code and duplicate tests (audit 2026-10-14)

- [x] 6.1 Delete verified-unused Python helpers: `flock.read_lock_pid`, `framing.decode_frame`, `providers.nautilus_venue`, `Inventory.adopt_positions`, `_orders.TRUSTWORTHY_*`, the `RithmicReadOnlyExecutionClient` alias, four never-produced `TransportFaultKind` members, the `RITHMIC_GATEWAY_CLIENT` ignored-selector test
- [x] 6.2 Delete verified-unused Rust items: `RithmicSession::recv_event`, `FakeSession::is_connected`, `AccountRow::into_account`, `SubKey::is_internal`, `restore_after_reconnect` / `on_subscribe` / `on_unsubscribe`, `rpc_table::by_method`, `ClientCtx.id`, the `rithmic-nt-connect` `thiserror` dependency and the `pub use rithmic_plants::*` re-export
- [x] 6.3 Delete one-off probes `crates/rithmic-nt-connect/examples/probe_daily_bars.rs` and `scripts/probe_hydrate_under_live_md.py` (AGENTS.md: no one-off probes)
- [x] 6.4 Delete duplicate tests `test_order_book_empty_levels_raises`, `test_emit_accepted_only_while_submitted` (tests/test_notification_intake.py)
- [x] 6.5 Delete the five duplicated tests in `tests/test_data_transport_recovery.py` that `tests/test_recovery_bridge.py` already covers (kept the replay wiring, exhaustion and disconnect tests)
- [x] 6.6 Stale docs: rewrite README, crates/README, scripts/README, AGENTS facade line, STATUS/runbook/exec-matrix facts; delete `docs/plans`, ADR 0001, dated reference notes
- [x] 6.7 Deleted `scripts/spike_bracket_order.py` and its test (proof recorded in STATUS). Kept `request_bars_error` and the Python `GatewayClient` order/PnL methods: the first may have out-of-repo readers, and the methods carry the gates / restore / one-shot / ticker-reset scenarios that the spec requires both clients to pass
- [x] 6.8 Deleted the 12 unused `RithmicSession` place/cancel/modify/load wrappers (-200 lines; `cancel_all_orders` is still used); the offline-testable remainder of `session_unit.rs` now runs against `DirectSession`

## 7. Deferred (architecture review 2026-10-14)

The architecture findings are planned in `harden-gateway-runtime`.
