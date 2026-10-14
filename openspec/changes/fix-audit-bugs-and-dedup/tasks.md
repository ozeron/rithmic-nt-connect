## 1. Test seams (no behavior change)

- [x] 1.1 Add the fake Rithmic WebSocket server (`fake_venue.rs`, feature `testing`): ws listener on 127.0.0.1, prost framing and template routing, login/logout/heartbeat/system info, request log; a test proves `DirectSession` connects, logs in and disconnects against it
- [x] 1.2 Fake venue templates and scripting for every plant call `session.rs` makes (ticker/book/front month/reference data, history replays, order place/cancel/modify/brackets/show orders/history/fill history/RMS, PnL, account list); per-template replies, rejects, delays, drops and pushed notifications
- [x] 1.3 Bring `FakeSession` to parity: trading gate (`require_trading`), disconnect/`begin_order_op` gate, `enter_plant` on cancel and modify
- [x] 1.4 Add `crates/rithmic-plants/tests/session_contract.rs`: one generic scenario suite run against `FakeSession` and `DirectSession` over the fake venue (trading deny, `NotSent` vs `OutcomeUnknown`, order-plant attach, partial-connect teardown)
- [x] 1.5 Gateway client time: switch `client/{mod,runtime,attach}.rs` to `tokio::time::Instant`; pump backoffs into `GatewayParams`; extend `tests/common` with event-driven waits for new tests

## 2. Plants and exec fixes (each test-first)

- [x] 2.1 Cache the order handle and resolved account outside `inner`; `reset_ticker` does I/O unlocked; test: cancel completes during a slow ticker reset; `resolved_account` doesn't wait on a reconnect
- [x] 2.2 `subscribe_order_updates` rollback uses `opened_here` (a plant opened by the failing call has no published handle, so no order op can be in flight on it); tests: a failed re-subscribe on a live plant keeps it up, a failed first subscribe closes the plant it opened
- [x] 2.3 `collect_history_rows` fails on any error frame; fill history becomes `ReconciliationUnavailable`; tick/bar partials go through `is_transient`; tests for each caller
- [x] 2.4 Dedup only between adjacent slices at the shared boundary second/day; tests: identical in-slice prints kept, boundary repeat removed, daily boundary
- [x] 2.5 `StreamHub::take`: drain before `Down`; re-drain after `fail`/`closed` and keep the failure pending; tests for both races
- [x] 2.6 Serialise `load_orders` / `load_order_history` with an `order_drain` mutex; concurrent-load test over the fake venue
- [x] 2.7 Forwarder `catch_unwind` marks the lane failed; test with a panicking conversion
- [x] 2.8 Exchange-time `ts_event` for LastTrade/BBO; move `ts_ns` into `rithmic-exec::ident` and use it from `dto.rs`; tests with and without source fields
- [x] 2.9 `DisconnectGate` as a counter; `request_plants` probes history when it adds it; explicit `disconnect_order_plant` keeps order commands `NotSent` until ensure/subscribe; tests
- [x] 2.10 Golden tests for `From<&RithmicResponse> for PlantEvent` built from real-shaped messages (LastTrade, BBO, OrderBook, both order notifications, PnL, time bar)

## 3. Typed bracket updates (parity checklist)

- [x] 3.1 `PlantEvent::BracketUpdate(BracketUpdateDto)` decoded in `dto.rs`, emitted by `FakeSession`
- [x] 3.2 Additive `session.proto` event body, server encode, client decode, regenerate `session_pb2.py`
- [x] 3.3 PyO3 dict conversion, `_lib.pyi`, Python data/exec dispatch logs it; same-scenario test through direct and gateway (`client_scenarios.rs`, `test_gateway_scenarios.py`)

## 4. Gateway fixes (each test-first)

- [x] 4.1 History holds `md_history_gate` only for the live-intent check; add `history_load_lock`; test: subscribe and place on one socket complete during a slow fake history load; refusal test unchanged
- [x] 4.2 Forwarder overflow also sends `Gap(History)` when the client's kinds include bars; test
- [x] 4.3 Per-facade intent refcount in `client/runtime.rs` (ticker, book, time bars, PnL, order, brackets); facade disconnect/drop releases its holdings; `account_id` in the runtime key; tests for shared symbol, exec stop, different accounts
- [x] 4.4 `idle_exit::wait_until_should_exit` enables `Notified` before reading state; paused-time race test
- [x] 4.5 Decode failure recovers `request_id` and replies `bad_request`; client maps it to `NotSent`; test with a corrupt frame
- [x] 4.6 Per-request `catch_unwind` on the command lane with an `internal` reply; teardown always runs; test with a panicking fake call
- [x] 4.7 `plant_err_frame` defaults one-shot commands to `venue_unknown`; table-driven test over every `Error` variant
- [x] 4.8 Remove the extra client-side transport retries in `load_bar_slice`; test counts the attempts
- [x] 4.9 Move `GetReferenceData` / `GetFrontMonth` / `ResolvedAccount` to the slow lane; slow-lane tasks finish detached on disconnect; `tests/lanes.rs` updated
- [x] 4.10 Order-plant re-seat sends `Gap(Order)` to order peers; a deliberate re-seat does not trigger a full rebuild in `pump.rs`; test
- [x] 4.11 Pre-handshake read timeout and small max frame; test with a silent and an oversized client
- [x] 4.12 Singleton lock file opened with `truncate(false)`; losing-contender test
- [x] 4.13 PyO3 transport callbacks delivered from a dedicated std thread; Python test where a listener calls a session method synchronously

## 5. Python fixes (each test-first)

- [x] 5.1 Cache `resolved_account` via `asyncio.to_thread` in `_connect` / `_rearm_after_reconnect`; `_account_raw` reads the cache; `_seed_account_if_needed` checks `_account_seeded` first; test with a blocking stub
- [x] 5.2 Gate position reports on live PnL, filter by `_position_seen_ns`, skip unreadable roots; skip flat augmentation when not live; tests
- [x] 5.3 Rust closed-set `update_type`; Python snapshot vs incremental conversion with `F_LAST` rules; venue-shaped fixtures for every type; e2e test records the live `update_type` histogram
- [x] 5.4 `_route` inside `try` in `_request_trade_ticks` / `_request_bars`; bad rows skipped and counted; tests
- [x] 5.5 Brackets: strict recognition, events for all legs, deny the list on any missing leg; tests
- [x] 5.6 Clock-free fill dedup key and trade id in live intake; live-then-recon dedup test
- [x] 5.7 Clear `_pnl_snapshot_observed` in `_recover_order_gap` and `_resync_pnl_subscription`; test
- [x] 5.8 Bad order notification is logged and triggers a re-drain; the poll task survives; test
- [x] 5.9 `_connect` failure cancels its poll tasks and detaches recovery; test
- [x] 5.10 Bar unsubscribe compares the period; test
- [x] 5.11 `_restore_plants_after_transport_up` restores PnL with trading off; test
- [x] 5.12 Recovery bridge: `_on_down` / `_on_failed` work hops onto the loop, latch stays immediate; test
- [x] 5.13 Pure-Python `GatewayClient`: buffered partial reads, bounded pending replies, not-sent vs unknown classification; tests
- [x] 5.14 `bars_timestamp_on_close` config (default `True`), convention logged at start (the client cannot read the engine config), `closed_bars` / `BarSequenceGuard` adapted, comment fixed; `test_bar_seam_parity.py` and `test_bar_hygiene.py` parametrised; ADR `docs/adr/0003-bar-timestamp-convention.md`

## 6. Duplication

- [ ] 6.1 `mirror_conv!` macro generates the 14 DTO↔proto mappings both ways; `python/mod.rs` order notification reuses the field list; `client_convert.rs` round-trip test over every pair
- [ ] 6.2 `one_shot` helper for the gated command handlers and the three gated loads in `dispatch/orders.rs`
- [x] 6.3 Lane derived from the RPC table rows; delete the hand lists in `dispatch/mod.rs`
- [ ] 6.4 One `leave_order_plant` / `leave_pnl_plant` used by teardown and by explicit disconnect (rollback flag)
- [ ] 6.5 `reset_ticker_plant` reuses the market-data restore pass and clears degraded flags
- [ ] 6.6 `reconnect.rs`: generic `bump<K>` / `drop_count`; trim the pass-through methods
- [ ] 6.7 `call_expect!` macro in `client/mod.rs`
- [ ] 6.8 `session.rs`: generic `connect_plant`; `ensure_pnl_plant` uses `connect_pnl`; shared `parse_order_terms`; one `adjust_bracket_on`; simplified error helpers; misplaced doc comments removed
- [ ] 6.9 `dto.rs`: one order-notification builder for both arms; `TimeBarProbeRow` built on `HistoryBarDto` parsing
- [ ] 6.10 `history.rs` uses `rithmic_exec::product_root` for the listed-contract check; test for `ESZ26` and lowercase input
- [ ] 6.11 `execution.py`: `_deny` / `_deny_all` with one ordered gate list; `_make_poll_task`; modify/cancel rejects use the resolved venue id
- [ ] 6.12 Move `notification_action`, `sentinel_none`, `enum_int` into `rithmic-exec` with Rust tests and PyO3 bindings; Python originals kept as oracles in `test_exec_rules_parity.py`
- [ ] 6.13 RMS types defined once in `rithmic_gateway.types` (fields reconciled); `wire_types.py` re-exports
- [ ] 6.14 `history_window.py` parity test against the Rust slice functions (exposed via `_lib` if missing)
- [ ] 6.15 `_FlockedDirectSession`: explicit methods only where they add locking or refcount, forward the rest, `TYPE_CHECKING` protocol base; `test_wire_session_parity.py` and ty still pass
- [ ] 6.16 Fix the stale comment at `session.py:167-173`

## 7. Distribution

- [ ] 7.1 `python/pyproject.toml` becomes a maturin `bindings = "bin"` package (crate `rithmic-gateway`, `python-source` for `rithmic_gateway`, deps `protobuf` only); local build test
- [ ] 7.2 `binary.py` lookup: `RITHMIC_GATEWAY_BIN` → wheel scripts dir (`sysconfig`) → `PATH`; cargo-target scan removed; error names the wheel; tests updated
- [ ] 7.3 `rithmic-nt-connect` depends on `rithmic-gateway`; retire `wheel-data` binary bundling and `scripts/build_wheel.sh`; update `tests/test_packaging.py`
- [ ] 7.4 `.github/workflows/wheels.yml`: maturin-action matrix (Linux x86_64/aarch64 manylinux, macOS arm64), protoc, dev versioning, no-Rust install smoke, provenance attestation, rolling `main` prerelease plus tag releases; actions pinned by SHA
- [ ] 7.5 `scripts/publish_index.py` plus a Pages job: PEP 503 index with sha256 fragments, keeps the last 30 dev builds
- [ ] 7.6 README install section (index config, `uv add`, source fallback); `gateway-remote.md` and `ops-runbook.md` start the parent with `rithmic-gateway`

## 8. Docs and verification

- [ ] 8.1 STATUS: book update types, bar timestamp default, history lock wording, distribution; regenerate with `scripts/status_progress.py` and `--check`
- [ ] 8.2 ops-runbook lines 151-162: the lock is no longer held during the load; refusal unchanged
- [ ] 8.3 AGENTS.md conventions checklist reviewed for the data/exec changes; note any row that can't be marked
- [ ] 8.4 Full verify set: fmt, clippy `-D warnings`, cargo tests, ruff, ty, pytest, qlty, status check
- [ ] 8.5 Live on the Rithmic test account (MotiveWave closed, no live place): `tests/e2e`, `smoke_gateway_shared_ticks.py`, `verify_order_dry_run.py`, `rithmic-load` S3 place latency

## 9. market-data-lake (separate repo and PR)

- [ ] 9.1 Branch in `/private/mnt/DATA/code/market-data-lake`; `market-data-lake-tools/pyproject.toml` uses the `rithmic` index source; `uv lock`
- [ ] 9.2 `rithmic_client.py`: drop `auto_spawn` / `gateway_bin` / `spawn_environ`; dial an explicitly started parent; small launcher around the wheel's `rithmic-gateway` for local runs; password only in the parent's environment
- [ ] 9.3 Update the error text, the `worker.py` docstring and `AGENTS.md`; run `tests/test_rithmic_futures.py` and one ingest against a test-account parent; open the PR after the first index publish
