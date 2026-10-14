## 1. Load harness and baseline (no behavior change)

- [x] 1.1 Add `crates/rithmic-loadtest` (`rithmic-load`, excluded from the wheel): start the fake gateway parent, attach the real Rust client over a unix socket, JSON report with hdrhistogram p50/p99/p999, sent/received, gaps and skipped, throughput, peak RSS
- [x] 1.2 Scenario S1 ticker flood at 1k / 10k / 50k / 100k / 200k events/s with 1, 4 and 8 clients; each event carries its creation timestamp
- [x] 1.3 Scenario S2 flood plus order place/cancel round-trip latency; S3 flood plus a slow history RPC on the same socket (fake gains a configurable history delay)
- [x] 1.4 Scenario S4 stalled consumer (one client stops reading); S5 plant reconnect during a flood; S6 10 minute soak with RSS sampling
- [x] 1.5 Allocation microbench of `publish_plant_event` with a counting global allocator (server path only), 1 and 4 subscribers
- [x] 1.6 Scenario S7: direct-mode and gateway-mode FFI throughput through the PyO3 `Session` into the dict converter (pytest `perf` marker, reuses `tests/gateway_fake.py`)
- [x] 1.7 Run everything on the unmodified code and commit `docs/perf/baseline-before.json`; record the observed pump ceiling and any unexpected finding in the design
- [ ] 1.8 Ratify or edit the budgets in `specs/gateway-throughput/spec.md` against the baseline

## 2. Event-driven delivery and gaps (review H1, L9)

- [x] 2.1 Add `Session::next_events()` over stable `StreamHub` lanes (one forwarder per plant connection) to the trait; implement for `DirectSession`, `FakeSession` and `GatewaySession`; RPC-table, PyO3 and Python parity tests updated
- [x] 2.2 Add the additive `Gap { stream, skipped }` event to `session.proto`; encode in the gateway convert layer, decode in the client convert layer, regenerate `session_pb2.py`; verify the Python lake client ignores unknown event bodies and add the test
- [x] 2.3 Replace `run_event_pump` polling with one awaiting task per stream; re-obtain receivers on epoch change before `restore_intents`; no sleep, no gate held while waiting
- [x] 2.4 Publish gaps on per-stream topics; client queues them; PyO3 raises `ChannelLaggedError`; the data client resyncs only that stream
- [x] 2.5 Parity test: the same scripted lag yields the same gap through `DirectSession` and `GatewaySession`
- [x] 2.6 Gate: S1 sustained 50k events/s and the burst case meet the budgets; idle wake-to-deliver p99 at most 2 ms

## 3. Connection lanes (review H2)

- [x] 3.1 Add a `Lane` column (command / slow) to `rpc_table.rs`; the table test fails for a row without one
- [x] 3.2 Split `handle_client` into reader, command, slow-lane (semaphore, default 4) and writer tasks; decode straight from the payload with no prefix copy; replies drain before events
- [x] 3.3 Tests: cancel behind a history load returns first; place then cancel stay ordered; partial frame during an event write does not desync
- [x] 3.4 Gate: S2 and S3 meet the order-isolation budget; S4 peers lose at most 10 percent
- [x] 3.5 Update the RPC-kinds table in `AGENTS.md` with the lane rule

## 4. Lock diet (review H3)

- [x] 4.1 `DirectSession`: clone plant handles under a short lock for subscribe, history, `ensure_*`; do I/O unlocked
- [x] 4.2 `session_ops` becomes an `RwLock` (read for place/cancel/modify, write for disconnect with the bounded wait)
- [x] 4.3 Explicit deadlines for connect and history; `list_systems` releases the GIL (`py.detach`) and has a deadline (review M8)
- [x] 4.4 Remove the gateway session gate from order one-shots and history (`dispatch/orders.rs`, `dispatch/history.rs`); keep a read-lock that reconnect can write-lock; in-flight calls fail as transport errors on replacement
- [x] 4.5 Tests: cancel during a history load, event retrieval during a retrying connect, connect deadline
- [x] 4.6 Not triggered: the harness runs against the fake plant, which has no `inner` mutex; the fix is structural (no lock across I/O) and unit-tested. Snapshot design not pursued (see design, phase 4)

## 5. Copy diet and FFI batching (review M6, M7, L4, L8; moved from retire-python-mux-and-noop-knobs 3.1 to 3.3)

- [x] 5.1 One receiver per stream; convert to `PlantEvent` once; check for a subscriber before encoding
- [x] 5.2 Fan-out frames carry a `Kind`; each client's forwarder filters to the kinds it subscribed (a mask kept in `client_insert`/`client_remove`) instead of re-keying topics; `data.py` book/bar skipping unchanged
- [x] 5.3 Client queues hold `Arc<PlantEvent>`; arm a queue on first subscribe/poll; push the clone only to armed queues and move into the last
- [x] 5.4 `poll_events(channel, max, timeout_ms)` batch API blocking on the awaitable receiver with the GIL released; migrate `data.py`, `execution.py`, `polling.py`; remove the idle sleeps
- [x] 5.5 Compute `ts_event_ns` in Rust; `_convert.py` reads the field; cache `symbol_and_exchange_from_instrument_id`
- [x] 5.6 `subscribe_transport_events` returns a cancel handle; Python wrapper cancels on last listener and close; test the task ends and the session is freed
- [x] 5.7 Gate: allocation microbench at most two allocations per event; S6 soak RSS growth at most 5 percent

## 6. Error taxonomy and direct trading gate (review M1, M2)

- [x] 6.1 Add `Error::Rejected`, `Error::NotSent`, `Error::Unknown`; mark the send boundary in `session.rs`; map at the source
- [x] 6.2 Gateway wire codes for the three classes; client and PyO3 map them; `execution.py` emits deny / `OrderRejected` / latch accordingly
- [x] 6.3 Parity test: the same scripted failures yield the same class through both modes
- [x] 6.4 `DirectSession` takes the trading and cancel-all flags and enforces them; `trading_enabled()` returns `Some(bool)` in both modes; test that a direct place with trading off is denied without the Python client

## 7. Recovery semantics (spike; after S5 passes on current code)

- [ ] 7.1 Make S5 (reconnect during flood) pass and record its current behavior (loss, duplicates, gap markers, teardown churn)
- [ ] 7.2 Decide option A (client-driven replay, remove dead restore work) or B (parent-owned restore, epoch bump as `TransportUp` plus gap); record the decision in the design
- [ ] 7.3 Implement the decision; one failing intent must not loop reconnect forever (bounded give-up with a typed state)
- [ ] 7.4 `reset_ticker` becomes a per-client resync once gap events exist (review M4); update the misleading docstring in `data.py`
- [ ] 7.5 Extend `RestorePlan` / `restore_intents` and the gate/reconnect tests if a new intent bit is introduced

## 8. CI, docs and verification

- [ ] 8.1 CI smoke subset (S1 at 10k events/s for 5 s, S2 isolation, S4) with loose floors; fails on loss without a gap
- [ ] 8.2 Commit `docs/perf/baseline-after.json`; document how to run and read the report in `docs/references/ops-runbook.md`
- [ ] 8.3 Update `docs/STATUS.md` (do not claim live verification) and run `python scripts/status_progress.py --check`
- [ ] 8.4 `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test` for all four crates plus the loadtest crate
- [ ] 8.5 `uv run ruff check .`, `uv run ruff format --check .`, `uv run ty check python/rithmic_nt_connect tests`, `uv run pytest -q`
- [ ] 8.6 Live tick delivery and reconnect smoke on a market-data account remain open (task 5.3 of `rust-unified-session`); do not claim them
