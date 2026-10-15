## Execution plan

Agents run in the shared checkout on branch `feat/unify-session-host`. Each agent edits only the files of its track, does not commit, and reports changed files plus check results. The orchestrator reviews, ticks tasks and commits per wave.

| Wave | Runs in parallel | Why this order |
|---|---|---|
| 1 | 0.1 baselines · 0.2 ticker-failure probe · 0.3 `TransportInterrupted` · 0.5 ADR 0004 draft | Each touches separate files (perf docs, gateway tests, proto plus codec, docs) |
| 2 | Group 1 alone | Renames and moves touch every crate |
| 3 | A: group 2 · B: 3.1 then 3.2–3.4 · D: Python track 6.2–6.5 prepared against D9 | B stays out of `server/mod.rs` until A's 2.5 lands, except 3.1, which lands before 2.5 |
| 4 | A: group 4 · C: scenario list and host tests · B: 6.1 | Builds on `LocalSession` and `RemoteSession` |
| 5 | A: 5.1–5.4, then C: 5.5–5.6, then D switches to `Session.fake` | Direct switch-over |
| 6 | 6.6 live e2e (user-run, needs credentials) · group 7 | Merge gate |

## 0. Prep (parallel, can start now)

- [x] 0.1 Record baselines (gateway mode done: `docs/perf/2026-10-15-pre-session-host-baseline.md`; direct mode deferred to 5.5a2 because no credential-free direct session exists yet): `RITHMIC_PERF=1` FFI throughput in direct and gateway modes, plus `rithmic-loadtest` budgets, written to `docs/perf/`.
- [x] 0.2 Characterize today's parent on a ticker-only failure with a `FakeSession` test (does it reconnect all plants?). Record the answer in `design.md` D3.
- [x] 0.3 Add the additive `TransportInterrupted = 12` event to `session.proto` and regenerate `session_pb2.py`. Test that an old Rust client and the lake `GatewayClient` ignore it.
- [x] 0.4 Decide the history policy (D8.2) and record it in `design.md`.
- [x] 0.5 Draft ADR 0004 (market-data replay moves to the session layer; a plant failure is an event, not a stream error).
- [x] 0.6 API freeze for the Python track: signatures recorded in `design.md` D9 (not in `_lib.pyi`, which must match the built extension).

## 1. Create `rithmic-host` and move the contract (no behaviour change)

- [x] 1.1 Add the `crates/rithmic-host` workspace member (deps: `rithmic-exec`, `async-trait`, `tokio`, `thiserror`; feature `testing`).
- [x] 1.2 Move the contract from `rithmic-plants` into `rithmic-host`: DTOs, `PlaceOrder` / `PlaceBracketOrder` / `ModifyOrder`, `AccountTriple`, `Error` / `Result`, `Stream`, `PlantSet`, history-slice maths. Venue parsing (`From<&RithmicResponse>`, `from_message`, `From<rithmic_rs::*Error>`) stays in `rithmic-plants` as free functions.
- [x] 1.3 Add the adapter-owned `Env` enum in `rithmic-host`, map it to `rithmic_rs::RithmicEnv` inside `rithmic-plants`, and remove the direct `rithmic-rs` dependency from `rithmic-gateway` and `rithmic-nt-connect`.
- [x] 1.4 Move the existing `Session` trait **unchanged** (`backend.rs`) to `rithmic-host` as `Session`; `rithmic-plants` and `rithmic-gateway` re-point their imports. Rename `DirectSession` → `RithmicVenue` and make `RithmicSession` private (`Plants`). The split into consumer `Session` and venue `Venue` is group 2 (2.4), so group 1 stays rename-only.
- [x] 1.5 Rename the existing websocket fake in `rithmic-plants` (`fake_venue.rs`, type `FakeVenue`, a fake Rithmic R|Protocol server) to `fake_server.rs` / `FakeRithmicServer`. It stays in `rithmic-plants`. Then move `fake.rs` (`FakeSession`) to `rithmic-host` behind `testing` as `FakeVenue`.
- [x] 1.6 Split `rpc_table.rs`. `rithmic-host` owns the rows keyed by `Session` method name (kind, gate, lane, restore) plus `tests/lanes.rs` and the restore-coverage tests. `rithmic-gateway` keeps only the `Body` variant ↔ method-name mapping (`make_body`, `lane_of(&Body)` via that mapping) and the proto-coverage test.
- [x] 1.7 Rename the old type names in `rithmic-loadtest` (`rig.rs`), scripts and comments.
- [x] 1.8 Update the dependency graph per design D1 and update `crates/README.md`. Run `cargo fmt --check`, `clippy -D warnings`, all crate tests, `pytest -q`.

## 2. Move hub logic into `SessionHost` / `LocalSession` (gateway behaviour unchanged)

- [x] 2.1 Move `FanoutHub` and per-client queues (`subscriptions.rs`) and `ParentGates` into `rithmic-host` (`fanout.rs`, `gates.rs`). Rename client ids to `SessionId`.
- [x] 2.2 Move `IntentStore`, `RestorePlan`, `MdIntent` and `ReconnectController` (`reconnect.rs`) into `intents.rs`.
- [x] 2.3 Move the event pump, reconnect loop, `restore_pass` and `restore_intents` (`server/pump.rs`) into `pump.rs`, generic over `V: Venue`, keeping `PumpTiming` as `Timing`.
- [x] 2.4 Add `SessionHost<V>` (with `HistoryPolicy` from 0.4, set by the roots) (`start`, `attach`, `request_plants`, generation) and `LocalSession<V>`. Implement the consumer `Session` trait on `LocalSession` by moving the logic of `server/dispatch/{subscriptions,orders,history,info}.rs` (gates, intent notes, rollback, `recon_lock`, `history_load_lock`, refuse-under-live-market-data, error mapping).
- [x] 2.5 Shrink the gateway server so each connection attaches one `LocalSession` and dispatch only decodes → calls → encodes. Lanes read the host's `RPC_TABLE`. Handshake, auth, idle exit and singleton are unchanged.
- [x] 2.6a Make intents a set per session (idempotent repeat subscribe), make `connect` ensure the host (idempotent), and make `disconnect_order_plant` per session (D8.5–D8.7).
- [x] 2.6 Port the host-semantics tests (refcount, rollback, gates, restore coverage, degraded intents, slow consumer, detach and drop) to `rithmic-host/tests` on `SessionHost<FakeVenue>`.
- [x] 2.7 Run the gateway suites unchanged (`client_scenarios.rs`, dispatch/gate, lanes), `tests/test_gateway_scenarios.py`, the lake client tests, and a `rithmic-loadtest` run against the pre-change budget.

## 3. Stream-generic transport; `RemoteSession`

- [x] 3.1 Make `serve` take any `AsyncRead + AsyncWrite + Unpin` stream, and move unix listen/dial to the gateway binary and the PyO3 root.
- [x] 3.2 Rename `GatewaySession` → `RemoteSession` and add `RemoteSession::over(stream)`.
- [x] 3.3 Make the shared scenario list run against `LocalSession` and against `RemoteSession` over `tokio::io::duplex()` to a server on `SessionHost<FakeVenue>`.
- [x] 3.4 Move the gateway transport tests off socket files where they don't test unix specifics.

## 4. Host-owned per-plant recovery and session events

- [x] 4.1 Characterise today's parent behaviour on a ticker-only failure (done in 0.2: all plants rebuilt; see D3).
- [x] 4.2 Implement smallest-unit recovery in the host: ticker/history recreate only that plant (carrying today's `reset_ticker` semantics), PnL re-subscribe, order-plant re-seat, login-level full reconnect. Then restore the intents those plants carry.
- [x] 4.2b Snapshot on join (D8.10), code part (the live repeat-subscribe check is split out as 4.2c):
  - add `Venue::pnl_snapshot()` (wrapping `get_pnl_position_snapshot`) and `Venue::refresh_book(key)`
  - the host calls them when a session joins or re-joins an already-live PnL or book intent
  - `FakeVenue` records the calls
- [x] 4.2c Live check (2026-10-15, NQZ6 on Rithmic Test: repeat subscribe rejected `[1029]`, unsubscribe + subscribe resends the snapshot; `refresh_book` stays unsubscribe + subscribe) in `tests/e2e` (test account, user-run): does a repeat `subscribe_order_book_summary` resend a snapshot? If yes, switch `refresh_book` from unsubscribe + subscribe to a repeat subscribe.
- [x] 4.3 Add `SessionEvent::{Interrupted, Resumed { generation }, RecoveryFailed}` delivered to every attached session. Map them to the `TransportInterrupted` (from 0.3) and `TransportResumed` wire events for remote clients. During recovery, `next_events` returns empty batches, not a stream error (D8.4).
- [x] 4.4 Update `ticker_failure_characterization.rs` to the new sequence (ticker only; PnL and order untouched; `TransportInterrupted` before `TransportResumed`), and add tests for:
  - a ticker failure leaves PnL and order plants untouched
  - each intent is re-issued once
  - degraded market-data intent and later retry
  - account intent escalation
  - the event order per session
  - a command in flight during the break → unknown outcome, not retried
- [x] 4.5 Re-run the throughput and latency budgets (`gateway-throughput` spec) and `rithmic-loadtest`.

## 5. Direct mode on the host; gates; fake session for Python

- [x] 5.1 Add `HostRegistry`. (From A2: a host whose recovery is exhausted is terminal, so the registry must replace a terminal host on the next `get_or_start` rather than return it.) In the **same PR**, delete `_SESSION_CACHE`, the Python `SessionLock` acquire and the holder count in `session.py`, so the flock is taken once (D8.3); the forwarding wrapper may remain until `collapse-session-surface`. The registry maps fingerprint → `Weak<SessionHost>`, with flock-before-login, a `lock_dir` parameter, last-detach shutdown, and non-blocking drop. The gateway binary uses it for its single host.
- [x] 5.2 `_lib.Session` holds `Arc<dyn Session>`: direct → `HostRegistry::get_or_start(..RithmicVenue..).attach()`, gateway → `RemoteSession`. `request_plants` stays a separate call.
- [x] 5.3 Add `Session.open_gates(trading, cancel_all) -> Gates` (design D4) and remove `configure_gates` and `is_direct` from the extension and `_lib.pyi`.
- [x] 5.4 (from B2) Switch the PyO3 binding to `session_events()`, so exec disarms at interruption time rather than after the restore, then remove the legacy synthetic Down/Up the Rust client still emits for `subscribe_transport`. Deliver `SessionEvent` to Python listeners in both modes on the existing dedicated delivery thread. Add `Interrupted` / `Resumed` / `RecoveryFailed` pyclasses, with `transport.py` re-exporting the old names for one release.
- [x] 5.5 Add `Session.fake(...)` (feature `testing`) attaching to `SessionHost<FakeVenue>`, with a script API for events, plant failure, request rejection and call log. Build it in the CI test wheel.
- [x] 5.5a Add a packaging test asserting that the release wheel's `_lib.Session` has no `fake` attribute.
- [x] 5.5a2 (accepted 2026-10-15: host adds ~5% end to end into Python, within the amended 10% budget; optimization deferred) Record the direct-mode baseline. Run the FFI benchmark on `Session.fake` built against the pre-switch direct path (or `git stash` the switch-over), then again after the switch, and compare the two (spec: Direct mode keeps its throughput). Use a quiet machine with no other agents running.
- [x] 5.6 Port `tests/test_session_factory.py` / `tests/test_rust_gateway_session.py` cases that need real session behaviour to `Session.fake`. Cover:
  - one login per credential
  - per-session intents
  - last detach frees the flock
  - a dropped session does not block
  - the execution plant union

## 6. `RemoteSession` re-joins intents; simplify the Python adapter

- [x] 6.1 After a socket re-attach, `RemoteSession` re-issues its recorded intents before emitting `Resumed`. Commands in flight stay unknown. Add a duplex test for the drop → re-dial → re-join flow.
- [x] 6.2 *(code-complete on `ush/d-python` @ `cf570df`; merges after 5.4)* `data.py` (move to `Session.fake` once 5.5 lands; develop against `WireSessionStub` from 0.6 until then):
  - remove the `reset_ticker` call and the `is_direct` check from `resync_ticker_session`
  - make transport recovery reconcile-only (no market-data replay)
  - remove the channel-error → `_resync_channel(reset=True)` path; a stream error now means `RecoveryFailed` or a closed session
  - a gap still replays only that consumer's intent
- [x] 6.3 `execution.py`: replace `_open_direct_gates` with one `open_gates` call and a warning when trading is not granted. Disarm on `Interrupted` and re-arm on `Resumed` in both modes.
- [x] 6.4 Remove the `recovery_bridge.py` direct-mode no-op and run the bridge on the session's events in both modes.
- [x] 6.5 Remove `reset_ticker` from `WireSession`, `WireSessionStub` and `_lib.pyi`, and update `tests/test_data_transport_recovery.py`, `tests/test_recovery_bridge.py` and `tests/test_wire_session_parity.py`.
- [ ] 6.6 Run the full suite. Run `RITHMIC_TEST_DOTENV=.env uv run pytest tests/e2e -v` on the test account (MotiveWave closed) in **both** modes, including a direct-mode plant-reconnect case. Re-run the 0.1 baselines and compare.
  - **2026-10-15 live run on Rithmic Test** (overrides `RITHMIC_HISTORY_READY_PROBE=0`, `RITHMIC_E2E_CONTRACT=NQZ6`):
    - **direct:** 10 passed, then 3 more after the gate fix; `test_direct_ticker_failure_restores_intent` passed (Interrupted → Resumed, ticks resumed)
    - **gateway:** 7 passed, including the RMS round trip through both the Python and Rust clients
    - **both modes:** 17 errors + 2 failures, all from the Test plant's front-month reply lacking `trading_symbol` (also on `main`); the data-client tests that need a `FuturesContract` cannot run on this account
  - Still open: re-run after track I (hop optimization) lands; the data-client suite needs an account with reference data, or a contract-based instrument fixture.

## 7. Docs and follow-ups

- [x] 7.1 Write ADR 0004 "Market-data replay lives in the session layer". It supersedes ADR 0002's market-data replay clause and keeps its re-arm and capability policy clause.
- [x] 7.2 Update AGENTS.md:
  - "Direct ↔ gateway plant-surface parity" becomes "one host, two attachments"
  - the RPC checklist uses the new names
  - gateway RPC kinds point at `rithmic-host`
  - recovery: the host restores, consumers only reconcile
- [x] 7.3 Update `crates/README.md`: the crate table, the dependency graph and an old → new name table.
- [ ] 7.4 Update `docs/STATUS.md`: the direct-mode reconnect recovery mark and the gateway notes. Then run `python scripts/status_progress.py` and `--check`.
- [x] 7.5 Rebase `collapse-session-surface` onto this change: group 1 becomes "delete Python wrappers" (registry, holders and listeners now come from the host), and remove its `reset_ticker` and `configure_gates` mentions.
- [ ] 7.6 Run the full verify gate from AGENTS.md, including `qlty check --fail-level=medium`.
