# Session host recovery: group 4

Implementation on `ush/a2-recovery`, originally based on `feat/unify-session-host`
at `9edf95dd739a45410afdb4e31fa6de6dc76a572a`, rebased without conflicts onto
`c2e0260` after Track B2 landed (`540b76b`, `c2e0260`). This report covers 4.2, the code
portion of 4.2b, server-side 4.3, 4.4 and the requested 4.5 load comparison.
`SessionEvent` and `Session::session_events()` signatures are unchanged.
No gateway client source was edited. No live Rithmic tests were run.

## Recovery matrix

| Failure | Plants touched | Intents restored | Session events |
|---|---|---|---|
| Ticker socket/channel | `reset_ticker` only | Ticker and book keys | `Interrupted(g)` then `Resumed(g+1)` |
| History socket/channel | `reset_history` only, including history readiness probe | Live time bars | Same |
| PnL socket/channel | Existing PnL handle is ensured; PnL re-subscribed | PnL | Same |
| Order socket/channel | Disconnect and ensure order plant | Order updates and brackets | Same |
| Forced logout / host-requested login recovery / simultaneous plant failures | Full disconnect/connect | All held intents | Same |
| Account intent fails three restore passes | Escalates to full disconnect/connect | All held intents; later passes retry only failed joins | One interruption for the whole recovery, then resume on success |
| Rebuild/account restore exhausts three recovery attempts | Host stops recovery and closes session streams after their queued records | No commands replayed | `Interrupted(g)` then `RecoveryFailed(g, attempts=3)`; generation stays `g` |

A ticker reset leaves history, PnL and order plants untouched. Live bars are
history-plant intents, so ticker-only recovery does not replay them. Each
successful intent is restored once per rebuilt plant; retry passes contain only
failed intents still held by attached sessions. A last-holder detach during
restore backoff removes its joins from the retry plan. After three restore passes, failing market-data intents remain
noted and degraded. A later subscribe retries the join without adding a holder.

During recovery, LocalSession streams return empty batches or records queued
before the break. Only exhausted recovery or session closure ends the stream.
The pump finishes already-taken batches instead of aborting publishers mid-batch.
Per-session delivery barriers keep queued records ahead of lifecycle signals.
Commands already dispatched at interruption return `OutcomeUnknown`; the host
never retries them. New commands fail as not sent while the host is recovering.

## Snapshots and wire frames

A second or repeated live PnL subscribe invokes `Venue::pnl_snapshot`, wrapping
rithmic-rs `get_pnl_position_snapshot`, without issuing another PnL subscribe.
A second or repeated book subscribe invokes `Venue::refresh_book(symbol,
exchange)`. Its current implementation unsubscribes then subscribes the summary
via the existing market-data-update protocol. Refcounts do not change on a
repeat. Failed new joins roll back only the new session; a failed shared book
refresh is marked degraded so an existing holder can heal it.

Each gateway connection forwards its own LocalSession events:

- `Interrupted { generation: g }` -> `TransportInterrupted { generation: g }`,
  unsolicited event field 12 and request id 0.
- `Resumed { generation: g+1 }` -> `TransportResumed { generation: g+1 }`,
  existing event field 10 and request id 0.
- `RecoveryFailed` -> flush the already-queued interruption/replies and close
  the socket. There is no new failure wire frame.

An order notification queued before a break is written before the interruption.
Healthy connections keep the same socket and their subscription ownership.
The shared lifecycle type and wire schema were not modified.

The additional Track C finding is fixed in server dispatch: typed `Rejected`,
`NotSent`, `OutcomeUnknown` and `ReconciliationUnavailable` errors carry their
inner payloads in the wire message. Codes and legacy/untyped Display messages
are unchanged. Python surfaces this text in gateway mode; it now matches the
direct path instead of adding another `venue rejected:` prefix.

## Verification

The failure characterization and regressions have red-before-fix evidence: the updated ticker
characterization initially received Resumed instead of Interrupted; the typed
error test initially encoded `venue rejected: x` instead of `x`; a detach during
restore backoff initially replayed the removed ticker intent (2 calls instead of 1).

Added or changed host tests (`crates/rithmic-host/tests/session_host.rs`):

- `ticker_failure_restores_only_ticker_and_book_once`
- `history_failure_recreates_only_history_and_restores_bars_once`
- `pnl_failure_only_resubscribes_pnl`
- `order_failure_reseats_only_order_and_restores_orders_and_brackets_once`
- `login_failure_rebuilds_all_and_restores_each_intent_once`
- `restore_retries_only_the_failed_md_intent_and_then_degrades_it`
- `restore_does_not_retry_intents_after_the_last_holder_detaches` (ticker and PnL)
- `account_restore_failure_escalates_to_full_reconnect`
- `account_retry_does_not_repeat_successful_sibling_joins`
- `persistent_account_failure_escalates_then_gives_up_with_no_resume`
- `every_session_including_an_idle_attachment_gets_ordered_generations`
- `queued_events_survive_and_next_events_is_empty_while_recovering`
- `bounded_recovery_failure_closes_streams_after_queued_events_and_wakes_connect`
- `command_in_flight_at_break_is_unknown_and_never_retried`
- `joining_and_rejoining_live_pnl_and_book_request_snapshots_without_new_holders`
- `failed_join_snapshot_rolls_back_only_the_new_session`
- `requested_login_recovery_before_the_pump_starts_is_not_lost`
- `connect_during_restore_waits_for_the_host_without_an_extra_login`

These recovery tests use paused tokio time. Existing host refcount, rollback,
gates, drops, slow-consumer, gaps and mixed native/encoded fanout tests also pass.

Gateway coverage (`tests/ticker_failure_characterization.rs`):
`ticker_only_failure_preserves_sibling_plants_and_signals_interruption`,
`queued_order_precedes_interruption_and_every_connection_resumes`, and
`exhausted_recovery_flushes_interruption_then_closes_the_socket`.
The integrated B2 end-to-end test,
`remote_session_receives_host_interruption_resume_and_restored_ticker_over_duplex`,
uses the real `RemoteSession` and server over `tokio::io::duplex()`. It verifies
initial ticker delivery, `Interrupted(1)` then `Resumed(2)`, restored ticker
delivery on the same connection, and exactly one host reset/re-subscribe with no
client replay. No client source changes were needed for the rebase or test.
Server unit tests cover typed-error wire encode/decode and unchanged legacy
messages. Offline fake Rithmic websocket tests verify history-only logins/logouts
and the actual snapshot/refresh request templates; a typed venue-boundary test
separates ForcedLogout from ordinary socket closure.

The Rust and Python trait scanners now stop at the Session/Venue boundary;
venue-only recovery helpers are not consumer RPCs. The Python regression test is
`test_venue_recovery_helpers_are_not_consumer_rpcs`.

Required checks passed after the B2 rebase and final source changes:

```text
cargo fmt --all -- --check                                        PASS
cargo clippy --workspace --all-targets -- -D warnings              PASS
cargo test -p rithmic-host -p rithmic-plants -p rithmic-gateway
  -p rithmic-exec -p rithmic-nt-connect
  --features rithmic-gateway/testing,rithmic-host/testing          465 passed
uv sync --extra dev                                              PASS
uv run pytest -q                                                 799 passed, 99 skipped
uv run ruff check .                                              PASS
uv run ruff format --check .                                     PASS
uv run ty check python/rithmic_nt_connect python/rithmic_gateway tests  PASS
git diff --check                                                 PASS
```

The Python suite reported two existing fake-gateway thread warnings (`eof
header`). No live test was enabled. These checks are repeated after the recovery
commit for the final verification gate.

## Interleaved release comparison

Final comparison: base `c2e026026bc2fd827b5929d6a832a80fff898c24`, candidate on
`ush/a2-recovery` including the B2 integration, typed-error fix and final detach
retry fix. Both binaries were built with
`cargo build -p rithmic-loadtest --release` (the base uses `--manifest-path`
for its temporary archive).
Following the baseline report’s "Group 2 interleaved A/B" method, all four
scenarios ran sequentially for three rounds. Rounds 1 and 3 ran base then
candidate; round 2 ran candidate then base. No other tests or builds ran in this
worktree during measurement. The shared machine’s one-minute load average ranged
from 5.00 to 10.59.

All values below are microseconds; ratios are candidate/base medians.

| Scenario / metric | Base median | Candidate median | Ratio |
|---|---:|---:|---:|
| reconnect_during_flood event p99 | 246 | 221 | 0.90 |
| orders_flood_slow_rpc event p99 | 1519 | 1407 | 0.93 |
| stalled_consumer event p99 | 559 | 2073 | 3.71 |
| flood_10k_4conn_1session event p99 | 515 | 571 | 1.11 |
| Loaded order p99 | 837 | 925 | 1.11 |
| Idle order p99 | 214 | 200 | 0.93 |

Every p99 sample, in round order:

| Scenario / metric | Base | Candidate |
|---|---|---|
| reconnect_during_flood event p99 | [236, 299, 246] | [266, 221, 214] |
| orders_flood_slow_rpc event p99 | [1535, 1423, 1519] | [1407, 3371, 631] |
| stalled_consumer event p99 | [525, 559, 1024] | [8167, 2073, 476] |
| flood_10k_4conn_1session event p99 | [2281, 515, 321] | [7467, 396, 571] |
| Loaded order p99 | [987, 837, 794] | [985, 925, 485] |
| Idle order p99 | [238, 214, 186] | [169, 244, 200] |

- All 24 runs exited successfully, with zero unsignalled loss and zero reordered
  events. All non-reconnect runs delivered 100% of events. Candidate reconnect
  rounds 2 and 3 delivered 79,987/80,000 and 79,999/80,000 events per consumer;
  each consumer received one exact gap for 13 and 1 skipped events respectively.
  All other reconnect runs delivered 100%. Every reconnect consumer received
  one resume and stayed on its existing transport.
- Loaded order p99 was 985 / 925 / 485 us; maximum was 10,311 / 1,684 / 1,840 us.
  These satisfy the absolute order budgets of p99 <= 10 ms and max <= 50 ms.
  The concurrent slow read lasted 8 seconds and every 50k/s flood event arrived.
- Healthy stalled-consumer peers sustained 20,000 events/s without loss in every
  round; the stalled connection was disconnected in every base/candidate run.
  Candidate event p99 median was **3.71x base**. Candidate round 1 also exceeded
  the latency thresholds observed in the dedicated 20k/s budget scenario
  (p99 8.167 ms, p99.9 20.063 ms), although this scenario is shorter and includes
  a stalled peer. The sharp spread across rounds prevents attributing this to
  the source change or scheduling noise. Quiet-host confirmation remains needed;
  this report does not claim a general absence of tail-latency regression.
- Mixed 10k/s flood median p99 was 1.11x base, with candidate samples ranging
  from 396 to 7,467 us. It retained full delivery at 10,000 events/s.

These are the four requested default scenarios, not the full-duration latency,
sustained/burst, direct FFI, allocation or soak certification. All harness fields
(including p50/p99/p99.9/max, throughput, per-consumer gaps and RSS), run order,
load averages, source diff digest and binary hashes are preserved in
[the companion JSON](2026-10-15-session-host-recovery.json). That file also keeps
the earlier pre-B2 comparison, explicitly labelled as superseded after source
changes. Its samples were not mixed into the final medians.

Binary SHA-256:

- Base: `0a90b16e4efd850924f3d767831afa484058ffefe30b61ff7d949504cc5f9429`
- Candidate: `bdbf464ee69252766d9b9a4eebe0ef348d2a40c0d3d25407289c6d8c0dc6aa31`

Candidate source provenance: SHA-256 of `git diff c2e0260 -- crates tests` is
`653c8d0fb38d322930d2329cfcedf0374e7ba8a335cbee31cf7e9e58c31a217b`. No source changed between that build
and measurement. The base and candidate shared the dependency target cache;
a package-level release clean was needed between builds to avoid using base
host metadata for the candidate. The exact build and clean commands are in the
JSON. Temporary base source copies and copied binaries were removed after
preserving the reports; no temporary git worktree was created.

Reproduce the run matrix using independently copied release binaries:

```bash
./BASE/rithmic-load run SCENARIO --out base-round-N.json
./CANDIDATE/rithmic-load run SCENARIO --out candidate-round-N.json
```

Repeat for each of the four named scenarios in each round, reversing the two
commands in round 2. Keep the machine free of other builds/tests.

## Choices beyond the spec and deferred validation

- Recovery is bounded at three rebuild attempts, each with up to three restore
  passes. Production pauses retain the existing five-second rebuild retry and
  two-second restore backoff. Exhaustion leaves the host terminal; a caller must
  start a new host rather than revive a stopped pump with `connect`.
- Concurrent failures from distinct plant pumps widen recovery to a full login;
  no secondary failure is discarded during pump shutdown.
- Lifecycle delivery barriers wait at most two seconds for a stalled consumer,
  so it cannot prevent healthy sessions from recovering. This timeout does not
  perform venue I/O and barriers are never wire frames.
- rithmic-rs ForcedLogout becomes a typed session-wide transport failure in the
  internal stream hub. The host does not inspect error text to choose scope.
- The user-run live test deciding whether repeat book subscribe sends a snapshot
  remains deferred. The requested unsubscribe/subscribe strategy is implemented
  and verified with the fake server; live snapshot timing is not claimed here.
- Direct PyO3 switch-over belongs to later host work. RemoteSession re-join and
  event mapping are supplied by the integrated B2 commits and verified here
  through duplex. This branch adds no client source edits.
- The load comparison above uses the four requested scenarios and their existing
  durations. It does not substitute for the separate full sustained/burst,
  allocation, direct-FFI or ten-minute soak measurements.

## Implementation receipt for the orchestrator

```yaml
status: complete
plan_path: openspec/changes/unify-session-host/tasks.md
u_ids_attempted: [4.2, 4.2b-code, 4.3-server, 4.4, 4.5-requested-comparison, typed-error-parity, B2-integration]
u_ids_completed: [4.2, 4.2b-code, 4.3-server, 4.4, 4.5-requested-comparison, typed-error-parity, B2-integration]
implementation_engine_binding: null
requested_route: native Codex
actual_route: native Codex
requested_model: null
actual_model: gpt-6.1-sol
requested_effort: null
fallback_reason: null
run_id: null
source_kind: plan
source_digest: sha256:88e064e306e6a876fd9ebd0f61a5ce92eb324810708a326a9f76f2f7918c3093
plan_checkpoint: null
blockers: []
recovery_path: null
settled_decision_conflicts: []
behavior_change: true
standalone_shipping_skipped: true
```

`changed_files` are the host, plants and server sources/test files named in the
verification section, the two trait scanners, and this report with its companion
raw performance JSON. `verification_results` are the exact check results above
and interleaved measurements above.

`verification_evidence` and `unit_receipts`:

| Unit | Evidence and integration receipt |
|---|---|
| 4.2 / 4.4 | Behavior changed. Inspected existing ticker characterization, host restore/refcount/degraded/gap tests and venue forwarder tests. Added the named paused-time host tests and venue protocol tests; retained the existing suites unchanged where applicable. Characterization failed before implementation; last-holder detach regression failed before its fix. Full Rust/Python checks pass. |
| 4.2b code | Behavior changed. Inspected shared subscribe/refcount tests and venue subscribe templates. Added snapshot/join/rollback tests and fake-server assertions for the actual PnL/book protocol calls. Full checks pass. Live repeat-subscribe behavior was explicitly excluded by the user and remains deferred. |
| 4.3 server | Behavior changed. Inspected existing gateway resume, queue ordering and write-flush behavior. Added host lifecycle/failure/queued-data tests and gateway event-order/EOF tests. Updated the original ticker characterization; full checks pass. |
| Typed error parity | Behavior changed. Inspected client `map_reply`/conversion mappings and existing dispatch tests without editing the client. New wire encode/decode regression observed `venue rejected: x` before the fix and `x` after it. Committed as `3bff4bf` after the required checks. |
| B2 integration | Behavior changed by the integrated branches. Rebase onto `c2e0260` was conflict-free. Added the real RemoteSession duplex recovery test; all four recovery characterization tests and the full suite pass. |
| 4.5 requested comparison | No behavior change in measurement code. Used the existing harness and scenarios unchanged. Built both release binaries, ran three alternating rounds, retained every raw report and binary hash, and removed the temporary base copies after preserving results. |

All units ran natively in this worktree; no detached worker or external-model
route was used. The coherent recovery unit is committed after local checks;
commits are listed in the returned report. No standalone review, PR, push or CI
watch was performed. The caller owns the remaining integration/release gates.
