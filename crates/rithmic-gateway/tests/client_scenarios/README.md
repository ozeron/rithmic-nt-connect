# Shared attachment scenarios

`../client_scenarios.rs` declares one `scenarios!` list. Every entry runs through
`Arc<dyn Session>` twice, with the same body and assertions:

- `local`: `SessionHost<FakeVenue>::attach()`.
- `remote`: the real gateway server over `tokio::io::duplex()`, then
  `RemoteSession::over(...)`, on a host whose venue is the scripted `FakeVenue`.

Both roots use the same gates, queue capacity, timing and
`HistoryPolicy::RefuseUnderLiveMd`. This is an explicit root setting; these tests
do not compare the intentionally different production-root history defaults.
`GatewayState` at base `9edf95d` accepts only `SessionHost<dyn Venue>`, so the
remote root uses its existing type erasure. It cannot take a typed
`SessionHost<FakeVenue>` without changing production server signatures. The
venue behind that erased host is still exactly the fake exposed by the harness.
No production source or test-only production helper was changed for this suite.

## Harness API

`Harness::start(Opts)` constructs a fixture; `attach()` returns a new independent
`Arc<dyn Session>` attached to that fixture's host; `venue()` exposes that host's
`FakeVenue`. The thin helpers `command`, `calls`, `count`, `clear_calls`,
`wait_for_call` and `push_last_trades` support the original scripts/recorder.
Remote attachments use separate duplex connections. The separate remote-only
facade cases exercise multiple facades sharing one socket.

Recovery extension for tracks A2/B2: subscribe to `session.session_events()`
before calling `harness.venue().fail_stream(Stream::Ticker, "cause")` (or
`drop_plants()`). The harness imposes no recovery policy or down/up expectations.
The seven old recovery characterizations live in `legacy_recovery.rs`; their
scenario bodies were retained, and no recovery scenarios were added.

## Shared scenario list and observed results

All entries below pass locally. Remotely, all pass except
`all_command_outcomes_preserve_the_error_payload`, which fails intentionally
strict assertions against the source payload. There are no ignored tests.

- `one_session_stopping_keeps_the_others_order_stream`
- `a_session_that_disconnects_releases_only_what_it_held`
- `a_dropped_session_releases_only_what_it_held`
- `two_attachments_share_one_venue_subscription_by_refcount`
- `last_attachment_releases_its_intents_and_the_host_accepts_a_new_one`
- `cancel_all_has_its_own_gate_independent_of_trading`
- `history_is_refused_under_a_live_ticker_intent`
- `bracket_updates_reach_only_connections_that_subscribed_to_them`
- `concurrent_polls_and_rpcs_make_progress_without_deadlock`
- `a_plant_lag_reaches_the_client_in_order_as_channel_lagged`
- `the_next_events_batch_carries_the_gap_in_order`
- `an_order_stream_gap_reaches_order_subscribers`
- `order_peers_are_told_when_the_order_plant_is_reseated`
- `ticker_book_and_time_bar_intents_are_idempotent_and_refcounted`
- `all_event_fields_survive_both_attachments`
- `pnl_intents_are_shared_and_disconnect_is_per_session`
- `reads_return_the_same_records_and_complete_empty`
- `all_commands_reach_the_venue_once`
- `all_command_outcomes_preserve_the_error_payload`
- `all_gated_rpcs_are_denied_before_the_venue`
- `subscription_failure_rolls_back_and_can_be_retried`
- `a_slow_consumer_gets_a_gap_without_costing_its_peer_events`

History, fill/order-history and RMS calls currently return empty vectors in
`FakeVenue`. These scenarios verify completion and exact request recordings;
nonempty front-month/reference records and every subscribed event DTO are
compared field for field. They do not claim nonempty history replay coverage.

The two per-mode slow-consumer tests assert the same contract: typed gaps stay
in source order, delivered records match the source at exactly the index after
each gap, delivered + sum(skipped) equals sent, and skipped is positive. A final
drain rejects duplicate/trailing records, and the fast peer receives every event.
Under the session-backend requirement "Identical event records from both modes",
gap counts depend on queue depth; equal skip counts across modes are not required.

## Behavioral differences exposed

1. All seven remote commands preserve `Rejected`, `NotSent` and `OutcomeUnknown`
   variants but prepend their display text to the string payload. For example,
   local `Rejected("price refused")` becomes remote
   `Rejected("venue rejected: price refused")`; similarly `"no route"` becomes
   `"not sent: no route"`, and `"write failed"` becomes
   `"outcome unknown: write failed"`. The server dispatch serializes
   `Error::to_string()` and the client wraps that string in the typed variant
   again. All 21 combinations are checked with unchanged strict expectations;
   each command reaches the venue exactly once, including unknown outcomes.
Track A2 fixes the server to send the inner error payload. These strict
assertions stay unchanged and should turn green when A2 is merged.

Slow consumers can retain different subsets because their queue depths differ.
Remote has a 16,384-record per-stream facade queue; Local initially backs up
behind the host's 65,536-record per-session outbound queue and broadcast ring.
This is an allowed queue-depth difference, not a production bug. Both modes
must preserve the exact typed gaps each stream produces, source order without
duplicates, and conservation. No ignored tests or mode-specific expected counts
are used.

## Remote-only coverage retained

- `a_client_that_never_reads_is_dropped_without_hurting_its_peer`
- `a_fingerprint_for_a_different_credential_set_is_refused_quickly`
- `a_panicking_handler_answers_unknown_and_leaks_nothing`
- `one_facades_unsubscribe_does_not_end_another_facades_stream`
- `one_exec_facade_stopping_keeps_the_others_order_stream`
- `a_facade_that_disconnects_releases_only_what_it_held`
- `a_facade_dropped_without_disconnect_still_releases_what_only_it_held`

Other unchanged remote-only suites cover socket lanes (`../lanes.rs`), attachment
authentication/handshake (`../client_attach.rs`, `../stream_transport.rs`),
framing, singleton/idle-exit, and outcomes caused by socket loss or timeout
(`../client_outcomes.rs`). Local attachments have no corresponding socket.

## Offline checks

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p rithmic-host -p rithmic-plants -p rithmic-gateway -p rithmic-exec -p rithmic-nt-connect --features rithmic-gateway/testing,rithmic-host/testing --no-fail-fast
```

The test command is expected to fail only on the strict error-payload scenario
until the A2 server fix lands. The OpenSpec task checkbox stays unchanged.
