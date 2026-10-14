## Context

The architecture review (2026-10-14) verified in code:

- `server/pump.rs:95-112`: one `poll(Duration::ZERO)` per stream per loop under the session gate, then an unconditional 10 ms sleep. Streams are polled serially in fixed order, so a ticker flood also delays order events. Overflow (`ChannelLagged`) is logged and discarded.
- `server/mod.rs:637-671`: `dispatch(...).await` runs inside the connection's `select!` arm. The frame-read future is rebuilt every iteration (`read_exact` is not cancel-safe) and every request frame is copied once to re-add its length prefix before decoding.
- `dispatch/orders.rs`, `dispatch/history.rs`: the global session gate is held across place/cancel/modify and history round trips. `DirectSession.inner` is one mutex held across subscribe, history, `ensure_*` and `connect` (which retries forever in rithmic-rs 3.x), and every `poll_*` takes the same mutex.
- `client/runtime.rs:463`: every inbound event is cloned into every facade queue, including facades that never poll that channel.
- Direct mode: Python pollers do one `to_thread(poll)` per event and a fixed idle sleep, with single-event polls through the same mutex.

Facts about the dependency that shape the design (rithmic-rs 3.2.0): each plant handle exposes an awaitable `subscription_receiver.recv()`; a handle can be cloned and gets its own receiver; the broadcast capacity is configurable (`subscription_capacity`, default 10,000); tokio's broadcast clones each message per receiver, so the number of receivers per stream matters.

There is no benchmark of any kind in the repository today.

## Goals / Non-Goals

**Goals:**
- A reproducible synthetic load harness and a recorded before/after baseline.
- Event-driven delivery with no fixed-interval polling; per-stream isolation.
- Explicit gaps instead of silent loss, in both modes.
- No lock held across venue I/O; one receiver per stream; no per-consumer deep copy.
- Error classes assigned at the source; trading gate enforced in Rust.

**Non-Goals:**
- No change to the Rithmic wire or to rithmic-rs (cloning per receiver is a known upstream cost; we minimize receivers instead).
- No remote transport (still unix only).
- No claim about real market rates until captured on a market-data account; the budgets are for the software with a fake plant.
- Not changing the history-under-live-market-data refusal (M5) beyond keeping its lock short; policy stays as specified.

## Decisions

**1. Measure first, in a separate crate.** Add `crates/rithmic-loadtest` (binary `rithmic-load`, not shipped in the wheel). It starts the existing fake gateway parent, attaches the real Rust client over a real unix socket, drives scenarios, and writes a JSON report (hdrhistogram p50/p99/p999, events sent/received, gaps and skipped, throughput, peak RSS). Scenarios: S1 ticker flood at 1k/10k/50k/100k/200k events/s with 1/4/8 clients; S2 flood plus order place/cancel latency; S3 flood plus a slow history RPC; S4 stalled consumer; S5 reconnect during flood; S6 10 minute soak; S7 direct-mode FFI throughput through the PyO3 `Session` (a pytest-marked script reusing `tests/gateway_fake.py`). Allocations per event are measured by a separate microbench of `publish_plant_event` with a counting global allocator, so the count is server-path only and not polluted by the client. The baseline is committed before any behavior change; it should show the ~100 events/s ceiling, which also validates the review.

**2. Event-driven pump with owned receivers.** Add to `Session`: `event_streams()` returning owned receivers (ticker, history, PnL, order) plus a stream epoch, built from cloned handles under a short lock. Each receiver yields `Event(PlantEvent)`, `Gap { stream, skipped }` (from `Lagged`) or `Closed`. The parent runs one task per stream awaiting `recv()`, so a ticker flood cannot delay the order stream, there is no sleep, and no lock is held while waiting. On epoch change (reconnect) tasks re-obtain receivers before intents are restored. Alternative: keep polling but drain-until-empty and lower the sleep; rejected because it still wakes at a fixed cadence, still holds the gate, and cannot meet the idle-latency budget.

**3. Gap is an event, not a log line.** `session.proto` gains an additive `Gap { stream, skipped }` event body. The parent publishes it on per-stream topics that every client with interest in that stream joins. The Rust client queues it on the channel queue; PyO3 raises the existing typed `ChannelLaggedError` (direct mode already does) so the Python resync path is the same in both modes. The Python lake client ignores unknown event bodies (a task verifies and adds a test); this is additive and does not break it. Parity: one test asserts the same scripted lag yields the same gap through `DirectSession` and `GatewaySession`.

**4. Connection lanes.** Per connection: a reader task owning the read half and decoding straight from the received payload (no prefix copy, cancel-safe because nothing selects over it); a command task that processes state-changing and subscription RPCs in arrival order; a slow lane for history loads spawned under a per-connection semaphore (default 4); a writer task that owns the write half and drains replies before events (biased). `rpc_table.rs` gains a `Lane` column and its table test fails for a row without a lane, so a new RPC must declare command or slow. `AGENTS.md`'s RPC-kinds table gains the lane rule.

**5. Lock diet.** (a) Generalize the existing `order_handle()` pattern: subscribe, history, `ensure_*` clone a plant handle under a short lock and do I/O unlocked. (b) `session_ops` becomes an `RwLock`: place/cancel/modify take read (they run concurrently), disconnect takes write with the existing bounded wait. (c) Connect and history get explicit deadlines instead of rithmic-rs's infinite `Retry`. (d) The gateway `SessionHandle` gate is removed from order one-shots and history; it remains only as a read-lock that reconnect can write-lock, and calls in flight when plants are replaced fail as transport errors (unknown for commands). (e) If the harness still shows contention on `inner`, move plant state behind an atomically swapped `Arc` snapshot (separate step, measured, not assumed).

**6. Copy diet.**
- One receiver per stream, because tokio broadcast deep-clones the protobuf response per receiver.
- Convert `RithmicResponse` to `PlantEvent` once; encode once and share `Bytes`; encode only when the topic has a subscriber (today encode happens before the subscriber lookup).
- Hub topics become `(symbol, exchange, kind)` with `kind` in ticker/book/bars, so a client subscribed only to the ticker receives no book frames.
- Client side: `Arc<PlantEvent>` per decoded event; push the clone only to armed queues and move into the last one. PyO3 converts to a dict from the shared reference, once per poll.
- FFI: `poll_events(channel, max, timeout_ms)` returns a batch and blocks inside Rust on the awaitable receiver (GIL released) so Python does one thread hop per batch and no idle `sleep`. `ts_event_ns` is computed once in Rust and read as a field.
- Transport callback: `subscribe_transport_events` returns a cancel handle; the Python wrapper cancels it on the last listener and on close, which also breaks the keep-alive cycle.

**7. Error taxonomy at the source.** `rithmic_plants::Error` gains `Rejected`, `NotSent`, `Unknown`. `session.rs` marks the send boundary: failures before the frame is written are `NotSent`; a synchronous venue answer with a non-zero code is `Rejected`; transport, timeout or channel errors after write are `Unknown`. The gateway maps them to distinct wire codes; `execution.py` maps them to deny / `OrderRejected` / latch accordingly. Parity test: the same scripted failures through both modes yield the same class.

**8. Trading gate in `DirectSession`.** Construct it with the trading and cancel-all flags; the trait's `trading_enabled()` returns `Some(bool)` in both modes (today direct returns `None`, which Python reads as allowed). The Python client keeps its check as defense in depth until a later cleanup.

**9. Recovery semantics are a spike, after the reconnect scenario exists.** Today, after a successful restore the parent bumps `transport_epoch`; every client handler exits and `teardown` forgets the intents and leaves the venue, so recovery is effectively client-driven and `restore_intents` restores state that is then discarded. A permanently failing intent also loops reconnect forever. `reset_ticker` resets the shared plant for all peers. Decide, with scenario S5 as the oracle, between (A) keeping client-driven replay and removing the dead restore work, or (B) keeping parent-owned restore and making the epoch bump a `TransportUp` plus gap rather than a disconnect. Do not change this before S5 passes on the current code. `reset_ticker` should become a per-client resync once gap events exist.

**10. Budgets are initial and ratified from the baseline.** The numbers in `gateway-throughput` (50k events/s sustained, 200k burst, p99 5 ms at 20k/s, order p99 at most 2x idle and 50 ms, at most two allocations per event, RSS growth at most 5 percent) are set from engineering judgment with headroom over expected CME NQ/ES rates; they are validated against the real baseline in task 1 and edited in the spec before archive if the harness shows they are mis-sized. Real market peak rates still need a capture from a market-data account.

## Baseline findings (unmodified code, `docs/perf/baseline-before.json`)

Measured with `rithmic-load suite` on a 10-core macOS machine, all parties in one process.

- **H1 reproduced.** A 1,000 events/s feed is delivered at about 138 events/s (13.8 percent after the drain window); 10k, 50k and 200k events/s all deliver 137 to 190 events/s. The pump ceiling is the whole story at every rate. Latency grows without bound (p99 7 s at 1k/s, 54 s in the 45 s soak). The fake plant's queue is unbounded, so RSS climbs to about 390 MB; the real rithmic-rs buffer would instead lag and, today, drop silently.
- **Idle wake-to-deliver:** p50 5.2 ms, p99 11.6 ms (the 10 ms poll interval), over the 2 ms budget.
- **H2 reproduced two ways.** On one raw connection a place sent behind a 3 s read RPC is answered after 2.9 s and the replies arrive in order slow, place. Through the library with open-loop order flow, order p99 is 7.6 s under an 8 s slow RPC versus 0.20 ms idle. (A sequential order loop hides this behind one stuck call; the harness is open-loop for that reason.)
- **Stalled consumer:** the parent drops a connection that stops reading; peers' delivery is bounded by the same pump ceiling, so isolation cannot be judged until H1 is fixed.
- **Allocations on the publish path:** 4 per event, 107 bytes. The spec budget of 2 is a target for the copy-diet phase.
- **Per-client fan-out buffer is 1,024 events** (`DEFAULT_QUEUE_CAP`), and an overflow disconnects the client. At 50k events/s that is a 20 ms stall. The baseline cannot reach it (the pump is the bottleneck), so event-driven delivery must size this deliberately.
- **Python boundary (S7):** about 80 events/s through `Session.poll_event` in gateway mode, the same ceiling seen from Python.

## Phase 2 results and decisions taken during implementation

Measured with the same harness (`docs/perf/after-phase2.json`). The numbers that matter, before to after:

| Scenario | Before | After |
|---|---|---|
| Idle wake-to-deliver p99 | 11.6 ms | 0.3 ms |
| 1k events/s, 1 connection | 13.8 percent delivered | 100 percent, p99 0.1 ms |
| 10k events/s, 4 connections + 1 session | 1.4 percent, p99 7.9 s | 100 percent, p99 0.4 ms |
| 50k events/s sustained, 4 connections | 0.4 percent | 100 percent, p99 1.5 ms |
| 200k events/s burst, 4 connections | 0.7 percent | 100 percent |
| 20k events/s latency, connection + session | p99 17.7 s | p99 0.5 ms |
| Stalled consumer | peers 0.9 percent | peers 100 percent, staller cut |
| Python `poll_event` (S7) | 80 events/s | about 380,000 events/s (blocking), 25,000 to 30,000 (one thread hop per event) |

Decisions that differ from or extend the plan above:

- **Stable lanes, not refreshed receivers.** The plan sketched `Session::event_streams()` returning owned receivers that consumers re-obtain on reconnect. Implementation uses a `StreamHub` of four bounded lanes that outlive plant connections, fed by one forwarder task per plant connection (one `resubscribe()` receiver per stream, so tokio's per-receiver clone happens once). Consumers call `Session::next_events(stream, max, timeout)` (batch, waits event-driven, no lock) or the compatibility `poll_*`. No refresh logic and no epoch plumbing for consumers.
- **Gap as an event.** `PlantEvent::Gap(GapDto)` rides the existing hub, client queues and polls. A gap on the ticker or history stream is published to every symbol topic; pnl and order gaps use their sentinel topics.
- **Fan-out overflow is a gap, not a disconnect.** The old behavior (disconnect at 1,024 events) lost the whole burst. Topic rings and client queues are now 65,536 frames; a receiver that still falls behind gets an exact gap and keeps its connection. A client whose socket stops draining is cut by the writer's 5 second timeout.
- **Stale items are counted.** Events still queued when a plant detaches are discarded by the generation bump; they are reported as one exact gap on the next read instead of vanishing. A small carry buffer keeps a batch within `max` without dropping the gap's successor.
- **Client queue cap (4,096) now binds.** With delivery no longer capped by the pump, a one-event-at-a-time Python poller overruns the library client's 4,096-event queue on a large burst (reported as `ChannelLaggedError`, never silent: 20,000 sent = 4,096 received + 15,904 reported skipped). The copy-diet phase should size this deliberately and move the adapter to batch polling.
- **Not yet fixed (their phases):** order p99 under a slow RPC is still about 7.5 s and the shared-socket consumer is still dropped during it (the connection handler stays in dispatch and cannot drain events; Phase 3). Publish-path allocations are still 4 per event (Phase 5).

## Phase 3 results

`docs/perf/after-phase3.json`. Reader, command, slow and writer tasks per connection; a `Lane` column in the RPC table with `lane_of` and a test that the two agree; replies drain ahead of events; frames decode straight from the payload.

| Scenario | Before | After |
|---|---|---|
| Place behind a 3 s read RPC, one raw connection | answered after 2,903 ms, replies in order slow, place | answered in 3.8 ms, replies in order place, slow |
| Order p99 under 50k events/s flood plus an 8 s slow RPC (open-loop orders) | 7,577 ms (max 7,983 ms); the shared socket's consumer was dropped, 0.3 percent of events delivered | 0.49 ms (max 0.60 ms); 100 percent delivered |

Notes from implementation: the writer must end when the reply channel closes and drains, not when the event channel closes (fan-out forwarders hold sender clones, so waiting for both left the socket open for up to 2 s after teardown and broke the reattach scenario about 40 percent of the time). The spec's order-isolation budget became an absolute 5 ms p99 because idle is 0.08 ms and a ratio is noise. The gateway session gate is still held by the time-bar and tick history loads and by the place/cancel one-shots; phase 4 removes it, and the isolation result above uses a fill-history load, which already runs without the gate.

## Phase 4 results and decisions

- **Gateway gate is shared/exclusive.** `SessionHandle` wraps an `RwLock`: RPCs take it shared (`lock()`), so no RPC waits behind another and a time-bar load that holds it no longer holds up a place; reconnect, restore and shutdown take it exclusively (`lock_exclusive()`), which waits for in-flight calls and keeps new ones out. Waiting for events needs no gate (`checkout()` once). A rebuild still waits for an in-flight RPC, bounded by that RPC's own deadline.
- **`DirectSession` plant I/O runs on cloned handles.** `RithmicSession::handles()` returns owned clones of the ticker, history and PnL handles under an instant's lock; subscribe, unsubscribe, front month, reference data, tick and time-bar loads, probes, time-bar subscriptions and PnL subscribe all run on that snapshot, so a multi-minute history load no longer holds the mutex that `ensure_order_plant`, other subscribes and placements need. Event retrieval already bypassed the mutex in phase 2.
- **Order operations are concurrent.** `session_ops` is an `RwLock`: place, cancel and modify hold it shared and run together; `disconnect` takes it exclusively with the existing bounded wait, and a waiting disconnect keeps new order ops out.
- **Deadlines.** A plant connect fails after 60 s with a typed `Error::Session` instead of retrying forever (rithmic-rs' `Retry` has no deadline); the wrapper only abandons a connect that has assigned nothing, so no half-built state is left. `list_systems` has a 30 s deadline and runs with the GIL released.
- **4.6 not triggered.** The harness's order-isolation scenario runs against the fake plant, which has no `inner` mutex, so it cannot show `DirectSession` contention either way. The fix is structural (the lock is no longer held across I/O) and is covered by unit tests of the gate and of concurrent order ops; a measured direct-mode comparison needs real plants and stays with the live verification that is already open. The atomically-swapped snapshot design is therefore not pursued.
- **Measurement caveat.** This host runs unrelated heavy workloads (load average 15 to 19 on 10 cores during the final runs), so tail latencies vary by an order of magnitude between runs (order p99 under load: 0.5 ms clean, 1.4 to 8 ms busy). Records in `docs/perf` are single runs; budgets have headroom accordingly and CI gates only on loss, ordering and loose floors.

## Phase 5 results and decisions

`docs/perf/after-phase5.json`, `after-phase5-ffi.json`.

| Measure | Before | After |
|---|---|---|
| Server publish path allocations per event | 4 (107 bytes) | 1 (50 bytes) |
| Python adapter polling shape (S7) | 20,000 to 30,000 events/s (one thread hop per event, fixed idle sleeps) | about 700,000 events/s (one hop and one lock-free wait per batch of up to 256, no idle sleep) |
| 2 minute soak at 10,000 events/s | n/a | 1.2 M events, 100 percent delivered, no unsignalled loss, RSS 13.1 to 11.5 MB |

What changed, and where it differs from the plan:

- **Where the 4 allocations were.** Two cloned key strings and two buffers in `codec::encode` (payload, then a second copy with the length prefix). `encode` now writes prefix and message into one buffer; routing is decided from borrowed strings (`route_of`) against a `symbol -> exchange` map, so the hot path builds no key; an event whose topic has no attached client is dropped before it is converted or encoded. Strings move into the wire message instead of being cloned.
- **Per-kind delivery (5.2) is a forwarder filter, not a re-keyed topic.** The plan re-keyed hub topics as `(symbol, exchange, kind)`. That would have changed refcounting, `RestorePlan` and every dispatch path; instead each frame carries a `Kind` tag and each client's forwarder holds a bit mask of the kinds it asked for (maintained in `client_insert` / `client_remove`), so a ticker-only client is sent no time-bar or book frames. Same observable behavior, a fraction of the change; the broadcast clone of a frame is a reference-count bump.
- **Client queues share events by reference and arm on use.** Every facade queue holds `Arc<PlantEvent>` (one decode however many facades read it) and a queue is filled only after its facade subscribed to or polled that channel; the subscribe arms before its RPC is sent, so nothing is lost. A facade that uses a channel pays for it; one that does not pays nothing. The queue cap rose from 4,096 to 16,384.
- **Python pollers use `next_events`.** The data client's poll loop and the exec client's `PlantPoller` take batches (256 per wake, 50 ms bounded wait inside Rust), so there are no idle sleeps; a `gap` event triggers the same resync as a channel error. `poll_*` remain for callers that want one event (inventory drains, verify, the lake client).
- **`ts_event_ns` is computed once, in Rust.** Market-data dicts carry the effective event time, so `_ts_ns` is a dict read; the `rithmic-exec` rule still serves dicts with only `ssboe`. `symbol_and_exchange_from_instrument_id` is cached like the forward direction.
- **The transport callback is cancellable.** `subscribe_transport_events` returns a `TransportSubscription`; the Python wrapper hands the extension a weak relay (so the task never keeps the session alive), and cancels the registration when the last listener leaves and on disconnect.

## Phase 6 results and decisions

- **Classes are assigned where the error arises.** rithmic-rs already separates the cases (`RequestRejected`, `NoTradeRoute`/`InvalidArgument`, and the connection errors), so `rithmic_plants::Error` gains `Rejected` and `NotSent` beside the existing `OutcomeUnknown`, and the order helpers (`place`, `cancel`, `modify`, brackets, `cancel_all`) classify at the call: a venue reject is `Rejected`; a call that never left (invalid argument, no trade route, local validation, plant missing, session disconnecting) is `NotSent`; anything else after the request may have left is `OutcomeUnknown`. The gateway maps the three to wire codes `venue_rejected`, `not_sent`, `venue_unknown`; `GatewaySession` maps them back to the same typed errors; PyO3 raises `GatewayError` with codes `venue_rejected`, `not_sent`, `outcome_unknown` in both modes. `FakeFailure` gained the three classes and one test drives each through the direct path and through a gateway client and compares.
- **The exec client acts on the class.** A reject or a not-sent request after `OrderSubmitted` is a definitive failure of that order: `OrderRejected` (or `ModifyRejected` / `CancelRejected` for those commands) and the plant keeps trading. Only an outcome that may be live latches the plant for a recon cycle, which is what every failure used to do. A not-sent request cannot be a pre-submit deny at this point (the submit event was already announced); rejecting it is the only legal Nautilus transition and says so in its reason.
- **`DirectSession` enforces the gates itself.** Trading RPCs (place, cancel, modify, bracket place/adjust, order and bracket subscribe, `ensure_order_plant`, working-order, RMS and history loads) fail with `trading_disabled`, and cancel-all with `cancel_all_denied`, the same codes the parent answers, before anything is sent. Both gates are closed at construction. The session is a process singleton shared by the data and exec clients, so a gate can only be opened (`allow`, OR semantics), by the exec client from its `enable_trading` and from `RITHMIC_GATEWAY_CANCEL_ALL`; one client cannot close it under another. The gateway parent builds its `DirectSession` with the parent's own gates, so it is enforced there as well as at dispatch. `trading_enabled` / `cancel_all_enabled` on the extension return the real gate in both modes (direct used to return `None`, which the exec client read as "allowed").
- **Not done here:** `NoTradeRoute` and other non-constructible rithmic-rs variants cannot be built in unit tests (the types are `non_exhaustive`); the reject arm is covered end to end through the fake.

## Risks / Trade-offs

- [Per-stream tasks reorder across streams] → ordering is only guaranteed within a stream today; the spec keeps it that way; order and PnL are separate streams already.
- [Event-driven tasks starve the order stream under a ticker flood] → separate tasks plus the S2 isolation gate; if tokio scheduling is not enough, give the order stream a dedicated runtime thread (measured decision).
- [Gap frames confuse older clients] → additive proto; verify the Python `GatewayClient` ignores unknown bodies and add the test before shipping.
- [Concurrent order ops raise ordering questions] → only the command lane processes place/cancel/modify, in arrival order per connection; concurrency is between connections, as today.
- [Failing in-flight calls on reconnect looks like a regression] → they were already racing a rebuilt venue session; they now fail as unknown, which is the specified class.
- [Batch polling changes Python pollers on the hot path] → keep single-event `poll_*` as thin wrappers until the pollers migrate, then remove; the parity test covers both.
- [Harness numbers depend on the machine] → CI uses loose floors and the loss/isolation invariants; absolute budgets are checked in the manual full run and recorded in the committed report.
- [Live behavior unverified] → the harness bounds the software only; tick delivery at market rates needs a market-data account (open task 5.3 of `rust-unified-session`).

## Migration Plan

Phases are separate commits, each green on `cargo test`, `uv run pytest` and the harness smoke: 1 harness and baseline, 2 event-driven pump and gap, 3 lanes, 4 lock diet, 5 copy diet and FFI batching, 6 error taxonomy and direct gate, 7 recovery spike. Proto changes are additive, so a rollback of any phase leaves the wire compatible. The wheel does not ship the harness.

## Open Questions

- What are real peak per-symbol and aggregate tick rates on the target account? Needs a capture.
- Option A or B for recovery (decision 9).
- Does the order stream need its own OS thread, or is a separate task enough (decided by S2)?
- Should `RITHMIC_SUBSCRIPTION_CAPACITY` be exposed, and at what default, once the baseline shows buffer headroom?
