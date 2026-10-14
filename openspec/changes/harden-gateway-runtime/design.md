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
