# Direct host-hop proxy — 2026-10-15

Task 5.5a2 for `openspec/changes/unify-session-host`, design D8.9 and spec
[Direct mode keeps its throughput](../../openspec/changes/unify-session-host/specs/session-host/spec.md#requirement-direct-mode-keeps-its-throughput).
Synthetic FakeVenue only; no credentials, live plants, sockets, Python or FFI.
See [README.md](README.md) for the harness and
[pre-session-host baseline](2026-10-15-pre-session-host-baseline.md) for the earlier
gateway/FFI measurements; those are not a pre-switch direct baseline.

## Verdict

**The proxy does not meet the no-lower-throughput bar.** The host median is
2,200,478 events/s versus 5,951,355 events/s for the venue: **63.0% lower**
(host/venue = 0.3697, or 2.70× as much elapsed time per event). The throughput
ranges do not overlap. This is not “within noise” under the requested rule
(<5% median difference with overlapping ranges).

The literal spec asks for Python FFI throughput against a baseline recorded
before the switch. This Rust-only proxy measures the added host delivery path;
it cannot establish that FFI acceptance requirement. Task 5.5a2 stays unchecked.
No production delivery code or capability/STATUS claims changed.

## Environment and provenance

- Worktree: `/private/mnt/DATA/code/rithmic-connect-wt/b-transport`.
- Branch: `ush/g-perf`; base revision: `c76859b53dc0d9f817cd1b387f41d22b1e2ad787`.
  Measurements used that revision plus this commit's harness additions.
- Machine: Apple M4, 10 logical CPUs, 16 GiB RAM, arm64.
- OS: Darwin 25.6.0, `ozeronmacmini.local`, kernel `25.6.0` / `RELEASE_ARM64_T8132`.
- Compiler: `rustc 1.98.0 (88d9e12ae 2026-08-18)`; optimized release binary.
- `target/release/rithmic-load` SHA-256:
  `808b4ea336a5a47c5b72e8bb599730f28249ed90741ea12de4f65e93ef401909`.
- First/last sample start (UTC): `2026-10-15T14:23:53.668873+00:00` / `2026-10-15T14:23:54.743767+00:00`.
- Each sample was a separate process. Five rounds ran sequentially in order
  `venue, host; host, venue; venue, host; host, venue; venue, host`.
- No `cargo`, `rustc`, `clang`, `clang++`, `maturin` or `rust-lld` process appeared
  in `ps -axo pid=,comm=` immediately before or after any sample. Our checks/build
  completed before the series; another build was observed only after the series.
- Load averages (1/5/15 min), before and after **every** sample:
  `13.126953125 / 8.44189453125 / 7.1904296875`. These are also recorded per sample
  in JSON. The one-minute average exceeded the core count after the release
  build: this shared Mac was not a proven quiet host. The series is short enough
  that the load-average values did not update. No host scheduling/noise control
  beyond interleaving and build-process checks is claimed.

## What is measured

Both paths instantiate the same `FakeVenue`, close trading/cancel-all gates,
connect, and subscribe the same `NQH6`/`CME` ticker. The host path wraps that fake
in `SessionHost<FakeVenue>` with default queues/timing and uses `attach_owned()`.
The venue path reads the fake's own stream directly.

**Checkout correction:** at the supplied base revision, the old blanket
`impl Session for V: Venue` has already been removed. The venue case therefore
calls `Venue::next_events` directly, exactly the delegation the old blanket
implementation performed. It does not reintroduce that compatibility API.
The host case calls `Session::next_events` on `LocalSession`.

Each case warms 8,192 events, then scripts 200,000 measured ticker events in
unpaced bursts of at most 1,024. Each burst fully drains through
`next_events(Stream::Ticker, 1024, Duration::from_secs(10))` before the next burst.
The last burst is partial. At most 1,024 events are in flight, below all default
queue capacities (65,536), so this isolates lossless delivery without overflowing
the fake or hiding a loss as throughput. This closed-loop burst workload is a
proxy for the hop cost, not an open-loop saturation/overload test.

`duration_s` spans first fixture creation/publish through final take and sequence/
histogram accounting. `min_throughput_eps` is received divided by that actual
elapsed time, not an offered-rate setting. Setup, histogram allocation, warmup,
disconnect and JSON serialization are excluded. The monotonic timestamp is set
immediately before `FakeVenue::push_event`; latency ends at `next_events` return,
before per-event accounting. All events in a returned batch share its take time.
The existing histogram reports microseconds, with a 1 µs floor.

The existing process-global counting allocator applies to the entire measured
loop: fixture symbol/exchange strings, channels, batch vectors, host fan-out and
native cloning, plus runtime work. This differs from `alloc_publish_*`, which
counts only the server publish path. Atomic allocation counting itself adds
shared contention, particularly on the host's multiple tasks; these numbers
are for the instrumented harness, not an allocator-free production profile.
RSS is per-process peak including setup/warmup, in KiB in JSON and MiB below.

Every sample received all 200,000 events and histogram observations. Sequence
checks enforce exact order from zero, including the first/last event. There
were no gaps, skips, duplicates, reordered events or unsignalled losses. A gap,
wrong sequence, missing timestamp, unexpected event, or drain timeout fails the
scenario rather than producing a deceptively healthy report.

## Medians (five samples per path)

Latency columns are the medians of each run's percentile, not pooled percentiles.

| Path | Events/s | Events/s range | p50 µs | p99 µs | p99.9 µs | Allocs/event | Bytes/event | Peak RSS MiB |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `venue` | 5,951,355 | 5,721,635–5,999,265 | 103 | 145 | 151 | 2.038 | 1,479.2 | 8.48 |
| `host` | 2,200,478 | 2,154,881–2,268,293 | 203 | 301 | 320 | 5.647 | 3,806.2 | 14.58 |

## Every sample, in execution order

All rows used 200,000 measured events plus 8,192 warmup events. Round.position
makes the alternating within-round order explicit. Full precision, timestamps,
load averages, parameters and all standard report fields are preserved in the
[harness-native JSON array](2026-10-15-direct-hop.json).

| Sample | Round.position | Path | Duration s | Events/s | p50 µs | p99 µs | p99.9 µs | Allocs/event | Bytes/event | Peak RSS MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1.1 | `venue` | 0.034955 | 5,721,635 | 107 | 152 | 168 | 2.038 | 1,479.2 | 8.45 |
| 2 | 1.2 | `host` | 0.091808 | 2,178,462 | 206 | 367 | 435 | 5.653 | 3,824.0 | 15.06 |
| 3 | 2.1 | `host` | 0.089364 | 2,238,042 | 201 | 297 | 315 | 5.647 | 3,806.2 | 14.56 |
| 4 | 2.2 | `venue` | 0.033606 | 5,951,355 | 103 | 144 | 149 | 2.038 | 1,479.2 | 8.48 |
| 5 | 3.1 | `venue` | 0.033337 | 5,999,265 | 102 | 143 | 151 | 2.038 | 1,479.2 | 8.50 |
| 6 | 3.2 | `host` | 0.088172 | 2,268,293 | 205 | 291 | 306 | 5.581 | 3,770.3 | 14.58 |
| 7 | 4.1 | `host` | 0.090889 | 2,200,478 | 203 | 301 | 320 | 5.682 | 3,829.6 | 14.03 |
| 8 | 4.2 | `venue` | 0.033839 | 5,910,376 | 104 | 145 | 149 | 2.038 | 1,479.2 | 8.48 |
| 9 | 5.1 | `venue` | 0.033501 | 5,970,060 | 103 | 150 | 254 | 2.038 | 1,479.2 | 9.00 |
| 10 | 5.2 | `host` | 0.092813 | 2,154,881 | 202 | 569 | 855 | 5.568 | 3,793.5 | 15.06 |

## Where the added work is

The measured allocation increase is 3.608 allocations/event and 2,327.0 bytes/event
(differences of medians). The source path explains extra work absent from the
venue case: `pump::serve_stream` takes and publishes each event;
`fanout::publish_plant_event` routes it through the hub and allocates an `Arc`
native record; `local/context.rs::spawn_forwarder` moves it from the topic's
broadcast ring to the session's mpsc queue; `LocalSession::ensure_native_delivery`
clones the owned event (including its symbol/exchange strings) into another
`StreamHub`; the consumer drains that stream. There are extra wakeups, queue
operations, locks, batches, one shared record and owned-string copies.

This is source-level attribution supported by allocation counts, not measured
CPU percentages. `cargo-flamegraph` was not installed; no tools were installed.

## Reproduce

Run only in this worktree, with no other builds running:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p rithmic-loadtest
cargo build -p rithmic-loadtest --release
./target/release/rithmic-load list
./target/release/rithmic-load run direct_hop_venue --events 200000 --out target/venue.json
./target/release/rithmic-load run direct_hop_host --events 200000 --out target/host.json
```

`--events` defaults to 200,000 and accepts 1 through 2,147,483,647; `--rate` and
`--secs` apply to the existing paced scenarios, not this pair. These scenarios
are individually selectable with `run`/`list` and do not lengthen `suite`/`smoke`.
For the recorded series, a Python driver ran the two commands five times each
using the alternating order above, guarded every invocation with the `ps` check,
recorded `os.getloadavg()` before/after, and concatenated each harness report
into the companion array. It added provenance only under
`params.measurement` (round/position, UTC start, source revision, binary hash,
load averages and the empty build-process list); all metric fields are the
unmodified harness output.

## Verification

- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `cargo build -p rithmic-loadtest --release`: passed.
- `./target/release/rithmic-load list`: includes `direct_hop_venue` and `direct_hop_host`.
- `cargo test -p rithmic-loadtest`: passed; new CLI integration test runs both
  routes with 70,001 events (more than stream capacity, partial final burst),
  checking delivery/order, JSON fields, nonzero throughput/allocations/RSS and
  percentile order. Before implementation it failed with
  `unknown scenario direct_hop_venue`.
- No live tests ran. Adapter conventions for emitted Nautilus events are N/A:
  this change only adds a synthetic Rust harness and performance documentation.

## After hop optimization

The original sections above describe the earlier implementation and are retained
as historical evidence. This section measures the optimized implementation on
`ush/i-hop-opt`, based on `0eb0b177203d0ab970909805f6df633c57c8a520`.

**The final candidate misses the requested 10% throughput target.** Its median is
4,385,885 events/s versus 5,882,850 for the venue,
**25.4% lower** (host/venue = 0.7455). The historical host median
was 2,200,478 events/s; the new host median is 1.99× that
rate. Allocations fell from 5.647 to 2.231/event, versus
2.038 for the matched venue. The historical comparison spans different
runs/load conditions; the final interleaved venue is the acceptance control.
The FFI requirement in task 5.5a2 remains unproven and unchecked.

**The gateway no-regression gate is also not established.** Functional delivery
and ordering checks pass, but the latest series has higher median p99 in the
10k flood, reconnect and order-flood workloads, and higher order-RPC tails.
The tables below report those increases rather than treating overlapping ranges
as proof of equivalence. This is a verified functional candidate with incomplete
performance acceptance.

### Delivery path and ordering

The native path is now venue `StreamHub` → host pump → attachment queue →
`LocalSession::next_events`. `SessionCtx` installs weak topic delivery handles
instead of spawning a forwarder. Publication synchronously appends to the
matching session/stream queues. There is no topic-broadcast hop, per-session
mpsc hop, native delivery task, or second `StreamHub` on this path.

For a homogeneous batch with one interested native attachment, the host moves
the venue's owned batch into that attachment and moves it out at `next_events`;
its strings are never cloned. Other unicast native records are moved in owned
batches. Multicast retains the existing shared `Arc<EventRecord>` and memoized
encoding. Owned conversion happens once at the native consumer boundary
(`Arc::try_unwrap` moves the last record when possible); the gateway keeps the
shared/encoded record through wire encoding. Explicit compatibility broadcast
receivers still work, but their rings are allocated only when requested.

The gateway writer consumes these **same attachment queues** through
`EventReceiver`, replacing its mpsc receiver. `take_events` still works after
subscribing and before native polling. RPC kinds, lanes, gates, intent sets,
refcounts, reconnect restore and wire/event DTO shapes are unchanged.

The guarantees are:

- Each session has an independent bound per stream, set by the host queue
  capacity. A full queue evicts only that session's oldest records and prefixes
  the retained records with an exact typed gap. Evicting an existing gap carries
  its skipped count forward. A stalled session never backpressures its peers.
  The queue layout/depth changed, so retained prefixes and skip counts can
  differ from the old multi-queue path; the contract permits depth-dependent
  counts and still requires delivered + skipped conservation.
- A stream's pump publishes in venue source order. All topics feeding that
  stream append directly to one session queue, so their source order is kept.
  The old topic forwarders only guaranteed FIFO within each topic; cross-topic
  task scheduling had no source-order guarantee. Different plant streams still
  have independent source ordering; no venue-wide order is invented.
- The gateway merges stream heads by attachment enqueue sequence. Native reads
  honor `max`, including partial batch reads, and preserve gap/record order.
  Reads are cancellation safe; concurrent readers hand off wakeups when records
  remain.
- Before lifecycle notification the host still quiesces every publisher. Native
  records/gaps are already queued at that point. A gateway barrier is sequenced
  after preceding records and acknowledged by the writer after their writes;
  the existing two-second wait for a stalled writer's barrier acknowledgment is
  retained. Queued records drain before terminal close; recovery keeps streams
  open and returns empty batches.

No contract semantics were changed. In particular, failure after a taken batch
stays pending until that batch is published. With native attachments, the pump
cooperatively coalesces already available records up to its existing 1,024-record
bound, without a timer or waiting for a full batch. Gateway-only hosts publish
immediately: their records are encoded individually, so this extra yield/drain
has no batch-move benefit. Empty zero-timeout `StreamHub` takes now return
immediately after checking queued records/failure/close instead of waiting for
a timer tick.

### Failure found during validation

The first development candidate's reconnect-under-flood trial exposed an
existing `StreamHub::take` deadlock. An `if let` temporary retained the failure
mutex while a late queued record made the code try to lock that same mutex to
put the failure back. A process sample showed the pump blocked in
`StreamHub::take` → `std::sync::Mutex::lock`. The new deterministic
`a_record_arriving_during_the_failure_check_is_drained_without_relocking_failure`
test failed with `late drain deadlocked on its own failure mutex` before the fix
and passes after moving the failure take into a separate statement. The final
candidate completes recovery; the frozen base still contains that bug.

### Final direct-hop measurements

Five rounds, alternating `venue, host; host, venue; venue, host; host, venue;
venue, host`, with the original 200,000 measured / 8,192 warmup / 1,024 burst
parameters. Every trial delivered all 200,000 records in exact sequence, with
zero gaps, duplicates or unsignalled loss. Percentiles are medians of per-run
percentiles, not pooled percentiles.

| Path / version | Events/s median | Events/s range | p50 µs | p99 µs | p99.9 µs | Allocs/event | Bytes/event | Peak RSS MiB median |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Historical venue | 5,951,355 | 5,721,635–5,999,265 | 103 | 145 | 151 | 2.038 | 1,479.2 | 8.48 |
| Historical host | 2,200,478 | 2,154,881–2,268,293 | 203 | 301 | 320 | 5.647 | 3,806.2 | 14.58 |
| Final venue | 5,882,850 | 5,051,223–5,889,029 | 104 | 148 | 164 | 2.038 | 1,479.3 | 8.28 |
| Final host | 4,385,885 | 4,066,446–4,741,162 | 111 | 210 | 220 | 2.231 | 2,103.8 | 9.08 |

| Round.position | Path | Events/s | p50 µs | p99 µs | Allocs/event | Bytes/event | Peak RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1.1 | `venue` | 5,769,016 | 105 | 153 | 2.038 | 1,479.3 | 8.28 |
| 1.2 | `host` | 4,385,885 | 111 | 210 | 2.225 | 2,100.4 | 9.06 |
| 2.1 | `host` | 4,480,709 | 108 | 209 | 2.231 | 2,103.8 | 9.38 |
| 2.2 | `venue` | 5,885,043 | 104 | 148 | 2.038 | 1,479.3 | 8.23 |
| 3.1 | `venue` | 5,882,850 | 104 | 148 | 2.038 | 1,479.3 | 8.28 |
| 3.2 | `host` | 4,066,446 | 119 | 225 | 2.282 | 2,117.3 | 8.91 |
| 4.1 | `host` | 4,741,162 | 101 | 198 | 2.144 | 2,251.2 | 9.73 |
| 4.2 | `venue` | 5,051,223 | 121 | 175 | 2.038 | 1,479.3 | 8.75 |
| 5.1 | `venue` | 5,889,029 | 104 | 148 | 2.038 | 1,479.3 | 8.36 |
| 5.2 | `host` | 4,282,201 | 114 | 213 | 2.241 | 2,094.2 | 9.08 |

The remaining host overhead includes its required pump/task handoff, routing and
queue synchronization, extra dynamic `Venue::next_events` calls during burst
drains, and batch growth when coalescing takes. The process-global allocator
counter also contends across the producer and pump, while the venue case
produces and consumes in one task. These are source-level explanations for
remaining work, not measured CPU percentages or proof that the 10% target is
impossible. This implementation removes the diagnosed extra hops/clones but
still fails that performance gate.

### Gateway comparisons against the frozen base

Three valid interleaved rounds per scenario: base/candidate in rounds 1 and 3,
candidate/base in round 2. Preset rates, durations, connections, slow RPC and
plant-drop timing were unchanged. These workloads are paced, so delivered
rates are not saturation-throughput measurements. Raw records, including order
RPC percentiles, gaps, resumes and load averages, are in
[the gateway JSON](2026-10-15-hop-opt-gateway.json).

Bracketed values are min–max across the three trials; medians precede brackets.

| Scenario | Version | Delivered events/s | p50 µs | p99 µs [range] | p99.9 µs | Peak RSS MiB [range] |
| --- | --- | ---: | ---: | --- | ---: | --- |
| `stalled_consumer` | base | 20,000 | 109 | 347 [336–7,023] | 1,358 | 26.34 [25.33–27.11] |
| `stalled_consumer` | candidate | 20,000 | 108 | 337 [337–1,261] | 782 | 22.42 [22.25–22.69] |
| `flood_10k_4conn_1session` | base | 10,000 | 95 | 276 [251–493] | 687 | 15.42 [15.25–15.44] |
| `flood_10k_4conn_1session` | candidate | 10,000 | 86 | 291 [260–1,532] | 1,600 | 10.98 [10.88–10.98] |
| `reconnect_during_flood` | base | 9,998 | 65 | 188 [185–202] | 480 | 13.84 [13.64–13.97] |
| `reconnect_during_flood` | candidate | 9,998 | 60 | 222 [192–412] | 3,715 | 9.73 [9.67–9.75] |
| `orders_flood_slow_rpc` | base | 50,000 | 290 | 991 [965–1,351] | 47,519 | 15.17 [14.80–15.48] |
| `orders_flood_slow_rpc` | candidate | 50,000 | 288 | 1,415 [970–1,869] | 6,699 | 11.23 [10.78–11.53] |

| Order RPC phase (`orders_flood_slow_rpc`) | Base p99 µs [range] | Candidate p99 µs [range] |
| --- | --- | --- |
| Idle | 175 [153–178] | 206 [176–276] |
| Flood + slow RPC | 775 [732–851] | 846 [819–885] |

Relative to base, candidate median event p99 changed by −2.9% for stalled
consumers, +5.4% for the 10k flood (+15 µs), +18.1% for reconnect (+34 µs),
and +42.8% for order flood (+424 µs). Idle order-RPC p99 increased 17.7%
(+31 µs); loaded order-RPC p99 increased 9.2% (+71 µs). Candidate median RSS
fell in every scenario, and healthy consumers delivered the complete offered
rate in every non-recovery trial. Recovery gaps remained explicit and accounted
for missing records. Higher p99.9 for the 10k/reconnect cases is visible above;
it is not hidden by the improved p50 or memory figures.

All 24 valid trials have zero unsignalled loss and zero reordered records.
The frozen base timed out 1 time(s) during reconnect; those failed trials
are retained separately in [the failure JSON](2026-10-15-hop-opt-gateway-failures.json)
and excluded from successful-run medians. Baseline reconnect retries were bounded
at four per resumed round. This conditions the comparison on successful base
runs; it does not hide the base's recovery failure. A development candidate trial
was also aborted before the mutex fix and is not part of the final measurements.
Additional trial attempts were discarded when the process monitor observed
Maturin/cargo/rustc/clang/Swift builders; they are not included in the valid data.
The series waited between attempts for those interruptions, with the assigned
round order preserved on resume.

### Repeated measurements and noise limits

The final source and binary were measured twice. Before the user coordinated a
five-minute build pause, a complete five-round direct series and three-round
gateway series finished across multiple quiet windows. Those accepted trials
also had no observed builders; interrupted attempts were discarded. Their raw
results are retained in [the earlier direct JSON](2026-10-15-direct-hop-after-busy.json)
and [the earlier gateway JSON](2026-10-15-hop-opt-gateway-busy.json).

The tables above use a fresh series started immediately after that coordination.
Its direct trials completed without interruption at a one-minute load of 3.969.
Builders were observed again before the final three gateway trials, so those
trials waited for quiet windows too. Accepted samples all have empty
before/during/after builder lists. The gateway series ran from
`2026-10-15T15:44:01.458453+00:00` to `2026-10-15T15:48:57.183451+00:00`.
The metadata's `coordinated-build-pause` label identifies this requested series;
it does not assert that every attempt ran during the pause.

The earlier direct series measured 4,324,106 host versus 5,524,410 venue events/s
(21.7% lower), also failing the 10% bar. The earlier gateway series had lower
candidate median p99 for stalled, reconnect and order-flood workloads, but 9.7%
higher p99 for the 10k flood. Idle order-RPC p99 was 179 versus 149 µs there,
also higher for the candidate. Direction and size varied across series, with
large tail outliers despite the process checks. These data do not establish
the requested absence of regressions. A stable workload profile and further
optimization are still needed; overlapping ranges alone do not close that gate.

### Provenance and verification

- Same worktree and machine as the original report; release optimization.
  Frozen base binary SHA-256: `808b4ea336a5a47c5b72e8bb599730f28249ed90741ea12de4f65e93ef401909`.
  Final candidate binary SHA-256: `2be72469aa5c19bb4b3b445a8ba474af85acb0838a4cceb42d1a9336670005a2`.
  Candidate Rust-source SHA-256: `d65c25ec71fa3dd64a990051a4cc7e13e1cfa7c61c56658317eefdf1c71e8455` (sorted changed `.rs` paths and
  their bytes, including `local/delivery.rs`). The commit introducing this section
  contains that source; no binaries or one-off probe scripts are committed.
- Final direct trial UTC starts: `2026-10-15T15:43:59.792772+00:00`
  through `2026-10-15T15:44:01.282872+00:00`.
  Direct-trial 1/5/15-minute load ranges: 3.969–3.969 / 5.721–5.721 / 6.645–6.645.
  All valid-trial 1/5/15-minute load ranges: 3.616–16.648 / 5.424–8.203 / 6.352–7.276.
  Every sample has before/after process checks and a 0.2-second build-process
  monitor; no listed builder was observed in any accepted sample. This shared
  Mac still had scheduling noise; no isolated-core or quiet-host claim is made.
- Full precision, timestamps, hashes, order and load averages are retained in
  [the final direct JSON](2026-10-15-direct-hop-after.json) and the gateway JSON.
  Metric fields are the harness output; added provenance lives under
  `params.measurement`. The initial timed-out base trial lost exact load/time
  metadata when its runner stopped; its failure record says so.
- `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`
  and the requested six-package Rust test command with gateway/host/extension
  testing features passed: **503 tests**. The unchanged client scenario suite
  (Local/Remote conservation included), host recovery ordering, and all four
  ticker-failure characterizations passed.
- `uv sync --extra dev` passed; the final extension was additionally rebuilt with
  `--reinstall-package rithmic-nt-connect` so Python tested this Rust source.
  `uv run pytest -q`: **832 passed, 101 skipped**, with two EOF-on-shutdown thread
  warnings in existing Python fake-server tests. `uv run ruff check .`,
  `uv run ruff format --check .` and the requested `uv run ty check` passed.
- Added coverage proves exact stalled-LocalSession gap conservation with a
  lossless peer; multi-topic lifecycle enqueue barriers; partial-batch/overflow
  order; concurrent-reader wakeups; true zero-timeout drains; and the late-record
  failure-mutex race. The removed forwarder's ticker/bar/account gap helper tests
  were replaced by actual attachment-overflow tests on those consumer channels.
  The two primary isolation/barrier tests passed on the frozen source before the
  implementation changed, establishing their meaning independently of the new
  queue layout.
- No live tests ran. Data conversion, timestamps, instrument identity, execution
  classification, command retry policy, config and credentials are untouched.
  The applicable stream isolation, typed-gap, ordered failure and recovery rows
  are exercised by the tests above. No advertised capability or STATUS mark changed.
