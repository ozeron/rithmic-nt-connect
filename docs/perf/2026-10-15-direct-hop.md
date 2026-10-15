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
