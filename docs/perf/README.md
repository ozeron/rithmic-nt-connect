# Load harness reports

`rithmic-load` (crate `crates/rithmic-loadtest`) drives the real gateway server
over a fake plant, in one process, on a real unix socket. No Rithmic credentials.

```bash
cargo build -p rithmic-loadtest --release
./target/release/rithmic-load list
./target/release/rithmic-load suite --out docs/perf/report.json          # ~8 min quick suite
./target/release/rithmic-load suite --full --out docs/perf/report.json   # spec durations (30 s flood, 10 min soak)
./target/release/rithmic-load smoke                                      # CI subset, ~25 s, exits non-zero on failure
./target/release/rithmic-load run flood_10k_4conn_1session --rate 20000 --secs 10
./target/release/rithmic-load run direct_hop_venue --events 200000 --out target/venue.json
./target/release/rithmic-load run direct_hop_host --events 200000 --out target/host.json
RITHMIC_PERF=1 uv run pytest tests/test_perf_ffi_throughput.py -s         # S7: Python boundary
```

Reading a report: `min_delivered_ratio` is received/sent for the slowest reading
consumer at the end of the drain window (a ratio below 1 with no gap means the
events were still queued, not lost). `latency` is creation-to-delivery. Order
latency is measured open-loop: a place+cancel pair starts every 20 ms whether or
not the previous one finished. `allocs_per_event` counts only the server publish
path for `alloc_publish_*`; for `direct_hop_*` it covers the whole measured loop
(fixture creation, publish, delivery and accounting).

`direct_hop_venue` / `direct_hop_host` compare the same fake's own ticker stream
with delivery through `SessionHost<FakeVenue>` / `LocalSession`. They use unpaced
bursts of at most 1,024 events, fully drained before the next burst, and default
to 200,000 measured events (`--events`), after 8,192 warmup events. Throughput is
received/actual elapsed time; latency ends at the batched `next_events` return.
This Rust-only hop proxy does not measure Python FFI throughput. See
[the five-round comparison and limitations](2026-10-15-direct-hop.md) and its
[raw JSON](2026-10-15-direct-hop.json).

All parties share one process and CPU, so absolute numbers are conservative; compare
runs on the same machine.

- `baseline-before.json`, `baseline-before-ffi.json`: the unmodified code, before `harden-gateway-runtime` (Rust suite and Python S7).
- `after-phase2.json`, `after-phase2-ffi.json`: after event-driven delivery and gaps (phase 2).
- `after-phase3.json`: after per-connection lanes (a slow read no longer holds up orders or events).
- `after-phase5.json`, `after-phase5-ffi.json`: after the copy diet (1 allocation per published event, shared client events, batched Python polling).
- `after-phase7.json`: reconnect during a flood after parent-owned restore (one venue subscribe, no unsubscribe, no client reattach).
- `baseline-after.json`: the quick suite on the final code (quiet host); `after-soak-full.json`: the 10 minute soak (RSS growth 3.3 percent after the first minute).

Noise: the harness shares the machine with whatever else is running. On a quiet machine latencies are sub-millisecond; with a load average above the core count tails move by 10x between runs. Compare on a quiet host and repeat before trusting a single tail number.
