# Load harness reports

`rithmic-load` (crate `crates/rithmic-loadtest`) drives the real gateway server
over a fake plant, in one process, on a real unix socket. No Rithmic credentials.

```bash
cargo build -p rithmic-loadtest --release
./target/release/rithmic-load list
./target/release/rithmic-load suite --out docs/perf/report.json          # ~8 min quick suite
./target/release/rithmic-load suite --full --out docs/perf/report.json   # spec durations (30 s flood, 10 min soak)
./target/release/rithmic-load run flood_10k_4conn_1session --rate 20000 --secs 10
RITHMIC_PERF=1 uv run pytest tests/test_perf_ffi_throughput.py -s         # S7: Python boundary
```

Reading a report: `min_delivered_ratio` is received/sent for the slowest reading
consumer at the end of the drain window (a ratio below 1 with no gap means the
events were still queued, not lost). `latency` is creation-to-delivery. Order
latency is measured open-loop: a place+cancel pair starts every 20 ms whether or
not the previous one finished. `allocs_per_event` is the server publish path only.

All parties share one process and CPU, so absolute numbers are conservative; compare
runs on the same machine.

- `baseline-before.json`, `baseline-before-ffi.json`: the unmodified code, before `harden-gateway-runtime` (Rust suite and Python S7).
- `after-phase2.json`, `after-phase2-ffi.json`: after event-driven delivery and gaps (phase 2).
