# Pre-session-host performance baseline — 2026-10-15

Baseline for `openspec/changes/unify-session-host`, task 0.1 and design D8.9,
before the host extraction. Synthetic fake plants only; no credentials or live/e2e
connections. Report interpretation follows [README.md](README.md).

## Environment and provenance

- Repository: `/private/mnt/DATA/code/rithmic-connect`
- Branch: `feat/unify-session-host`
- Git revision: `5088ade5a1b233d420819551dfc7a2ce19205866` (unchanged during measurement).
- Machine: Apple M4, 10 logical CPUs, 16 GiB RAM, arm64.
- `uname -a`: `Darwin ozeronmacmini.local 25.6.0 Darwin Kernel Version 25.6.0: Fri Jul 31 19:11:03 PDT 2026; root:xnu-12377.161.14~5/RELEASE_ARM64_T8132 arm64`
- Python: 3.13.13, Clang 22.1.3; uv 0.11.26; rustc 1.98.0.
- Measurement began around `2026-10-15T11:14:42Z`; Rust suite began around `11:15:35Z`.
- Shared development machine: load averages before FFI were `5.69 4.02 4.69`;
  before Rust suite, `8.18 5.40 5.18`. Other agents were editing docs/proto/tests.
  This was not an isolated or quiet-host run. Benchmarks ran sequentially.
- Initial tree had untracked OpenSpec changes. This task adds only the FFI access
  case and these perf artifacts; concurrent agents' files were left untouched.
- `uv run` rebuilt the editable PyO3 extension. Rust load harness uses `release`;
  the FFI fake parent uses the test helper's default `debug` build.
- SHA-256, measured binaries:
  - PyO3 `_lib.cpython-313-darwin.so`: `980e2a39c39def6c9b19e909bb68a45ad9236cb76b1943c531441a968b7a678c`
  - `target/release/rithmic-load`: `fa4c729ace4fee1f126447196e4e5185cc54deb673f92576cfe32602ef68e83b`
  - `target/debug/rithmic-gateway-fake`: `0963203c0eda2822653407b7d4ae4c1d8c2c6853552a1ad57d73dc65da09e90e`
- The fake-parent hash was captured immediately after FFI. Concurrent work later
  rebuilt that debug binary (post-suite hash
  `e29863da2934d6604853257f87f6cae3c590dd763c4ad6411b96fe83198517af`).
  The release load binary and PyO3 extension hashes stayed unchanged. The Rust
  suite runs the fake in-process and does not use the debug fake-parent binary.

## Exact commands

Run from the repository root:

```bash
uname -a
sysctl -n machdep.cpu.brand_string hw.ncpu hw.memsize
git branch --show-current
git rev-parse HEAD
uv --version
rustc --version
uptime
cargo build -p rithmic-loadtest --release
RITHMIC_PERF=1 RITHMIC_PERF_OUT=docs/perf/2026-10-15-pre-session-host-ffi.json uv run pytest tests/test_perf_ffi_throughput.py -q
./target/release/rithmic-load suite --out docs/perf/2026-10-15-pre-session-host-rust.json
uv run --no-sync ruff check tests/test_perf_ffi_throughput.py
uv run --no-sync ruff format --check tests/test_perf_ffi_throughput.py
uv run --no-sync ty check tests/test_perf_ffi_throughput.py
```

The FFI fixture internally runs:

```bash
cargo build -p rithmic-gateway --features testing --bin rithmic-gateway-fake
```

## Python FFI results

`5 passed in 30.97s`. Defaults: 2,000 trades, 120-second receive deadline;
empty polling uses 5,000 polls. Every trade scenario received all 2,000 events,
with zero reported skips.

| Scenario | Received / polls | Seconds | Events/s | Microseconds/poll |
| --- | ---: | ---: | ---: | ---: |
| `py_blocking_poll_event` | 2,000 | 0.002 | 957453.2 | — |
| `py_to_thread_poll_event` | 2,000 | 0.064 | 31228.2 | — |
| `py_to_thread_next_events_batch` | 2,000 | 0.002 | 1311977.0 | — |
| `py_empty_poll_cost` | 5,000 | 0.0012 | — | 0.2 |
| `py_to_thread_next_events_batch_field_access` | 2,000 | 0.002 | 1164059.7 | — |

The new additive access-pattern case uses the same batch size (256), timeout
(50 ms) and `asyncio.to_thread` hop as the existing batch case. For every trade it
reads `type`, `symbol`, `exchange`, `trade_price` and `trade_size`, validates the
identity and accumulates price × size. Checksum: `2199500.0`, also checked against
the expected sum when no events are skipped. All four existing cases are unchanged.

These timings start **after** the fake control channel has pushed the full burst.
They measure draining/FFI/field access, not producer-to-consumer streaming latency
or a sustained venue rate. Seconds are rounded by the harness; events/s uses the
unrounded elapsed time. Millisecond-scale cases are noisy: the single access-case
ratio to receive-only batching is descriptive, not a regression threshold.

## Measurement gaps and comparison limits

- **Direct-mode FFI baseline unavailable.** The current PyO3 constructor requires
  a password in direct mode and constructs `DirectSession` on real plants. It
  cannot accept `FakeSession`, and `Session.fake` is absent (confirmed on the
  rebuilt extension). No synthetic direct case was added. Task 5.5's fake-backed
  direct session is required to measure this path without credentials. This
  gateway baseline cannot establish D8.9's comparison to today's direct path.
- Rust results below are the default quick `suite`, not `--full` and not `smoke`.
  They do not certify full-duration throughput budgets. The `suite` command
  records measurements without enforcing the `smoke` command's invariant checks.
- Compare later on the same machine, build profiles and event counts. Tail latency
  is sensitive to concurrent work; a quiet-host repeat is needed before treating
  a tail change as a refactor regression.
- The perf addition emits no Nautilus data/execution events and changes no adapter
  capability claims or STATUS marks.


## Rust default-suite results

All 13 default scenarios completed (exit 0). All ten event-delivery scenarios
reported 100% minimum delivery, zero unsignalled loss, zero reordered events and
zero reported skips. The quick soak is 45 seconds. Peak RSS is process-cumulative
across this sequential suite, not independent per-scenario memory usage.

| Scenario | Sent | Minimum delivery | Minimum events/s | p50 ms | p99 ms | p99.9 ms | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `idle_wake` | 60 | 100% | 10 | 0.096 | 0.400 | 0.400 | 13.16 |
| `flood_1k_1conn` | 5,000 | 100% | 1,000 | 0.051 | 0.168 | 1.214 | 13.48 |
| `flood_10k_4conn_1session` | 50,000 | 100% | 10,000 | 0.113 | 1.289 | 10.303 | 14.91 |
| `flood_50k_4conn` | 400,000 | 100% | 50,000 | 0.419 | 14.695 | 26.495 | 16.02 |
| `burst_200k_1s` | 200,000 | 100% | 200,000 | 332.543 | 640.511 | 645.631 | 33.16 |
| `latency_20k` | 160,000 | 100% | 20,000 | 0.100 | 0.590 | 3.197 | 33.16 |
| `orders_flood_slow_rpc` | 600,000 | 100% | 50,000 | 0.263 | 1.356 | 4.911 | 33.16 |
| `stalled_consumer` | 160,000 | 100% | 20,000 | 0.075 | 0.391 | 2.947 | 33.16 |
| `reconnect_during_flood` | 80,000 | 100% | 10,000 | 0.074 | 0.423 | 2.563 | 33.16 |
| `soak` | 450,000 | 100% | 10,000 | 0.025 | 0.611 | 5.899 | 33.16 |

| Additional measurement | Result |
| --- | --- |
| Orders under 50k/s flood and 8-second read | Idle p99 0.144 ms; loaded p99 0.621 ms; loaded max 9.439 ms |
| Same-connection place behind 3-second read | Max 2.065 ms; place replied before read |
| `alloc_publish_1sub` publish allocation | 1.00 allocations/event; 52 bytes/event |
| `alloc_publish_4sub` publish allocation | 1.00 allocations/event; 52 bytes/event |

Reconnect delivered `TransportResumed` to both consumers, with one venue connect,
one disconnect, one subscribe and no unsubscribe after the drop. The parent
dropped the stalled consumer while healthy readers received every event.

Quick-soak current-RSS samples (KiB): `[14768, 13536, 9680, 9760, 9696, 10064, 10464, 10928, 10800, 10528, 11168, 11792, 12464, 12560, 12896, 12816, 12848, 12848, 12848, 12400, 11856, 11856, 11520, 11168, 10464, 10784, 11152, 10400, 10544, 10976, 11616, 11808, 11792, 12192, 12448, 12448, 12144, 11440, 11312, 9248, 10096, 10720, 11344, 11968, 12432]`.

## Raw JSON lines

The companion files retain the harness-native JSON arrays:
[FFI](2026-10-15-pre-session-host-ffi.json) and
[Rust](2026-10-15-pre-session-host-rust.json). Below is the same data as one
JSON object per line, with all fields preserved.

### Python FFI

```jsonl
{"scenario":"py_blocking_poll_event","events":2000,"received":2000,"reported_skipped":0,"seconds":0.002,"events_per_sec":957453.2}
{"scenario":"py_to_thread_poll_event","events":2000,"received":2000,"reported_skipped":0,"seconds":0.064,"events_per_sec":31228.2}
{"scenario":"py_to_thread_next_events_batch","events":2000,"received":2000,"reported_skipped":0,"seconds":0.002,"events_per_sec":1311977.0}
{"scenario":"py_empty_poll_cost","polls":5000,"seconds":0.0012,"us_per_poll":0.2}
{"scenario":"py_to_thread_next_events_batch_field_access","events":2000,"received":2000,"reported_skipped":0,"seconds":0.002,"events_per_sec":1164059.7,"notional_checksum":2199500.0}
```

### Rust default suite

```jsonl
{"scenario":"idle_wake","params":{"drain_secs":1.0,"drop_plants_at":null,"name":"idle_wake","orders":false,"rate":10,"raw":1,"sample_rss":false,"secs":6.0,"sessions":0,"slow_rpc_secs":0.0,"stalled":0,"warmup_secs":0.0},"duration_s":6.102959792,"sent":60,"conns":[{"kind":"raw","received":60,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":60,"p50_us":96,"p99_us":400,"p999_us":400,"max_us":400}}],"min_delivered_ratio":1.0,"min_throughput_eps":10.0,"latency":{"count":60,"p50_us":96,"p99_us":400,"p999_us":400,"max_us":400},"order_latency":null,"order_idle_latency":null,"peak_rss_kb":13472,"rss_samples_kb":[],"unsignalled_loss":0,"allocs_per_event":null,"alloc_bytes_per_event":null,"notes":[]}
{"scenario":"flood_1k_1conn","params":{"drain_secs":3.0,"drop_plants_at":null,"name":"flood_1k_1conn","orders":false,"rate":1000,"raw":1,"sample_rss":false,"secs":5.0,"sessions":0,"slow_rpc_secs":0.0,"stalled":0,"warmup_secs":0.0},"duration_s":5.102549375,"sent":5000,"conns":[{"kind":"raw","received":5000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":5000,"p50_us":51,"p99_us":168,"p999_us":1214,"max_us":38783}}],"min_delivered_ratio":1.0,"min_throughput_eps":1000.0,"latency":{"count":5000,"p50_us":51,"p99_us":168,"p999_us":1214,"max_us":38783},"order_latency":null,"order_idle_latency":null,"peak_rss_kb":13808,"rss_samples_kb":[],"unsignalled_loss":0,"allocs_per_event":null,"alloc_bytes_per_event":null,"notes":[]}
{"scenario":"flood_10k_4conn_1session","params":{"drain_secs":3.0,"drop_plants_at":null,"name":"flood_10k_4conn_1session","orders":false,"rate":10000,"raw":4,"sample_rss":false,"secs":5.0,"sessions":1,"slow_rpc_secs":0.0,"stalled":0,"warmup_secs":0.0},"duration_s":5.103405708,"sent":50000,"conns":[{"kind":"raw","received":50000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":50000,"p50_us":107,"p99_us":1275,"p999_us":12159,"max_us":16751}},{"kind":"raw","received":50000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":50000,"p50_us":106,"p99_us":1940,"p999_us":10431,"max_us":11023}},{"kind":"raw","received":50000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":50000,"p50_us":107,"p99_us":1059,"p999_us":10119,"max_us":10679}},{"kind":"raw","received":50000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":50000,"p50_us":107,"p99_us":1121,"p999_us":10087,"max_us":10479}},{"kind":"session","received":50000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":50000,"p50_us":135,"p99_us":1361,"p999_us":10255,"max_us":11127}}],"min_delivered_ratio":1.0,"min_throughput_eps":10000.0,"latency":{"count":250000,"p50_us":113,"p99_us":1289,"p999_us":10303,"max_us":16751},"order_latency":null,"order_idle_latency":null,"peak_rss_kb":15264,"rss_samples_kb":[],"unsignalled_loss":0,"allocs_per_event":null,"alloc_bytes_per_event":null,"notes":[]}
{"scenario":"flood_50k_4conn","params":{"drain_secs":10.0,"drop_plants_at":null,"name":"flood_50k_4conn","orders":false,"rate":50000,"raw":4,"sample_rss":false,"secs":8.0,"sessions":0,"slow_rpc_secs":0.0,"stalled":0,"warmup_secs":0.0},"duration_s":8.103148916,"sent":400000,"conns":[{"kind":"raw","received":400000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":400000,"p50_us":421,"p99_us":14911,"p999_us":23583,"max_us":26847}},{"kind":"raw","received":400000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":400000,"p50_us":416,"p99_us":16751,"p999_us":27583,"max_us":34239}},{"kind":"raw","received":400000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":400000,"p50_us":420,"p99_us":12503,"p999_us":23343,"max_us":24015}},{"kind":"raw","received":400000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":400000,"p50_us":417,"p99_us":15503,"p999_us":28303,"max_us":33791}}],"min_delivered_ratio":1.0,"min_throughput_eps":50000.0,"latency":{"count":1600000,"p50_us":419,"p99_us":14695,"p999_us":26495,"max_us":34239},"order_latency":null,"order_idle_latency":null,"peak_rss_kb":16400,"rss_samples_kb":[],"unsignalled_loss":0,"allocs_per_event":null,"alloc_bytes_per_event":null,"notes":[]}
{"scenario":"burst_200k_1s","params":{"drain_secs":15.0,"drop_plants_at":null,"name":"burst_200k_1s","orders":false,"rate":200000,"raw":4,"sample_rss":false,"secs":1.0,"sessions":0,"slow_rpc_secs":0.0,"stalled":0,"warmup_secs":0.0},"duration_s":1.741956709,"sent":200000,"conns":[{"kind":"raw","received":200000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":200000,"p50_us":328447,"p99_us":643071,"p999_us":644095,"max_us":644607}},{"kind":"raw","received":200000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":200000,"p50_us":326143,"p99_us":629247,"p999_us":633343,"max_us":633343}},{"kind":"raw","received":200000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":200000,"p50_us":327679,"p99_us":636927,"p999_us":638975,"max_us":639487}},{"kind":"raw","received":200000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":200000,"p50_us":349695,"p99_us":644095,"p999_us":646143,"max_us":646655}}],"min_delivered_ratio":1.0,"min_throughput_eps":200000.0,"latency":{"count":800000,"p50_us":332543,"p99_us":640511,"p999_us":645631,"max_us":646655},"order_latency":null,"order_idle_latency":null,"peak_rss_kb":33952,"rss_samples_kb":[],"unsignalled_loss":0,"allocs_per_event":null,"alloc_bytes_per_event":null,"notes":[]}
{"scenario":"latency_20k","params":{"drain_secs":10.0,"drop_plants_at":null,"name":"latency_20k","orders":false,"rate":20000,"raw":1,"sample_rss":false,"secs":8.0,"sessions":1,"slow_rpc_secs":0.0,"stalled":0,"warmup_secs":0.0},"duration_s":8.102384042,"sent":160000,"conns":[{"kind":"raw","received":160000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":160000,"p50_us":85,"p99_us":495,"p999_us":2247,"max_us":4455}},{"kind":"session","received":160000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":160000,"p50_us":126,"p99_us":659,"p999_us":3707,"max_us":6411}}],"min_delivered_ratio":1.0,"min_throughput_eps":20000.0,"latency":{"count":320000,"p50_us":100,"p99_us":590,"p999_us":3197,"max_us":6411},"order_latency":null,"order_idle_latency":null,"peak_rss_kb":33952,"rss_samples_kb":[],"unsignalled_loss":0,"allocs_per_event":null,"alloc_bytes_per_event":null,"notes":[]}
{"scenario":"orders_flood_slow_rpc","params":{"drain_secs":10.0,"drop_plants_at":null,"name":"orders_flood_slow_rpc","orders":true,"rate":50000,"raw":2,"sample_rss":false,"secs":12.0,"sessions":1,"slow_rpc_secs":8.0,"stalled":0,"warmup_secs":2.0},"duration_s":14.105752417,"sent":600000,"conns":[{"kind":"raw","received":600000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":600000,"p50_us":228,"p99_us":1374,"p999_us":4831,"max_us":8983}},{"kind":"raw","received":600000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":600000,"p50_us":227,"p99_us":1242,"p999_us":3793,"max_us":5915}},{"kind":"session","received":600000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":600000,"p50_us":337,"p99_us":1476,"p999_us":5423,"max_us":8775}}],"min_delivered_ratio":1.0,"min_throughput_eps":50000.0,"latency":{"count":1800000,"p50_us":263,"p99_us":1356,"p999_us":4911,"max_us":8983},"order_latency":{"count":1138,"p50_us":108,"p99_us":621,"p999_us":3029,"max_us":9439},"order_idle_latency":{"count":186,"p50_us":18,"p99_us":144,"p999_us":167,"max_us":167},"peak_rss_kb":33952,"rss_samples_kb":[],"unsignalled_loss":0,"allocs_per_event":null,"alloc_bytes_per_event":null,"notes":["slow RPC load_fill_history finished in 8.00s: ok (0 rows)"]}
{"scenario":"stalled_consumer","params":{"drain_secs":8.0,"drop_plants_at":null,"name":"stalled_consumer","orders":false,"rate":20000,"raw":3,"sample_rss":false,"secs":8.0,"sessions":0,"slow_rpc_secs":0.0,"stalled":1,"warmup_secs":0.0},"duration_s":8.102064584,"sent":160000,"conns":[{"kind":"raw","received":160000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":160000,"p50_us":75,"p99_us":420,"p999_us":3895,"max_us":10207}},{"kind":"raw","received":160000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":160000,"p50_us":75,"p99_us":371,"p999_us":2233,"max_us":3349}},{"kind":"raw","received":160000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":160000,"p50_us":75,"p99_us":383,"p999_us":2907,"max_us":4571}}],"min_delivered_ratio":1.0,"min_throughput_eps":20000.0,"latency":{"count":480000,"p50_us":75,"p99_us":391,"p999_us":2947,"max_us":10207},"order_latency":null,"order_idle_latency":null,"peak_rss_kb":33952,"rss_samples_kb":[],"unsignalled_loss":0,"allocs_per_event":null,"alloc_bytes_per_event":null,"notes":["1/1 stalled connections were dropped by the parent"]}
{"scenario":"reconnect_during_flood","params":{"drain_secs":8.0,"drop_plants_at":3.0,"name":"reconnect_during_flood","orders":false,"rate":10000,"raw":2,"sample_rss":false,"secs":8.0,"sessions":0,"slow_rpc_secs":0.0,"stalled":0,"warmup_secs":0.0},"duration_s":8.10271875,"sent":80000,"conns":[{"kind":"raw","received":80000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":1,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":80000,"p50_us":74,"p99_us":431,"p999_us":2873,"max_us":4391}},{"kind":"raw","received":80000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":1,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":80000,"p50_us":74,"p99_us":416,"p999_us":1646,"max_us":4171}}],"min_delivered_ratio":1.0,"min_throughput_eps":10000.0,"latency":{"count":160000,"p50_us":74,"p99_us":423,"p999_us":2563,"max_us":4391},"order_latency":null,"order_idle_latency":null,"peak_rss_kb":33952,"rss_samples_kb":[],"unsignalled_loss":0,"allocs_per_event":null,"alloc_bytes_per_event":null,"notes":["venue calls after the drop: 1 connect, 1 disconnect, 1 subscribe, 0 unsubscribe"]}
{"scenario":"soak","params":{"drain_secs":10.0,"drop_plants_at":null,"name":"soak","orders":false,"rate":10000,"raw":1,"sample_rss":true,"secs":45.0,"sessions":0,"slow_rpc_secs":0.0,"stalled":0,"warmup_secs":0.0},"duration_s":45.345684542,"sent":450000,"conns":[{"kind":"raw","received":450000,"missing_in_stream":0,"reordered":0,"gap_events":0,"gap_skipped":0,"resumed":0,"reattaches":0,"max_outage_ms":0,"dropped_by_parent":false,"latency":{"count":450000,"p50_us":25,"p99_us":611,"p999_us":5899,"max_us":10991}}],"min_delivered_ratio":1.0,"min_throughput_eps":10000.0,"latency":{"count":450000,"p50_us":25,"p99_us":611,"p999_us":5899,"max_us":10991},"order_latency":null,"order_idle_latency":null,"peak_rss_kb":33952,"rss_samples_kb":[14768,13536,9680,9760,9696,10064,10464,10928,10800,10528,11168,11792,12464,12560,12896,12816,12848,12848,12848,12400,11856,11856,11520,11168,10464,10784,11152,10400,10544,10976,11616,11808,11792,12192,12448,12448,12144,11440,11312,9248,10096,10720,11344,11968,12432],"unsignalled_loss":0,"allocs_per_event":null,"alloc_bytes_per_event":null,"notes":[]}
{"scenario":"rpc_isolation_same_connection","params":{"slow_secs":3.0},"duration_s":3.002891084,"sent":0,"conns":[],"min_delivered_ratio":0.0,"min_throughput_eps":0.0,"latency":{"count":0,"p50_us":0,"p99_us":0,"p999_us":0,"max_us":0},"order_latency":{"count":1,"p50_us":2065,"p99_us":2065,"p999_us":2065,"max_us":2065},"order_idle_latency":null,"peak_rss_kb":33952,"rss_samples_kb":[],"unsignalled_loss":0,"allocs_per_event":null,"alloc_bytes_per_event":null,"notes":["place_order behind a 3s read RPC on one connection replied in 2.1 ms (ack: true)","arrival order [11, 10] (10 = slow read RPC, 11 = place); slow reply at 3.00s: Some(\"LoadFillHistoryResponse(LoadFi\")"]}
{"scenario":"alloc_publish_1sub","params":{"events":100000,"receivers":1},"duration_s":0.0,"sent":100000,"conns":[],"min_delivered_ratio":0.0,"min_throughput_eps":0.0,"latency":{"count":0,"p50_us":0,"p99_us":0,"p999_us":0,"max_us":0},"order_latency":null,"order_idle_latency":null,"peak_rss_kb":67152,"rss_samples_kb":[],"unsignalled_loss":0,"allocs_per_event":1.0,"alloc_bytes_per_event":51.83488,"notes":[]}
{"scenario":"alloc_publish_4sub","params":{"events":100000,"receivers":4},"duration_s":0.0,"sent":100000,"conns":[],"min_delivered_ratio":0.0,"min_throughput_eps":0.0,"latency":{"count":0,"p50_us":0,"p99_us":0,"p999_us":0,"max_us":0},"order_latency":null,"order_idle_latency":null,"peak_rss_kb":67152,"rss_samples_kb":[],"unsignalled_loss":0,"allocs_per_event":1.0,"alloc_bytes_per_event":51.83488,"notes":[]}
```

## Group 2 interleaved A/B (2026-10-15)

Track A reported loaded-order p99 0.94–1.02 ms against this baseline's 0.621 ms. That baseline was taken under different machine load, so both release binaries were built and run alternately: `orders_flood_slow_rpc` and `stalled_consumer`, 3 rounds each, base = `878443b`, A = `3207699`, load average 4.6–10.5. Medians of p99:

| Scenario / metric | base | group 2 | ratio |
|---|---:|---:|---:|
| orders_flood_slow_rpc `order_latency.p99_us` | 924 | 931 | 1.01 |
| orders_flood_slow_rpc `order_idle_latency.p99_us` | 212 | 199 | 0.94 |
| orders_flood_slow_rpc `latency.p99_us` | 1284 | 1190 | 0.93 |
| stalled_consumer `latency.p99_us` | 1257 | 518 | 0.41 |

No regression from group 2. The earlier gap was machine noise between the two runs. Comparisons should always be interleaved runs.

## Group 4 quieter-machine re-check (2026-10-15)

A2's run reported `stalled_consumer` p99 at 3.71× base, with a wide spread (476–8,167 µs) because other agents were building at the time. It was re-run with no agents active (load average 3.2–5.0), 5 interleaved rounds, base = `c2e0260`, candidate = `f118c15`:

| Scenario | base p99 (µs) | group 4 p99 (µs) | ratio |
|---|---|---|---:|
| stalled_consumer | 427, 418, 574, 453, 487 (med 453) | 387, 402, 426, 434, 441 (med 426) | 0.94 |
| flood_10k_4conn_1session | 352, 415, 382, 373, 337 (med 373) | 353, 365, 363, 369, 344 (med 363) | 0.97 |

No regression from group 4.
