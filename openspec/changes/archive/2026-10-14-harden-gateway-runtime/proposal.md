## Why

An architecture review of the unified Rust session (2026-10-14) found the layering sound but the runtime not production-grade under load. The gateway event pump moves one event per stream per 10 ms (about 100 events/s) and swallows overflow, so a busy NQ/ES feed backs up and then loses ticks with no gap signal. One slow RPC (a 300 s history load) blocks `place`/`cancel` on the same shared socket. The session gate and the `DirectSession` mutex are held across venue I/O. Events are cloned into queues nobody reads. None of this is measured: the repository has no throughput, latency or allocation benchmark, so "production ready" is currently a claim, not a number.

## What Changes

- Add a **synthetic load harness** (fake plant, no Rithmic credentials) that pushes tick, order and history traffic through the real server, client and PyO3 layers and reports throughput, p50/p99/p999 latency, loss, gap signals, allocations per event and RSS. Run it first on the current code to record a baseline.
- Make event delivery **event-driven**: no fixed-interval polling or `sleep` in the pump, the direct polling path or the Python pollers. Events are awaited on owned receivers outside any session lock.
- **No silent loss**: a lagged or dropped stream emits an explicit gap signal that reaches every affected client in both modes (**BREAKING** proto addition, wired through plants, gateway server, client, PyO3 and Python in the same change).
- **Isolation**: per-connection reader/writer tasks; slow RPCs (history) run off the command lane with bounded concurrency, so `place`/`cancel`/subscribe never queue behind a history load.
- **Lock and copy diet**: no session gate or mutex across venue I/O; connect and history use cloned handles and deadlines; events are shared by `Arc`/`Bytes` instead of cloned per queue; encode only when a subscriber exists; a request frame is decoded without being copied; batch polling across the FFI boundary.
- **Error taxonomy**: `Rejected`, `NotSent`, `Unknown` classes at the source, so a venue reject is not reported as unknown and a pre-send failure is a local deny.
- **Trading-gate parity in Rust**: `DirectSession` enforces `enable_trading` itself instead of relying on the Python client.
- **Moved here from `retire-python-mux-and-noop-knobs`**: unread facade queues, per-tick timestamp FFI, transport-callback unsubscribe, double gate lock in the pump (its tasks 3.1 to 3.4).

## Capabilities

### New Capabilities
- `gateway-throughput`: measurable delivery guarantees: sustained and burst throughput budgets, latency budgets, no silent loss, channel and connection isolation, bounded memory and allocations, and the load harness that enforces them.

### Modified Capabilities
- `session-backend`: event streams are awaitable and owned by the consumer; no lock is held across venue I/O; trading-gate parity is enforced in `DirectSession`; outcome classes gain `Rejected`/`NotSent`/`Unknown`; a lag is a typed gap event in both modes.
- `gateway-client`: per-facade queues receive only armed channels and share events by reference; the client surfaces a gap signal; slow RPCs do not block other RPCs on the socket.

## Impact

- Rust: `rithmic-plants` (`backend.rs`, `direct.rs`, `session.rs`, `fake.rs`, `error.rs`), `rithmic-gateway` (`server/pump.rs`, `server/mod.rs`, `server/dispatch/*`, `client/runtime.rs`, `reconnect.rs`, `subscriptions.rs`, `session.proto`), `rithmic-nt-connect` (`python/mod.rs`: batch poll, callback handle, GIL), new load harness crate or bench target.
- Python: `data.py`, `execution.py`, `polling.py`, `session.py`, `_convert.py`; `rithmic_gateway` lake client must tolerate the new gap frame (additive).
- Protocol: additive `session.proto` frames (gap event); old clients ignore unknown frames, so the lake client is not broken.
- CI: a fast load smoke with loose floors; the full harness is run manually and its report committed as the baseline.
- Docs: STATUS, ops-runbook (how to run the harness, how to read the report), AGENTS.md (gateway RPC kinds table gains the lane rule).
- Not verified live: tick delivery at real market rates still needs a market-data account (open task 5.3 of `rust-unified-session`); the harness bounds the software, not the venue.
