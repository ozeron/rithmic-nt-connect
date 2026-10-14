## Why

An end-to-end audit on 2026-10-14 found about 30 real bugs and several duplication hotspots. The worst bugs:
- a history load blocks `place`/`cancel` on a shared gateway socket for minutes;
- a cancel can wait behind a 60 s ticker reconnect;
- gateway-mode account lookups block the asyncio loop;
- recon reports positions FLAT before PnL is live;
- every book update clears the book.

Everything below the `Session` trait (`RithmicSession` / `DirectSession`) is tested only live, which is why several of these bugs were never caught.

Installing is also harder than it should be. Consumers either build the extension with Rust and maturin, or depend on a local checkout. market-data-lake does the latter and is already broken against `main`: it still passes the removed spawn options. NautilusTrader shows the model to follow: prebuilt platform wheels published by CI on every merge.

## What Changes

- **Test seams.**
  - A fake Rithmic WebSocket server, so the unmodified `RithmicSession` / `DirectSession` and the `rithmic-rs` decode get offline integration tests.
  - `FakeSession` brought to parity with `DirectSession` (trading gate, disconnect gate, plant attach).
  - One scenario suite run against both sessions.
  - Controllable time in the gateway client and pump.
- **Plants and exec fixes.**
  - Order handle and ticker reset no longer hold the session lock across venue I/O.
  - A failed order-updates subscribe tears down only a plant it opened.
  - Partial history replies are errors.
  - Tick dedup runs only at slice boundaries.
  - Stream failures are delivered after queued events.
  - Order loads are serialised.
  - A forwarder panic closes its lane.
  - Bracket updates are decoded.
  - Trade and BBO `ts_event` come from exchange time.
  - Disconnect gate is a counter; mid-session history gets the readiness probe; explicit order-plant disconnect is honoured.
- **Gateway fixes.**
  - The history/live-market-data lock is held only for the check. A subscribe arriving mid-load goes ahead; history stays serialised on its own lock.
  - Bar overflow gaps reach the bar channel.
  - Per-facade intent refcount in the Rust client; the runtime key includes `account_id`.
  - Idle-exit lost wakeup fixed.
  - Undecodable request → `bad_request` reply (NotSent).
  - A panic in the command lane can no longer leak intents.
  - Command errors default to unknown outcome.
  - Client retry amplification removed.
  - Reads that take the session (`GetReferenceData`, `GetFrontMonth`, `ResolvedAccount`) move to the slow lane.
  - Order peers get a gap after an order-plant re-seat.
  - Pre-handshake limits.
  - Singleton lock file is not truncated before the lock is held.
  - PyO3 transport callbacks run off tokio workers.
- **Python fixes.**
  - No blocking session RPC on the loop.
  - Position recon is gated on live PnL.
  - Book `update_type` handled as snapshot / incremental / clear.
  - History requests always complete.
  - Bracket legs get events and incomplete brackets are denied.
  - Fill identity is clock-free.
  - PnL barriers reset on resync.
  - A bad notification no longer stops the order stream.
  - `_connect` cleans up on failure.
  - Bar unsubscribe includes the period.
  - PnL is restored with trading off.
  - Recovery-bridge callbacks hop onto the loop.
  - Pure-Python `GatewayClient` framing and outcome fixes.
- **Bar timestamps.** New `bars_timestamp_on_close: bool = True` on the data client config, matching Nautilus `time_bars_timestamp_on_close` and the Databento/Bybit/dYdX adapters. **BREAKING**: live and requested EXTERNAL bars are now stamped at close by default; the old open-time stamping needs `False`.
- **Duplication removed.**
  - Mirrored proto conversions generated from one field list.
  - One-shot command helper; RPC lanes derived from the RPC table.
  - One order/PnL leave path.
  - Generic refcount helpers.
  - A typed client call macro.
  - One plant bring-up function and shared order-term parsing.
  - Unified notification/bar DTO parsing.
  - `product_root` and `ts_ns` used from `rithmic-exec` only.
  - Python deny helpers and poll-task factory.
  - The `notification_action` rule moved to `rithmic-exec` (Python-object coercion stays in Python, per AGENTS.md).
  - The two RMS row-type copies (adapter and lake package cannot import each other) pinned to one shape by a test.
- **Distribution like Nautilus.**
  - `rithmic-gateway` becomes a maturin `bindings = "bin"` wheel carrying the client and the `rithmic-gateway` binary.
  - `rithmic-nt-connect` depends on it instead of bundling the binary.
  - CI builds wheels on every push to `main` and on tags, for Linux x86_64/ARM64 and macOS ARM64. They are published to a rolling GitHub prerelease and a PEP 503 index on GitHub Pages, with provenance attestations. No PyPI, no Windows.
  - Binary lookup is reduced to override → wheel → `PATH`. **BREAKING** for dev checkouts that relied on the cargo `target/` scan; they set `RITHMIC_GATEWAY_BIN`.
- **market-data-lake** (separate repo, separate PR): installs from the index, drops the removed spawn options, and dials a parent started explicitly.

## Capabilities

### New Capabilities
- `package-distribution`: how the client, parent binary and adapter are built, published and installed (platform wheels, index, release channel, binary lookup, install-without-Rust smoke).

### Modified Capabilities
- `session-backend`: no session lock across order-handle or ticker-reset I/O; scoped rollback of an order subscribe; partial history is an error; boundary-only dedup; stream failure ordering; serialised order loads; forwarder panic closes the lane; exchange-time `ts_event`; typed bracket updates; fake Rithmic venue and contract suite.
- `gateway-throughput`: history holds the live-MD lock only for its check; bar overflow gaps reach the bar channel; session-taking reads on the slow lane; order peers get a gap after a re-seat.
- `gateway-client`: per-facade intent refcount; `account_id` in the runtime key; `bad_request` maps to NotSent; command errors default to unknown; no compounded history retries; pure-Python client framing fix.
- `gateway-parent-lifecycle`: idle-exit wakeup; command-lane panic does not leak intents; pre-handshake limits; singleton lock file kept intact by contenders; binary lookup order.
- `adapter-wire-facade`: no blocking RPC on the loop; position recon gated on live PnL; book `update_type`; history requests always complete; bracket events and denial; clock-free fill identity; bar timestamp option; stream survives a bad notification.
- `exec-core`: the `notification_action` rule moves into `rithmic-exec`.

## Impact

- **Rust**:
  - `rithmic-plants` (`direct.rs`, `session.rs`, `streams.rs`, `history.rs`, `dto.rs`, `fake.rs`, new `fake_venue.rs`);
  - `rithmic-gateway` (`server/*`, `client/*`, `convert.rs`, `rpc_table.rs`, `reconnect.rs`, `idle_exit.rs`, `singleton.rs`, `session.proto`);
  - `rithmic-exec` (moved rules, `ts_ns`);
  - `rithmic-nt-connect` (bracket update dict, callback thread, new bindings).
- **Python**: `execution.py`, `data.py`, `recon.py`, `notification_intake.py`, `recovery_bridge.py`, `_orders.py`, `_convert.py`, `session.py`, `config.py`, `wire_types.py`, `_lib.pyi`; `rithmic_gateway/client.py`, `framed_conn.py`, `binary.py`, `__main__.py`, `pyproject.toml`.
- **Protocol**: additive `session.proto` fields (bracket update event); `bad_request` error code.
- **Packaging/CI**: new `.github/workflows/wheels.yml`, GitHub Pages index, rolling `main` prerelease; `scripts/build_wheel.sh` and the `wheel-data` binary bundling retired.
- **Docs**: STATUS, ops-runbook, gateway-remote, README install section, ADR 0003 (bar timestamps).
- **Consumers**: trading nodes that relied on open-time bars set `bars_timestamp_on_close=False`. market-data-lake changes in its own PR.
- **Not verified offline**: which book `update_type` values Rithmic sends live (an e2e test records them); place latency under real market rates.
