## Context

After `simplify-gateway-modes` (attach-only, credential-free clients) and `rust-unified-session` (one Rust `Session` trait, `DirectSession` and `GatewaySession`), the adapter reaches the gateway only through the Rust client. What remains in `python/rithmic_gateway`:

- `mux.py` / `runtime.py` / `transport_recovery.py` (~950 lines): the Python single-socket multiplexer. Its only production caller is the `max_workers > 1` peer path of `GatewayClient.load_time_bars_range` (default 1, documented "for experiments").
- Two copies of the retired-knob table (`rithmic_gateway/config.py`, `rithmic_nt_connect/config.py`) that still accept `mux`, `never`, `0` as no-ops with a one-time warning.

The Rust client also carries four known inefficiencies from review that were skipped as out of scope for the earlier changes.

## Goals / Non-Goals

**Goals:**
- Remove the Python mux stack and the no-op knob acceptance without changing the wire protocol or the lake-facing `GatewayClient`.
- Fix the four skipped behavior issues in the Rust client/gateway with tests that fail if they regress.
- Leave docs and specs describing only what exists.

**Non-Goals:**
- No change to `session.proto`, the Rust `Session` trait, or direct-mode behavior.
- No new gateway capability, so the direct/gateway plant-surface parity rule is untouched.
- No removal of `GatewayClient` or of `framed_conn.py`.

## Decisions

**1. Knobs raise, with no deprecation window.** The only consumer is the market-data lake and the grace period was one release. A set-but-ignored variable is the failure mode the "fail fast" requirement exists to prevent. Alternative: keep the warning another release; rejected because it keeps two tables, three test blocks and spec scenarios alive for no consumer.

**2. Delete the mux rather than shrink it.** `load_time_bars_range(max_workers>1)` is the only live use. Make it sequential-only and drop `max_workers` from the signature, so passing it is a `TypeError` instead of silent single-threading. Alternative: dial a plain `GatewayClient` per worker; rejected, the parent serializes history on its session mutex anyway, so parallel peers add sockets without throughput (see the chunked-history requirement).

**3. Unread facade queues: arm on first use.** `Runtime::deliver` clones each market/history/pnl/order event into every facade's queue. Add an `armed` flag per `EventQueue`, set by the first subscribe or poll of that channel; `deliver` skips unarmed queues. Alternative: store `Arc<PlantEvent>` so the fan-out is a refcount bump. Do both only if profiling still shows clone cost; arming removes the dead work and the 4096-cap churn (`lagged` bumps) first.

**4. Timestamp codec: compute where the dict is built.** `event_ts_ns` crosses the PyO3 boundary once per tick for two lines of arithmetic. Compute `ts_event_ns` in the Rust event-to-dict builder so Python reads a field. The id codec stays a single call per instrument (already `lru_cache`d); add the same cache to `symbol_and_exchange_from_instrument_id`. `tests/test_exec_rules_parity.py` stays the guard.

**5. Transport callback: return a cancel handle.** `subscribe_transport_events` spawns a task owning the Python callback and receivers and returns `()`. The task holds the bound method, which holds the `PySession` and the `Runtime` that owns the senders, so `Closed` never arrives. Return a `#[pyclass] TransportSubscription` with `cancel()` (abort handle); the Python wrapper cancels when the last listener leaves and on close.

**6. Event pump: hold the gate once.** `run_event_pump` takes the `SessionHandle` gate, and each `DirectSession::poll_with` then takes `inner` again (4 polls per iteration), and publishing runs under the gate. Poll the four streams through one session method that locks once, and release the gate before `publish_plant_event`. Reconnect and restore must still exclude the pump; keep that covered by the `FakeSession` restore tests.

## Risks / Trade-offs

- [Lake still calls `create_gateway_client` or passes `max_workers`] → confirm with the lake owner (extends task 6.4 of `simplify-gateway-modes`) before merging the deletion; the removal is one self-contained commit and reverts cleanly.
- [Arming skips an event that arrives between subscribe return and first poll] → arm on the subscribe call, before the RPC is sent, and test subscribe followed by an immediate event.
- [Releasing the gate before publish reorders events relative to reconnect] → publish only already-polled events; the restore path re-issues intents rather than replaying events, and a reconnect test asserts no loss or duplication.
- [Cancel handle forgotten by callers] → the Python wrapper owns it; the session `close` cancels any remaining subscription.
- [Live behavior not verified] → task 5.3 of `rust-unified-session` (tick delivery, reconnect smoke) needs a market-data account; this change inherits that gap and does not claim live verification.
