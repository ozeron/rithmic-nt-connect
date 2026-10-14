## Context

The 2026-10-14 audit covered the plants, gateway, Python adapter and test layout. The Rust tests are good at and above the `Session` trait: 311 pass, and the gateway runs its real server code against `FakeSession` over a unix socket. Below that trait nothing is injectable. `RithmicSession` holds concrete `rithmic_rs` plants and calls `RithmicOrderPlant::connect` directly. Several confirmed bugs (lock scope, subscribe rollback, partial history) live exactly there.

harden-gateway-runtime (archived 2026-10-14) removed the session gate from history loads but left `md_history_gate` held for the whole load. Its own design said to keep that lock short.

Distribution today:
- `scripts/build_wheel.sh` builds the adapter wheel locally and copies the gateway binary into `wheel-data/`.
- market-data-lake installs `python/` from a local path.
- Lake still passes the retired spawn options, so it breaks on current `main`.

## Goals / Non-Goals

**Goals:**
- Fix every confirmed bug from the audit, each with a regression test that fails first.
- Make direct mode testable offline, by running the real session against a fake Rithmic WebSocket server, with one contract suite.
- Remove the listed duplication without changing protocol semantics.
- Ship like NautilusTrader: CI-built platform wheels with native code inside, installable without Rust.

**Non-Goals:**
- Remote gateway transport, Windows support, or PyPI publishing.
- Changing the "no history while live market data" refusal when a load starts (only the lock scope changes).
- Rewriting the pure-Python lake client on top of Rust.
- Live place testing.

## Decisions

**1. Fake Rithmic WebSocket server instead of a connector trait.**
- What: a test-only R|Protocol server (`rithmic-plants/src/fake_venue.rs`, feature `testing`) listens on `ws://127.0.0.1:<port>`. The real `RithmicSession` / `DirectSession` and `rithmic-rs` connect to it unchanged. It speaks prost frames (4-byte length + message, `template_id` routing) for login/logout, heartbeat, account list, ticker/book subscribe, front month, reference data, history replays (`rq_handler_rp_code` / `has_more`), order place/cancel/modify/brackets, show orders and order-history, fill history, RMS info and PnL.
- Scripting: per-template replies or rejects (`rp_code`), delays, connect/login failures, connection drops, and pushed notifications. It records every request it received.
- Why: `rithmic_rs::RithmicResponse` and `RithmicError` are `#[non_exhaustive]` and their decoders are crate-private, so no code outside `rithmic-rs` can build a reply. A trait seam would have to return our own types, leaving the venue decode untested. The fake server tests the full direct path, including decode, and lets the `PlantEvent` golden tests come from real frames.
- Alternatives considered:
  - Own reply types behind traits: a large production refactor, and the decode stays untested.
  - Patching or forking `rithmic-rs` for test constructors: depends on upstream or on carrying a fork.

**2. Order handle and resolved account cached outside the session mutex.**
- What:
  - `DirectSession` keeps the order handle in a `std::sync::Mutex<Option<Handle>>`, set after ensure and cleared on disconnect. `order_handle()` takes the fast path; only bring-up locks `inner`.
  - `reset_ticker` takes the old ticker out under the lock, does the I/O unlocked, and puts the new one back.
  - The resolved account is cached the same way.
  - An `order_disconnected` flag, cleared by ensure or subscribe, gives the "stays down" rule.
- Alternative considered: split `RithmicSession` into per-plant mutexes. Cleaner long-term, but a much larger diff for the same effect.

**3. History lock: check, then release; history serialised on its own lock.**
- What: `md_history_gate` covers only `has_live_md_intents()` (and subscribe's note-and-join). A new `history_load_lock` keeps loads one at a time against the venue, as today.
- Why: the user chose "subscribe goes ahead". The starvation that motivated the gate (MY043) came from the session mutex, which is gone.
- Alternatives considered:
  - Refuse subscribes mid-load: delays live data by up to 300 s.
  - Wait off-lane: breaks per-socket ordering.

**4. Bar gap without a proto change.**
- What: the forwarder knows the client's `kinds` mask. On overflow it sends `Gap(Ticker)`, plus `Gap(History)` if bars are armed.
- Why: no new field. The Python history poller already resyncs on a history gap.

**5. Per-facade refcount in the client runtime.**
- What: `Runtime.intents: HashMap<IntentKey, HashMap<FacadeId, usize>>`, mirroring the server's `TopicIntent` keys. Subscribe and unsubscribe go on the wire only on process-wide transitions; facade drop releases its holdings.
- Why here: the server stays per-socket and idempotent, so the fix belongs where the sharing happens. `account_id` joins the runtime key so different accounts never share a handshake.

**6. Panics and decode errors keep the connection alive.**
- What:
  - Each command-lane request runs under `catch_unwind` and replies `internal` on panic.
  - An undecodable frame gets a best-effort `request_id` (field 1 varint) and a `bad_request` reply.
- Why: a dropped connection would fail every in-flight call as unknown, while a typed reply is precise.

**7. Error default for commands is unknown.**
- What: in `plant_err_frame`, any error other than `Rejected` or `NotSent` maps to `venue_unknown` for one-shot commands. Reads keep `*_failed`.
- Why: this follows the convention "after send, uncertain is unknown".

**8. Book update types.**
- What: Rust maps `update_type` to a closed enum (`ClearOrderBook`=1, `NoBook`=2, `SnapshotImage`=3, `Begin`=4, `Middle`=5, `End`=6, `Solo`=7, per `rithmic-rs` 3.0 `rti.rs`) and rejects others. Python converts snapshot images to CLEAR+ADD and everything else to incremental UPDATE/DELETE batches, per the spec.
- Risk: live traffic may only ever send snapshots. An e2e test records the histogram, and STATUS states what was observed.

**9. Bars at close by default.**
- What: `bars_timestamp_on_close: bool = True`, the same name and default as the Databento, Bybit and dYdX configs. `closed_bars` and `BarSequenceGuard` are keyed on the configured stamp. The seam-parity and hygiene tests run both ways.
- Why: Rithmic's marker is the close, a bar is only knowable at close (no look-ahead), and close is the Nautilus default. Recorded in ADR 0003.

**10. Duplication via macros and tables, not new abstractions.**
- What:
  - `mirror_conv!` lists each DTO↔proto field set once and generates both directions.
  - The lane comes from the RPC table rows.
  - `one_shot` helper for gated commands; `call_expect!` on the client.
  - Generic `bump<K>`; one `connect_plant`; shared order-term parsing.
- Why: each removes a place where two copies could drift. The existing parity tests (`rpc_table.rs`, `lanes.rs`, the wire parity tests) guard the result.

**11. Distribution like NautilusTrader.**
- What:
  - `python/pyproject.toml` switches to maturin `bindings = "bin"` (crate `rithmic-gateway`) with `python-source` for `rithmic_gateway`, the way ruff and uv ship binaries.
  - `rithmic-nt-connect` depends on `rithmic-gateway`, so `wheel-data` bundling and `build_wheel.sh` are retired.
  - `wheels.yml` uses `PyO3/maturin-action` (manylinux_2_35 x86_64/aarch64, macOS arm64) with protoc installed, on push to `main` and on tags.
  - A no-Rust smoke runs before publishing. Assets go to a rolling `main` prerelease. A small script regenerates a PEP 503 index (sha256 fragments, last 30 dev builds) on GitHub Pages. `actions/attest-build-provenance` attests every wheel.
- Why: no account or token to manage, nothing public beyond the already-public repo. uv supports the index natively.
- Alternatives considered:
  - Separate binary downloads: the user would have to find the binary.
  - `cargo install --git`: needs Rust everywhere.
  - PyPI: declined.

**12. Lake follows in its own PR.**
- What: index source, no spawn options, and an explicit parent: a service, or a small launcher around the wheel's `rithmic-gateway` modelled on `scripts/_gateway_parent.py`. The password lives only in the parent's environment.

## Risks / Trade-offs

- [The fake venue drifts from real Rithmic behaviour] → replies are built from the same `rti` prost types `rithmic-rs` decodes, its scripts mirror captured live sequences where available, and live e2e still runs before release.
- [Order-handle cache goes stale after a plant drop] → cleared on every disconnect path and on `ChannelClosed`; the stale-handle case is covered by the contract suite.
- [Subscribe during history adds load on the venue] → loads still refuse to start while live data exists, and loads stay serialised.
- [Close-time default changes live bar timestamps for existing nodes] → **BREAKING** in the proposal; the opt-out is documented and the adapter warns when it disagrees with the data engine.
- [Incremental book handling may be unexercised live] → closed-set validation plus the e2e histogram; snapshot behaviour is unchanged.
- [Maturin bin wheel for `rithmic-gateway` changes how `python/` builds] → consumers install wheels and never build; a source build needs Rust plus protoc, documented as the fallback.
- [Large single PR] → commits grouped by phase, each passing the full verify set.

## Migration Plan

1. Land the PR. CI publishes the first dev wheels and the Pages index.
2. Trading nodes:
   - set `bars_timestamp_on_close=False` only if they must keep open-time bars;
   - switch installs to the index.
3. Dev checkouts that used the cargo `target/` scan set `RITHMIC_GATEWAY_BIN` (or `uv run maturin develop` in `python/`).
4. Merge the market-data-lake PR after the first index publish.
5. Rollback: pin the previous wheel in the lock (`uv lock --upgrade-package rithmic-gateway==<old>`), or revert the PR. The protocol additions are backward compatible.

## Open Questions

- Which `update_type` values does Rithmic actually send on LucidTrading and Test? To be answered by the e2e histogram.
- Should `rithmic-nt-connect` dev wheels also cover cp314 on Linux ARM64 from day one, given Nautilus 1.231 wheel availability? Check in the first CI run.
