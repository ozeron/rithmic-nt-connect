## Context

Today's flows:

```
direct : client → _lib.Session → DirectSession ─────────────────────────────→ Rithmic
gateway: client → _lib.Session → GatewaySession → socket → server(hub logic) → DirectSession → Rithmic
```

The "hub logic" exists only on the gateway path. It consists of:
- `FanoutHub` with per-client bounded queues (`subscriptions.rs`)
- `IntentStore`, `RestorePlan`, `MdIntent` and `ReconnectController` (`reconnect.rs`)
- the event pump, reconnect loop and `restore_intents` (`server/pump.rs`)
- `ParentGates`
- the dispatch handlers that combine gate checks, intent notes, plant calls and error mapping (`server/dispatch/{subscriptions,orders,history,info}.rs`)

Python makes up for the missing logic in direct mode:
- `reset_ticker` when `is_direct` (`data.py:578`)
- `_open_direct_gates` (`execution.py:1083`)
- a recovery bridge that is a no-op in direct mode (ADR 0002)

The gateway server is the composition root for the parent. The PyO3 module is the composition root for the adapter.

Constraints that stay:
- `session.proto` and the lake client are unchanged.
- Only the parent holds the password, clients attach only, and clients never spawn the parent.
- One-shot commands are never retried, and a failure after send is unknown.
- Lanes keep slow reads from holding up commands on one socket.
- Trading is off by default.

## Goals / Non-Goals

**Goals:**
- One flow:
  ```
  client → _lib.Session → (LocalSession | RemoteSession → socket → server → LocalSession) → SessionHost → Venue → Rithmic
  ```
- Intents, fan-out, gates and recovery are implemented once, in `SessionHost`, and used in both modes.
- No mode branches in the Python adapter.
- Dependency injection at four seams (venue, transport, session, config). The hub logic is tested once against a fake venue.
- No wire change except one additive event, `TransportInterrupted` (D8.1).

**Non-Goals:**
- Generated surfaces, typed records and removing `poll_*`. These belong to `collapse-session-surface`, which is rebased onto this change.
- Per-plant actors inside `RithmicVenue` (a later step).
- Splitting the Nautilus clients.
- Retiring the lake client.
- Remote transports other than unix.

## Decisions

### D1. Names and crate graph (dependency inversion)

| Name | Meaning |
|---|---|
| `Venue` (trait) | the port the host drives: plants, requests, event streams |
| `RithmicVenue` | `Venue` on `rithmic-rs` (today's `DirectSession` plus the private `RithmicSession`, renamed `Plants`) |
| `FakeVenue` | scripted `Venue` for tests (today's `FakeSession`, in `rithmic-host`) |
| `FakeRithmicServer` | fake Rithmic websocket server for testing `RithmicVenue` offline (today's `rithmic_plants::fake_venue::FakeVenue`, which stays in `rithmic-plants`) |
| `SessionHost` | one shared login: intents, fan-out, gates, pump, reconnect, restore, session events |
| `Session` (trait) | what a consumer calls |
| `LocalSession` | a session attached to a `SessionHost` in the same process |
| `RemoteSession` | a session attached to a parent's `SessionHost` over the socket (today's `GatewaySession`) |

```
rithmic-exec        pure rules                                        (no deps)
rithmic-host        Venue, Session, DTOs, commands, Error, Env, RPC_TABLE,
                    SessionHost, LocalSession, HostRegistry, flock,
                    FakeVenue[testing]                                 (→ exec)
rithmic-plants      RithmicVenue                                       (→ host, exec, rithmic-rs)
rithmic-gateway     server(stream, host) + RemoteSession + session.proto (→ host)
  bin rithmic-gateway   composition root                               (→ gateway, plants)
rithmic-nt-connect  composition root: Local(RithmicVenue) | Remote     (→ host, plants, gateway)
rithmic-loadtest    host + gateway with FakeVenue                      (→ gateway, host[testing])
```

The traits live where they are used: the host defines `Venue` and `plants` implements it. So `rithmic-rs` cannot reach the host, the gateway library or Python, and the compiler enforces that. Only the two composition roots know every crate.

*Alternatives considered:*
- **Putting the host inside `rithmic-gateway`.** Direct mode would then run "gateway" code, the venue crate would depend on the gateway, and the extension would build the server. Rejected.
- **A separate `api` crate for the contract.** Once the host owns the contract, there's nothing left for it to isolate. Rejected.
- **A separate `wire` crate.** The server and `RemoteSession` stay in one crate, so they already share `pb`. Rejected.

### D2. `SessionHost` owns behaviour, sessions are attachments

```rust
pub struct SessionHost<V: Venue> { venue: V, intents: IntentStore, fanout: FanoutHub,
                                   gates: Gates, generation: AtomicU64, events: EventBus, … }
impl<V: Venue> SessionHost<V> {
    pub fn start(venue: V, gates: Gates, timing: Timing) -> Arc<Self>;   // spawns pump
    pub fn attach(self: &Arc<Self>) -> LocalSession<V>;                 // new consumer
}
```

**`LocalSession`.** Each one has a `SessionId`, its own fan-out queues and its own intent counts. Every `Session` method goes through the host. The behaviour per RPC kind is today's gateway dispatch behaviour, taken from `RPC_TABLE`:

| Kind | Path |
|---|---|
| one-shot command | gate → venue call → error map |
| intent | gate → note intent for this session → venue call on 0→1 → rollback on failure |
| intent release | drop count → venue call on 1→0 |
| read | gate → venue call |

The read-specific locks (`recon_lock`, `history_load_lock`, refuse-history-under-live-market-data) move with their handlers. `detach` (on `disconnect` or `Drop`) releases only that session's intents and queues. `Drop` never blocks on venue I/O: it releases counts and schedules any venue unsubscribe on the host's runtime.

**Lanes stay a transport concern.** `RPC_TABLE` moves to the host, and the server reads its `lane` column. An in-process `LocalSession` needs no lanes, because each call already runs on its own.

**What moves, and where:**

| From (`rithmic-gateway`) | To (`rithmic-host`) |
|---|---|
| `subscriptions.rs` (`FanoutHub`, `ParentGates`) | `fanout.rs`, `gates.rs` |
| `reconnect.rs` | `intents.rs` |
| `server/pump.rs` | `pump.rs` (pump, reconnect loop, restore) |
| `server/dispatch/{subscriptions,orders,history,info}.rs` logic | `local.rs` (`LocalSession` methods) |
| `rpc_table.rs` | `rpc_table.rs` |
| `rithmic-plants::fake{,_venue}.rs` | `fake.rs` (`testing`) |
| `rithmic-plants::{dto,error,streams,plants,config}` contract parts | `dto.rs`, `error.rs`, … |

### D3. One recovery model

The host's reconnect loop is the only plant recovery, and it is the same in both modes.

1. **Detect.** The pump's stream task reports a connection issue for a plant.
2. **Recover the smallest unit.** A ticker or history failure recreates that plant only, which carries today's `reset_ticker` semantics into the host. A PnL failure re-subscribes PnL. An order-plant failure re-seats the order plant (the existing re-seat path). A login-level failure reconnects everything.
3. **Restore.** Re-issue every intent the restored plants carry. A market-data intent that keeps failing is left degraded, and the next subscribe retries it. An account intent that cannot be restored escalates to a full reconnect.
4. **Notify.** Each attached session receives `Interrupted` at step 1, then `Resumed { generation }` after step 3, or `RecoveryFailed` after a bounded number of attempts.

`RemoteSession` adds its own socket-level loop. On socket loss it emits `Interrupted` and re-dials once for all waiters. Because the parent dropped the connection's intents, it re-issues every intent it has recorded (it already refcounts intents per facade), then emits `Resumed`. A one-shot command in flight at the drop is still unknown and is never retried.

The adapter's `TransportRecoveryBridge` keeps only policy:
- on `Interrupted`, exec disarms order handling
- on `Resumed`, exec drains working orders, waits for the PnL barrier and re-arms; data reconciles its book and bars on a gap

It no longer re-issues market-data intents and no longer calls `reset_ticker`. **ADR 0004** records this. It supersedes ADR 0002's rule that the adapter replays market data after transport recovery, and it keeps ADR 0002's rule that re-arm and capability policy stay in the adapter.

*Alternative considered:* keep market-data replay in the adapter for the remote socket path only. That keeps two recovery paths, which is the problem this change removes.

**Verified in task 0.2 (2026-10-15):** today's parent rebuilds **all four plants** on a ticker-only failure. The pump aborts every stream task, then calls full-session `disconnect()` / `connect()` (`server/pump.rs:120,173`), and re-issues every intent (ticker, book, PnL, orders). The characterization test is `crates/rithmic-gateway/tests/ticker_failure_characterization.rs`.

Attached clients receive only `TransportResumed`, with no "down" signal and no gap. The Rust client turns that into a synthetic Down/Up pair *after* the restore (`client/runtime.rs:749`), so a gateway exec client is never disarmed while the order plant is down.

Two consequences:
- Per-plant recovery (step 2) is a real improvement for gateway mode too: a ticker failure will stop bouncing the order and PnL plants.
- `TransportInterrupted` (D8.1) closes the disarm gap.

When group 4 lands, the characterization test is updated to the new expected call sequence.

### D4. Gates are host state; `open_gates` is the only call

`Gates { trading: AtomicBool, cancel_all: AtomicBool }` live on the host, closed by default.

- **Gateway parent:** its binary sets them from `RITHMIC_ENABLE_TRADING` / `RITHMIC_GATEWAY_CANCEL_ALL` when it starts.
- **`Session::open_gates(trading, cancel_all) -> Gates`:**
  - a `LocalSession` on an adapter-owned host opens the requested gates (open-only, never closes) and returns the effective state
  - a `LocalSession` created by the gateway server for a socket connection never elevates
  - a `RemoteSession` never elevates and returns the scopes the parent advertised at handshake
- **Exec client:** calls `open_gates` once, in both modes, and logs a warning when trading was requested but not granted.

So the `is_direct` check goes away, and a gateway client still can't elevate a gate.

### D5. Host registry and credential flock

`HostRegistry` (in `rithmic-host`) maps a credential fingerprint (same fields as today, never the password) to `Weak<SessionHost<V>>`.

- **`get_or_start(config, make_venue, lock_dir)`:** takes the flock and starts the host on a miss; returns the existing host on a hit.
- **Plant requests:** extra plants (execution) are a host call (`request_plants`), never part of construction.
- **Shutdown:** when the last attached session detaches, the host disconnects the venue (in the background on `Drop`, inline on `disconnect`) and drops the flock. A pump task or the registry's `Weak` keeps nothing alive.
- **The gateway binary** uses the same registry with a single host. Its process singleton (`singleton.rs`) still guards the socket path.

This is what `collapse-session-surface` group 1 described as "registry, holders and flock". Here the holders are attached sessions, so that group shrinks to deleting the Python wrappers.

### D6. Dependency-injection seams

| Seam | Mechanism | Used by tests as |
|---|---|---|
| Venue | `SessionHost<V: Venue>` (generic, monomorphised) | `SessionHost<FakeVenue>` |
| Transport | `serve(stream: impl AsyncRead + AsyncWrite + Unpin, host)` and `RemoteSession::over(stream)`. Unix dialling lives only at the roots. | `tokio::io::duplex()` |
| Session at PyO3 | `_lib.Session { inner: Arc<dyn Session> }` | `Session.fake(script=...)` (`testing` feature) |
| Config | `SessionConfig` / `GatewayConfig` values built at the roots; nothing below reads env | plain structs |
| Lock location | `lock_dir: PathBuf` argument | `tempdir()` |

Not injected:
- **Time:** use tokio paused time (`start_paused`, `advance`). The pump's `Timing` stays only as configuration.
- **Logging.**
- **A `CredentialLock` trait:** there's no second real implementation.

`FakeVenue` stays scriptable at the plant level: it emits events, fails a plant, rejects a request and records calls. This is the existing `FakeSession` behaviour moved under the new trait.

### D7. Test layout

| Layer | Suite |
|---|---|
| Host semantics | `rithmic-host/tests/*` on `SessionHost<FakeVenue>`: refcounts, gates, lanes table, restore plan coverage, per-plant recovery, degraded intents, detach and drop |
| Mode parity | one scenario list (today's `client_scenarios.rs`) run against `LocalSession` and against `RemoteSession` over `duplex()` to a server on the same host |
| Venue | `rithmic-plants` tests on recorded venue payloads (the venue-shaped fixtures rule) |
| Gateway transport | handshake, auth, lanes, idle exit, singleton, frame errors (unchanged suites, now over `duplex()` where possible) |
| Python adapter | `Session.fake(...)` for session and recovery tests. `WireSessionStub` stays for pure translation tests until `collapse-session-surface` |
| Live | `tests/e2e` unchanged, plus a direct-mode reconnect case |

`tests/rpc_table.rs` and `tests/lanes.rs` move to the host and keep their assertions.

### D8. Gaps found in review (2026-10-15)

These come from a review of the plan against the code. Each one is now part of the specs and tasks.

1. **A remote session cannot see a plant interruption.** The wire has `TransportResumed` (`session.proto` event field 10) but no "down" event, so a gateway client cannot disarm when the parent's plants drop. → Add one additive event, `TransportInterrupted = 12`. This is the only wire change. Old clients and the lake client ignore unknown oneof fields; task 0.3 verifies that. A new client talking to an old parent gets no `Interrupted`, which is today's behaviour.
2. **History refused under live market data is gateway-only today** (`server/dispatch/history.rs`, `reconnect.rs`). Moving it into the host unchanged would make direct mode refuse `request_bars` while subscribed. → It becomes a host option, `HistoryPolicy::{Allow, RefuseUnderLiveMd}`, chosen by the root: the parent keeps `RefuseUnderLiveMd`, direct keeps `Allow`. This is an explicit, documented root setting, not a hidden mode branch. Unifying it is an open question.
3. **The credential flock would be taken twice.** Until `collapse-session-surface` lands, `_FlockedDirectSession` takes the flock in Python before building `_lib.Session`. If the host registry also takes it, the second `flock` on a new file descriptor in the same process blocks or fails. → Group 5 moves the flock, the singleton and the holder count into the host registry together, and deletes `_SESSION_CACHE` and the Python `SessionLock` use at the same time. The forwarding wrapper may stay until `collapse-session-surface`.
4. **Consumers see recovery as stream errors today.** `next_events` raises on a failed plant, and the data client's `_poll_channel_loop` turns that into `_resync_channel` → `reset_ticker`. → With the host, a plant failure is not a consumer stream error. `next_events` returns empty batches during recovery and resumes afterwards; the session receives `Interrupted` / `Resumed`. A stream error reaches the consumer only after `RecoveryFailed` or a session close. A gap stays a gap.
5. **`disconnect_order_plant` is shared state today.** It disconnects the plant, and order commands fail `NotSent` until re-ensured. On a shared host that would cut off the other session. → It becomes per session: it releases this session's order and bracket intents, and this session's order commands fail `NotSent` until it ensures or subscribes again. The host leaves the order plant only on 1→0.
6. **A repeat subscribe must not double-count.** The server keeps per-client intent sets (`client_has` / `client_insert`). → The specs say intents are a set per session: a repeated subscribe is idempotent and re-joins a degraded intent.
7. **`connect` must stay idempotent across sessions.** → On `LocalSession`, `connect` means "ensure the host is connected". It succeeds if the host is already up and runs one plant connect for concurrent callers.
8. **The `testing` feature must stay out of release wheels.** → A packaging test asserts that `Session.fake` is absent from the release wheel.
9. **Throughput budgets are gateway-only.** → Add a direct-mode budget: a `LocalSession` must not be slower than today's direct path in `test_perf_ffi_throughput.py`. Record the baseline before group 2.

10. **A joining session gets no snapshot (found in wave 3).** The venue sends a book snapshot (`update_type` 3) or a PnL position snapshot only on its own subscribe. Under refcounting, a session that joins an intent the host already holds gets neither. This covers:
    - a second consumer
    - a gap re-sync (today direct mode re-subscribes at the venue, so the gap path does get a fresh snapshot)
    - a `RemoteSession` re-joining after a socket drop
    - the exec re-arm barrier waiting for fresh PnL

    Today's gateway has the same latent bug, because the parent no-ops a repeat subscribe. → **Snapshot on join:**
    - When a session joins, or re-joins after a gap, an already-live **PnL** intent, the host calls the venue's `get_pnl_position_snapshot` (rithmic-rs `RithmicPnlPlantHandle`; `Venue::pnl_snapshot`). The snapshot rows reach every PnL session; duplicate rows are harmless.
    - For an already-live **book** intent there is no L2 snapshot request, so the host refreshes the key at the venue with `Venue::refresh_book(key)`. Task 4.2b verifies live whether a repeat `subscribe_order_book_summary` resends a snapshot: if it does, `refresh_book` is that repeat; otherwise it is unsubscribe + subscribe. Either way every book session receives CLEAR plus a snapshot, which is a valid state.
    - Ticker, bars and order intents need no snapshot. A ticker or bar consumer that lost events has a gap; exec drains working orders with `load_orders`.

Also renamed in the task list: `rithmic-loadtest/src/rig.rs` (uses the old type names).

## Risks / Trade-offs

- **[Direct mode changes behaviour]** It gains refcounting, per-session queues, gates in the host and recovery. → The same scenario suite runs against both modes, and the live e2e suite runs on the test account in direct mode before release.
- **[Per-session queues add a hop in direct mode]** → Measure with `tests/test_perf_ffi_throughput.py` and `rithmic-loadtest` before and after. The budget is the existing gateway throughput spec.
- **[Moving market-data replay into `RemoteSession`]** A client and parent of different versions would both replay. → The parent treats a repeated subscribe as a no-op (current spec), so the only cost of double replay is a few no-op frames. ADR 0004 records the switch.
- **[Large mechanical move, about 2.5k lines]** → Move first, behaviour second: groups 1–3 are pure moves and renames with green tests, and the behaviour changes come in groups 4–6.
- **[Drop-time unsubscribe on the runtime at interpreter shutdown]** → It is best-effort and bounded. The explicit `disconnect` path is unchanged.
- **[Name churn for readers of old PRs]** → Old names are listed in `crates/README.md` for one release.

## Migration Plan

Each task group is one PR and leaves the tree green:

1. Create `rithmic-host` and move the contract (traits, DTOs, error, `Env`, `RPC_TABLE`, the fake). Rename `DirectSession` → `RithmicVenue` and `FakeSession` → `FakeVenue`. No behaviour change.
2. Move the hub logic (`FanoutHub`, gates, intents, pump, restore, handler logic) into `SessionHost` / `LocalSession`. The gateway server attaches a `LocalSession` per connection. No wire or behaviour change in gateway mode.
3. Make the server and `RemoteSession` generic over the stream. Move the tests to `duplex()`. Rename `GatewaySession` → `RemoteSession`.
4. Per-plant recovery and session events in the host.
5. Direct mode runs on `SessionHost` through the registry. Add `open_gates` and `Session.fake`.
6. `RemoteSession` re-joins its intents after a socket re-attach. Simplify the Python adapter (remove `reset_ticker`, `is_direct`, `_open_direct_gates`; the bridge runs in both modes).
7. Docs and ADR 0004, then rebase `collapse-session-surface`.

Rollback is a per-PR revert. Groups 1–3 are refactors. Group 5 is the switch-over for direct mode, and reverting it restores today's direct path.

## Parallel plan

Group 1 renames and moves across the whole workspace, so it lands alone and fast. After it, the work splits into tracks that touch different files:

| Track | Work | Starts after | Main files |
|---|---|---|---|
| **P: prep** | 0.1–0.6: baselines, ticker-failure characterization, `TransportInterrupted` plus the lake check, history-policy decision, ADR 0004 draft, Python API freeze in `_lib.pyi` | now | tests, proto, docs |
| **A: host core** (critical path) | group 2 → group 4 → 5.1–5.4 | 1 | `rithmic-host`, `server/dispatch` |
| **B: transport** | group 3 plus 6.1 (`RemoteSession` re-join) | 1 (3.1 can start now on current code) | `server/mod.rs` listen, `client/` |
| **C: test harness** | 5.5 `Session.fake`, the shared scenario list, porting host tests | 2.4 (`LocalSession` exists) | `rithmic-host/tests`, `nt-connect` testing feature |
| **D: Python adapter** | 6.2–6.5 against the frozen API from 0.6, using `WireSessionStub`; switched to `Session.fake` once C lands | 0.6 | `data.py`, `execution.py`, `recovery_bridge.py`, tests |
| **E: docs** | 7.1–7.4 | 0.5, finalised after A | docs |

**Critical path:** 1 → 2 → 4 → 5 → 6.6 (e2e in both modes) → 7.5 / 7.6. Tracks B, C, D and E run beside it.

**Conflict hot spots:**
- A and B both edit `server/mod.rs`. B should not change `server/mod.rs` until A's 2.5 lands, except for 3.1, which lands before 2.5.
- A (5.3, 5.4) and D both edit `_lib.pyi`. The 0.6 freeze makes D code against the agreed signatures.

## Resolved Questions (2026-10-15)

- **History refusal in direct mode (D8.2):** keep today's split. The parent's host uses `RefuseUnderLiveMd` and the direct root uses `Allow`. Revisit with live data in a later change.
- **Registry key and plant set:** one host per credential fingerprint, and the host grows its plant set (`request_plants`) as today. `plants` is not part of the key.
- **Python event names:** `Interrupted` / `Resumed` / `RecoveryFailed` are the new names. `transport.py` re-exports `TransportDown` / `TransportUp` / `RecoveryFailed` as aliases for one release.
- **Python API freeze (task 0.6):** the frozen signatures live in D9 below, not in `_lib.pyi`. Adding them to `_lib.pyi` before the extension has them would fail the stub-vs-extension check.

### D9. Frozen Python API (contract for track D)

```python
class Session:
    def open_gates(
        self, trading: bool, cancel_all: bool
    ) -> Gates: ...  # replaces configure_gates
    def subscribe_session_events(
        self, listener: Callable[[SessionEvent], None]
    ) -> SessionEventSubscription: ...  # replaces subscribe_transport_events

    # removed: reset_ticker, configure_gates, is_direct, subscribe_transport_events


@final
class Gates:  # frozen
    trading: bool
    cancel_all: bool


@final
class Interrupted:  # frozen; plant or socket path broke
    generation: int  # generation at the time of the break
    cause: str | None


@final
class Resumed:  # frozen; intents are live again
    generation: int  # new generation (previous + 1)


@final
class RecoveryFailed:  # frozen; recovery gave up
    generation: int
    attempts: int
    cause: str | None


SessionEvent = Interrupted | Resumed | RecoveryFailed


class SessionEventSubscription:
    def cancel(self) -> None: ...
```

Listeners run on the extension's dedicated delivery thread, as transport listeners do today. During host recovery, `next_events` returns empty batches (D8.4).
