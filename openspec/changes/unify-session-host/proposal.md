## Why

Direct and gateway modes look like two implementations of one `Session` trait, but they behave differently.

Only the gateway parent does these things (`rithmic-gateway`: `subscriptions.rs`, `reconnect.rs`, `server/pump.rs`, `server/dispatch/*`):
- refcounts subscription intents across consumers
- gives each consumer its own event queue
- owns the trading and cancel-all gates
- restores intents after a plant reconnect
- tells consumers about interruptions

Direct mode does none of that, so the Python adapter fills the gap with mode branches:
- `resync_ticker_session` calls `reset_ticker` only when `is_direct` (`data.py:578`).
- `_open_direct_gates` (`execution.py:1083`) opens the gates in direct mode only.
- The recovery bridge is a deliberate no-op in direct mode, so direct mode has no reconnect recovery (a gap tracked in `docs/STATUS.md`).

The result is two flows to understand, test and debug.

The fix is to make the gateway's sharing and recovery logic a component that both modes use. Gateway mode then becomes "that same component, reached over a socket".

## What Changes

- **New crate `rithmic-host`.** It holds `SessionHost`: one shared Rithmic login per credential, with intent refcounting, per-session fan-out, gates, the event pump, plant reconnect and intent restore. It also holds:
  - the consumer-facing `Session` trait and its in-process implementation `LocalSession` (a session attached to a host)
  - the `Venue` trait (the port the host drives)
  - the plant DTOs and commands
  - `RPC_TABLE` (kinds, gates, lanes)
  - the credential flock and the per-process host registry
  - `FakeVenue` (feature `testing`)

  The logic moves here from `rithmic-gateway`.
- **BREAKING (internal): `rithmic-plants` becomes the venue connection only.** `DirectSession` is renamed `RithmicVenue` and implements `rithmic_host::Venue`. `FakeSession` becomes `FakeVenue` in `rithmic-host`. The `&mut self` plant bundle `RithmicSession` becomes a private part of `RithmicVenue`. `rithmic-rs` is a dependency of `rithmic-plants` only. The adapter defines its own `Env` enum so `rithmic_rs::RithmicEnv` no longer leaks into the gateway or the extension.
- **BREAKING (internal): `rithmic-gateway` becomes a transport.** Each accepted connection is a `LocalSession` attached to the parent's host. The server decodes a frame, calls the session and encodes the reply. Handshake, auth, lanes, idle exit and the singleton stay. `GatewaySession` is renamed `RemoteSession`. The only wire change is one additive event, `TransportInterrupted`, which old clients ignore. The lake `GatewayClient` keeps working.
- **One recovery model in both modes.** The host's reconnect loop is the only plant recovery. Every attached session receives the same events: `Interrupted`, then `Resumed { generation }` (intents already restored) or `RecoveryFailed`. After re-attaching the socket, `RemoteSession` re-issues its own recorded intents, so a remote consumer sees the same `Resumed` as a local one. The adapter keeps only the reconciliation policy (exec disarm, order drain, PnL barrier). This supersedes the market-data replay part of ADR 0002 (new ADR 0004).
- **One gate call.** `Session.open_gates(trading, cancel_all)` replaces `configure_gates` and the `is_direct` check. On a host the process owns, it opens gates (never closes them). On a `RemoteSession` it never elevates and returns the gates the parent actually grants.
- **Python loses its mode branches:** `reset_ticker`, `is_direct` and `_open_direct_gates` are removed from the adapter, and the recovery bridge runs in both modes.
- **Dependency injection at four seams:**
  - `SessionHost<V: Venue>`
  - the server and `RemoteSession` take any `AsyncRead + AsyncWrite` stream
  - `_lib.Session` holds `Arc<dyn Session>` and gains `Session.fake(...)` (feature `testing`)
  - configuration is resolved once, at the two composition roots: the PyO3 module and the gateway binary

  Tests run against `SessionHost<FakeVenue>`, against `RemoteSession` over an in-memory duplex stream, and from Python against `Session.fake`.

## Capabilities

### New Capabilities
- `session-host`: one shared login per credential used by both modes, sessions attaching to it, host-owned intents, fan-out, gates and recovery, identical recovery events in both modes, the `Venue` port with a fake, the gateway as a transport around a host, and composition roots.

### Modified Capabilities
- `session-backend`:
  - The plant surface is split into `Session` (consumer-facing, implemented by `LocalSession` and `RemoteSession`) and `Venue` (implemented by `RithmicVenue` and `FakeVenue`).
  - Backend selection picks local or remote.
  - Gate parity and identical events are defined by the shared host.
  - The direct singleton is a host per credential.
- `supported-modes`: trading and cancel-all are host-owned gates, opened in-process through `open_gates` or set by the parent's environment, and a remote session never elevates them.
- `gateway-client`:
  - Transport recovery re-joins the client's own intents and reports `Resumed` like a local session.
  - Plant reconnect is the host's, in both modes.
- `adapter-wire-facade`: recovery (PnL restore, re-arm) runs in both modes. There is no direct-mode no-op.

## Impact

- **Rust:**
  - new `crates/rithmic-host`
  - `crates/rithmic-plants`: `DirectSession` → `RithmicVenue`, `RithmicSession` made private, its own `Env`
  - `crates/rithmic-gateway`: `subscriptions.rs`, `reconnect.rs`, `server/pump.rs` and the dispatch handler logic (about 2.5k lines) move to the host; the server shrinks to transport; `client/` → `RemoteSession`
  - `crates/rithmic-nt-connect`: holds `Arc<dyn Session>`, gains `Session.fake` and `open_gates`
  - `crates/rithmic-loadtest`: retargeted at the host plus the gateway
- **Python:**
  - `data.py`: the reset branch is removed from `resync_ticker_session`
  - `execution.py`: `_open_direct_gates` → `open_gates`
  - `recovery_bridge.py`: no direct-mode no-op
  - `session.py`: no `is_direct` use
  - `_lib.pyi`
- **Behaviour:**
  - Direct mode gains intent refcounting across data and exec, per-session queues, reconnect restore and transport events.
  - Gateway mode is unchanged on the wire. Its only visible change is that a socket re-attach no longer needs the adapter to replay market data.
- **Docs:**
  - ADR 0004 (supersedes the market-data replay clause of ADR 0002)
  - AGENTS.md: new parity section; "direct ↔ gateway parity" becomes "one host, two attachments"
  - `crates/README.md`
  - `docs/STATUS.md`: direct-mode recovery mark
- **Related change:** `collapse-session-surface` is rebased onto this one. Its group 1 shrinks to deleting the Python wrappers, because the registry, holders and listeners become host attach/detach and host events.
