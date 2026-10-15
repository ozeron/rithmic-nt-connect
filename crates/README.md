# Rust crates

Cargo workspace members (see the root `Cargo.toml`):

| Crate | Role |
| --- | --- |
| `rithmic-exec` | Pure, Nautilus-independent execution rules: lifecycle policy, exposure and commission lookup, fill identity / dedup, drain-row trust and staleness, inventory rules. |
| `rithmic-host` | `Venue` and consumer `Session` contracts, DTOs, commands, errors, `Env`, `PlantSet`, streams and RPC classification. `SessionHost` owns intents, fan-out, gates, pump and plant recovery; `LocalSession` attaches in-process. The `testing` feature adds `FakeVenue`. No venue SDK or protobuf dependency. |
| `rithmic-plants` | `RithmicVenue`, the `Venue` implementation over `rithmic-rs`, with private `Plants`, config, account discovery and venue parsing. The `testing` feature adds the websocket `FakeRithmicServer`. |
| `rithmic-gateway` | Protobuf transport: server attaches a `LocalSession` per connection; `RemoteSession` implements `Session` over a socket. Owns framing, handshake, auth, lanes, singleton and idle exit. The production binary composes the host with `RithmicVenue`; `testing` enables `rithmic-host/testing` for the fake binary. |
| `rithmic-nt-connect` | PyO3 composition root and execution-rule bindings. Currently binds `RithmicVenue`, `RemoteSession` and `SessionLock`; direct attachment is pending wave 5 (below). |
| `rithmic-loadtest` | Credential-free synthetic host / gateway load harness using `FakeVenue`; not shipped in wheels. |

Normal dependencies on this branch (arrow = depends on; external libraries
omitted except the venue SDK), verified with
`cargo tree -e normal --depth 1 -p <crate>` for each workspace member:

```text
rithmic-exec       -> (none)
rithmic-host       -> rithmic-exec
rithmic-plants     -> rithmic-host, rithmic-exec, rithmic-rs
rithmic-gateway    -> rithmic-host, rithmic-plants
rithmic-nt-connect -> rithmic-host, rithmic-plants, rithmic-gateway, rithmic-exec
rithmic-loadtest   -> rithmic-gateway[testing], rithmic-host[testing]
```

The gateway package still declares a normal dependency on plants: its production
binary constructs `RithmicVenue`. The host owns the sharing / recovery logic;
the gateway library maps protobuf requests to `LocalSession` calls. Compatibility
exports in gateway `subscriptions.rs`, `reconnect.rs` and `server/pump.rs` point
at the host. Neither host nor gateway directly depends on `rithmic-rs`; only
plants imports the venue SDK. Only `rithmic-nt-connect` optionally depends on
PyO3 (`python` feature; absent from the default dependency tree).

Old → new names (kept for one release):

| Old | New |
| --- | --- |
| `rithmic_gateway::client::GatewaySession` | `rithmic_gateway::client::RemoteSession` |
| `rithmic_plants::Session` (`backend.rs`) | Split into `rithmic_host::Venue` (plant port) and `rithmic_host::Session` (consumer surface), both in `backend.rs` |
| `rithmic_plants::DirectSession` (`direct.rs`) | `rithmic_plants::RithmicVenue` (`venue.rs`) |
| `rithmic_plants::RithmicSession` | Private `rithmic_plants::session::Plants` |
| `rithmic_plants::fake::FakeSession` | `rithmic_host::fake::FakeVenue` (`testing`) |
| `rithmic_plants::fake_venue::FakeVenue` | `rithmic_plants::fake_server::FakeRithmicServer` (`testing`) |
| `rithmic_rs::RithmicEnv` in adapter callers | `rithmic_host::Env`; plants maps to the venue enum |
| Plants DTOs, commands, `Error` / `Result`, `Stream`, `PlantSet` | The same names in `rithmic_host` |
| Gateway RPC classification | `rithmic_host::rpc_table`; gateway retains protobuf mapping |
| Gateway `subscriptions::{FanoutHub, ParentGates}` | Host `fanout::FanoutHub`, `gates::ParentGates` |
| Gateway `reconnect` / `server::pump` recovery | Host `intents` / `pump`, owned by `SessionHost` |
| Gateway dispatch plant policy | Host `LocalSession` (`local.rs`, `local/subscriptions.rs`); gateway dispatch calls it |

This table records the migration, not a promise of aliases for every old name.
The wire adds `TransportInterrupted`; existing message names stay. See
[ADR 0004](../docs/adr/0004-session-host-owns-plant-recovery.md) and the
one-host rule in [`AGENTS.md`](../AGENTS.md).

## Request path

The gateway server already attaches a `LocalSession` to the parent's host for
each socket. Local and remote consumers share host intent refcounts, gates,
per-consumer queues, snapshot-on-join for book / PnL, and plant recovery.

<!-- after wave 5 -->
With the PyO3 direct attachment switched to the host:

```text
direct : Python -> PyO3 Session -> LocalSession ------------------------> SessionHost -> RithmicVenue -> Rithmic
gateway: Python -> PyO3 Session -> RemoteSession -> socket -> server
                                                          -> LocalSession -> SessionHost -> RithmicVenue -> Rithmic
```

<!-- after wave 5 -->
The direct composition root shares a host per credential through the host
registry and credential flock. The parent owns its host and credentials;
remote clients remain attach-only. Consumers reconcile `Interrupted` / `Resumed` /
`RecoveryFailed`; the host restores plant intents, while `RemoteSession` also
re-joins recorded intents after socket re-attach. The adapter never resets the
ticker or replays market-data intents. Until wave 5 lands, PyO3 direct mode still
uses bare `RithmicVenue`, and Python still owns direct singleton / flock handling.

Tests: `cargo test -p rithmic-host -p rithmic-plants -p rithmic-gateway -p rithmic-exec -p rithmic-nt-connect`.

`server::serve_connection(stream, state)` and `RemoteSession::over(config, stream)`
accept any `AsyncRead + AsyncWrite + Unpin + Send + 'static` stream. The owned
stream constructor is single-use; `RemoteSession::with_connector(config, connect)`
accepts a factory for fresh streams when handshake retry or recovery is required.
Cloning a remote session creates a disconnected facade with its own queues and
intent holdings on that same runtime. `RemoteSession::new(config)` retains Unix
attachment and fingerprint-based runtime sharing. Supplied streams bypass the
Unix registry and flock attestation: the caller owns transport authentication,
and the fingerprint/token handshake still runs. Generic server streams require
a token; `server::listen_unix` alone may attest same-UID empty-token peers.

Rust framing, handshake, lanes, outcome/retry, queue and fan-out scenarios use
`tokio::io::duplex()`. Unix attachment/path/flock tests, the fingerprint-to-socket
registry check and real parent-process restart stay on Unix sockets. The Python
client and binary lifecycle suites keep their Unix endpoints (path permissions,
singleton and idle exit); the Python API and wire messages are unchanged.
