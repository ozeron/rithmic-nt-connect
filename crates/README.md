# Rust crates

Cargo workspace members (see the root `Cargo.toml`):

| Crate | Role |
| --- | --- |
| `rithmic-exec` | Pure, Nautilus-independent execution rules: lifecycle policy, exposure and commission lookup, fill identity / dedup, drain-row trust and staleness, inventory rules. |
| `rithmic-host` | Venue-independent contract: `Session`, DTOs, commands, account identity, errors, `Env`, `PlantSet`, event streams, history-slice maths and RPC classification. The `testing` feature adds the scripted `FakeVenue`. No `rithmic-rs` or protobuf dependency. |
| `rithmic-plants` | `RithmicVenue`, the in-process `Session` implementation over `rithmic-rs`, with its private `Plants` container, config, account discovery and venue parsing. The `testing` feature adds the websocket `FakeRithmicServer`. |
| `rithmic-gateway` | Shared-login broker, protobuf framing, server binary and `RemoteSession` client. Maps protobuf bodies to host RPC metadata. Its `testing` feature enables `rithmic-host/testing` for the fake gateway binary. |
| `rithmic-nt-connect` | PyO3 composition root: binds `RithmicVenue`, `RemoteSession`, `SessionLock` and execution rules to the unchanged Python `Session` API. |
| `rithmic-loadtest` | Credential-free synthetic gateway/session load harness using `FakeVenue`; not shipped in wheels. |

Dependency direction after Group 1 (arrow = depends on; external libraries omitted except the venue SDK):

```text
rithmic-host       -> rithmic-exec
rithmic-plants     -> rithmic-host, rithmic-exec, rithmic-rs
rithmic-gateway    -> rithmic-host, rithmic-plants
rithmic-nt-connect -> rithmic-host, rithmic-plants, rithmic-gateway, rithmic-exec
rithmic-loadtest   -> rithmic-gateway[testing], rithmic-host[testing]
```

The gateway still depends on plants in Group 1: the production binary constructs
`RithmicVenue`, and the hub/recovery implementation still lives in the gateway.
Group 2 moves that implementation into the host. `Session` is unchanged here;
the consumer/venue trait split is also deferred to Group 2. Neither the host nor
the gateway has a direct `rithmic-rs` dependency. Only plants imports the venue SDK.
No crate besides `rithmic-nt-connect` depends on PyO3.

Old → new names (kept for one release):

| Old | New |
| --- | --- |
| `rithmic_gateway::client::GatewaySession` | `rithmic_gateway::client::RemoteSession` |
| `rithmic_plants::Session` (`backend.rs`) | `rithmic_host::Session` (`backend.rs`), unchanged trait |
| `rithmic_plants::DirectSession` (`direct.rs`) | `rithmic_plants::RithmicVenue` (`venue.rs`) |
| `rithmic_plants::RithmicSession` | Private `rithmic_plants::session::Plants` |
| `rithmic_plants::fake::FakeSession` | `rithmic_host::fake::FakeVenue` (`testing`) |
| `rithmic_plants::fake_venue::FakeVenue` | `rithmic_plants::fake_server::FakeRithmicServer` (`testing`) |
| `rithmic_rs::RithmicEnv` in adapter callers | `rithmic_host::Env`; plants maps to the venue enum |
| Plants DTOs, commands, `Error` / `Result`, `Stream`, `PlantSet` | The same names in `rithmic_host` |
| Gateway RPC classification | `rithmic_host::rpc_table`; gateway retains protobuf mapping |

Python API names and wire behaviour are unchanged. Direct and gateway modes
implement the same `Session` trait; see the parity rule in [`AGENTS.md`](../AGENTS.md).

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
