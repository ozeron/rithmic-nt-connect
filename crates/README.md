# Rust crates

Cargo workspace members (see the root `Cargo.toml`):

| Crate | Role |
| --- | --- |
| `rithmic-exec` | Pure, Nautilus-independent execution rules (order-plant lifecycle policy, exposure and commission lookup, fill identity / dedup keys, drain-row trust and staleness, inventory rules). No dependency on plants, tokio, or Python. |
| `rithmic-plants` | Shared plant session façade over `rithmic-rs` (ticker, history, PnL, order plants): config, DTOs, history slicing and retry, account discovery, and the `Session` trait with its `DirectSession` implementation. A `testing`-feature `fake` session backs gateway tests. |
| `rithmic-gateway` | Shared-login broker: length-delimited protobuf framing, the `rithmic-gateway` server binary (generic over `Session`, RPC surface in `rpc_table.rs`), and `GatewaySession`, the Rust gateway client that implements `Session` over a unix socket. |
| `rithmic-nt-connect` | PyO3 extension (`rithmic_nt_connect._lib`, built with the `python` feature via maturin). Re-exports `rithmic-plants` and binds direct sessions, the gateway client, `SessionLock`, and the `rithmic-exec` rules to Python. |

Dependency direction (arrow = depends on):

```text
rithmic-nt-connect -> rithmic-gateway -> rithmic-plants -> rithmic-exec
rithmic-nt-connect -> rithmic-plants
rithmic-nt-connect -> rithmic-exec
```

`rithmic-exec` is the leaf; nothing in the first three crates depends on PyO3.

Direct and gateway modes are both implementations of the `Session` trait, so plant order and exec surfaces stay capability-compatible; see the parity rule in [`AGENTS.md`](../AGENTS.md).

Tests: `cargo test -p rithmic-plants -p rithmic-gateway -p rithmic-exec -p rithmic-nt-connect`.
