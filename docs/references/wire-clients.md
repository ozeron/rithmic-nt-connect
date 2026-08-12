# Wire-client references

## pbeets/rithmic-rs (primary Rust candidate)

- Repo: https://github.com/pbeets/rithmic-rs
- crates.io: `rithmic-rs`
- License: Apache-2.0
- Role: unofficial Rust R|Protocol client; template-versioned protos

Use as the **default foundation** for this project’s Rust core unless planning finds a blocking gap (missing plant coverage, license/proto redistribution conflict, or API mismatch with Nautilus mapping needs).

## async_rithmic (quirk / behavioral oracle)

- PyPI: `async-rithmic`
- Used successfully in MY046 for LucidTrading MD + account/PnL probe
- Plants: ticker, history, order, pnl, repository
- **Not** the production backend for this project (Rust-first decision); mine for plant semantics, subscription patterns, and error quirks

## parbhatc/Rithmic (Node quirk / behavioral oracle)

- Repo: https://github.com/parbhatc/Rithmic
- Vendored under MY046 `vendor/parbhatc-rithmic`
- MD-focused; order protos present but order APIs not exposed
- Useful for gateway/system discovery (`npm run systems`) and live/history smoke patterns

## Proto handling policy (this repo)

Do **not** commit gated Rithmic protobuf sources or certs unless the signed agreement explicitly permits redistribution.
Prefer depending on a client crate that already handles template packaging, or keep protos local/gitignored under `vendor/rithmic-proto/`.
