//! rithmic-nt-connect — PyO3 bindings consumed by the Python package.
//!
//! The plant façade lives in `rithmic-plants` and the gateway client in
//! `rithmic-gateway`; this crate exposes one `Session` class over both.

#![allow(missing_docs)]

#[cfg(feature = "python")]
pub mod python;
