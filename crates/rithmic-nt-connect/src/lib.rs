//! rithmic-nt-connect — Phase 2 Rithmic session facade (MD + history + PnL + orders).
//!
//! The plant façade itself lives in [`rithmic-plants`]; this crate re-exports
//! its public API for existing consumers and adds the PyO3 bindings consumed
//! by the Python package.

#![allow(missing_docs)]

pub use rithmic_plants::*;

#[cfg(feature = "python")]
pub mod python;
