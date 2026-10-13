//! Pure execution rules shared by the Python adapter and any other consumer.
//!
//! One implementation per rule; see `openspec/specs/exec-core` (change
//! `rust-exec-core`). Nothing here knows about Nautilus, plants, or an async
//! runtime.

pub mod plant;

pub use plant::{PlantPolicy, PlantState};
