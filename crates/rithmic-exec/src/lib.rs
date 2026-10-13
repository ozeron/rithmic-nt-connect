//! Pure execution rules shared by the Python adapter and any other consumer.
//!
//! One implementation per rule; see `openspec/specs/exec-core` (change
//! `rust-exec-core`). Nothing here knows about Nautilus, plants, or an async
//! runtime.

pub mod commission;
pub mod exposure;
pub mod plant;

pub use commission::CommissionTable;
pub use exposure::{exposure_complete, matches_root, product_root, signed_net, PositionRow};
pub use plant::{PlantPolicy, PlantState};
