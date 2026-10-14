//! Pure execution rules shared by the Python adapter and any other consumer.
//!
//! One implementation per rule; see `openspec/specs/exec-core` (change
//! `rust-exec-core`). Nothing here knows about Nautilus, plants, or an async
//! runtime.

pub mod commission;
pub mod drain;
pub mod exposure;
pub mod fills;
pub mod ident;
pub mod inventory;
pub mod lru;
pub mod notify;
pub mod plant;

pub use commission::CommissionTable;
pub use drain::{
    freshest_per_key, row_is_trustworthy, stale_reason, RowTerms, StaleFacts,
    TRUSTWORTHY_DURATIONS, TRUSTWORTHY_PRICE_TYPES,
};
pub use exposure::{exposure_complete, matches_root, product_root, signed_net, PositionRow};
pub use fills::{dedup_key, trade_id, FillParts};
pub use ident::{
    event_ts_ns, exchange_ts_ns, instrument_id, split_instrument_id, venue_ts_ns, IdentError,
};
pub use inventory::{
    flatten_side, order_is_gone, pnl_live, position_change, side_from_txn, PositionChange,
    StatusClass,
};
pub use lru::Lru;
pub use notify::kind_from_notify;
pub use plant::{PlantPolicy, PlantState};
