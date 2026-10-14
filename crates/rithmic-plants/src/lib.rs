//! rithmic-plants — shared Rithmic plant session façade over [`rithmic-rs`].
//!
//! This crate wraps Rithmic plants (market data, history, PnL, order) behind
//! a small API. It is consumed directly by `rithmic-nt-connect` (PyO3) and by
//! `rithmic-gateway` (broker) so both share one `RithmicSession` façade.

#![allow(missing_docs)]

pub mod account;
pub mod backend;
pub mod config;
pub mod direct;
pub mod dto;
pub mod error;
#[cfg(feature = "testing")]
pub mod fake;
pub mod history;
pub mod plants;
pub mod session;
pub mod streams;
pub mod systems;

pub use account::{pick_account, rows_from_account_list, AccountRow};
pub use backend::{AccountTriple, ModifyOrder, PlaceBracketOrder, PlaceOrder, Session};
pub use config::{
    SessionConfig, SessionConfigBuilder, DEFAULT_APP_NAME, DEFAULT_APP_VERSION, DEFAULT_LUCID_URL,
    DEFAULT_SYSTEM_NAME,
};
pub use direct::DirectSession;
pub use dto::{
    AccountPnlDto, BboDto, FillHistoryDto, FrontMonthDto, GapDto, HistoryBarDto, HistoryTickDto,
    InstrumentPnlDto, LastTradeDto, OrderBookDto, OrderNotificationDto, PlantEvent,
    ReferenceDataDto, TimeBarProbeRow,
};
pub use error::{Error, Result};
pub use plants::PlantSet;
pub use session::{Handles, RithmicSession};
pub use streams::{Stream, StreamHub};
pub use systems::{list_systems, normalize_gateway_url};

/// Re-export venue env enum so gateway/bin callers need not depend on `rithmic-rs` directly.
pub use rithmic_rs::RithmicEnv;
