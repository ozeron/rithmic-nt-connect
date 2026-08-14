//! rithmic-plants — shared Rithmic plant session façade over [`rithmic-rs`].
//!
//! This crate wraps Rithmic plants (market data, history, PnL, order) behind
//! a small API. It is consumed directly by `rithmic-nt-connect` (PyO3) and by
//! `rithmic-gateway` (broker) so both share one `RithmicSession` façade.

#![allow(missing_docs)]

pub mod config;
pub mod dto;
pub mod error;
pub mod history;
pub mod plants;
pub mod session;
pub mod systems;

pub use config::{
    DEFAULT_APP_NAME, DEFAULT_APP_VERSION, DEFAULT_LUCID_URL, DEFAULT_SYSTEM_NAME, SessionConfig,
    SessionConfigBuilder,
};
pub use dto::{
    AccountPnlDto, BboDto, FrontMonthDto, HistoryBarDto, HistoryTickDto, InstrumentPnlDto,
    LastTradeDto, OrderBookDto, OrderNotificationDto, PlantEvent, ReferenceDataDto,
    TimeBarProbeRow,
};
pub use error::{Error, Result};
pub use plants::PlantSet;
pub use session::RithmicSession;
pub use systems::{list_systems, normalize_gateway_url};

/// Re-export venue env enum so gateway/bin callers need not depend on `rithmic-rs` directly.
pub use rithmic_rs::RithmicEnv;
