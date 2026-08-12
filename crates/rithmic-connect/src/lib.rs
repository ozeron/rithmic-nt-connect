//! rithmic-connect — Phase 1 Rithmic session facade (MD + history + PnL; no orders).
//!
//! This crate wraps [`rithmic-rs`] plants behind a small API consumed by the
//! Python package via PyO3. Phase 1 intentionally exposes **no** public order
//! place / cancel / modify methods.

#![allow(missing_docs)]

pub mod config;
pub mod dto;
pub mod error;
pub mod session;

#[cfg(feature = "python")]
pub mod python;

pub use config::{
    DEFAULT_APP_NAME, DEFAULT_APP_VERSION, DEFAULT_LUCID_URL, DEFAULT_SYSTEM_NAME, SessionConfig,
    SessionConfigBuilder,
};
pub use dto::{
    AccountPnlDto, BboDto, FrontMonthDto, HistoryTickDto, InstrumentPnlDto, LastTradeDto,
    OrderBookDto, ReferenceDataDto, TickerEvent,
};
pub use error::{Error, Result};
pub use session::RithmicSession;
