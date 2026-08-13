//! rithmic-nt-connect — Phase 2 Rithmic session facade (MD + history + PnL + orders).
//!
//! This crate wraps [`rithmic-rs`] plants behind a small API consumed by the
//! Python package via PyO3. Phase 2 adds order plant place / cancel / modify
//! and order-notification polling when an account is configured.

#![allow(missing_docs)]

pub mod config;
pub mod dto;
pub mod error;
pub mod history;
pub mod plants;
pub mod session;
pub mod systems;

#[cfg(feature = "python")]
pub mod python;

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
