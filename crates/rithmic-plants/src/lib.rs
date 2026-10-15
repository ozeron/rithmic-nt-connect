//! Rithmic venue implementation over rithmic-rs; shared contracts live in rithmic-host.
#![allow(missing_docs)]
pub mod account;
pub mod config;
pub mod dto;
mod error;
#[cfg(feature = "testing")]
pub mod fake_server;
pub mod history;
mod session;
pub mod systems;
pub mod venue;

pub use account::{pick_account, rows_from_account_list, AccountRow};
pub use config::{
    SessionConfig, SessionConfigBuilder, DEFAULT_APP_NAME, DEFAULT_APP_VERSION, DEFAULT_LUCID_URL,
    DEFAULT_SYSTEM_NAME,
};
pub use systems::{list_systems, normalize_gateway_url};
pub use venue::RithmicVenue;
