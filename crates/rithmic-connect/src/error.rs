//! Error types for the rithmic-connect facade.

use thiserror::Error;

/// Result alias for facade operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Facade errors (never include passwords in messages).
#[derive(Debug, Error)]
pub enum Error {
    /// Incomplete or invalid session configuration.
    #[error("config error: {0}")]
    Config(String),

    /// Session is not connected / plant missing.
    #[error("session error: {0}")]
    Session(String),

    /// Underlying rithmic-rs / plant error.
    #[error("rithmic error: {0}")]
    Rithmic(String),

    /// Unexpected message shape.
    #[error("protocol error: {0}")]
    Protocol(String),
}

impl From<rithmic_rs::RithmicError> for Error {
    fn from(value: rithmic_rs::RithmicError) -> Self {
        Self::Rithmic(value.to_string())
    }
}

impl From<rithmic_rs::ConfigError> for Error {
    fn from(value: rithmic_rs::ConfigError) -> Self {
        Self::Config(value.to_string())
    }
}
