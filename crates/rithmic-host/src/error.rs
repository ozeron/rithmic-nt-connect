//! Error types for the rithmic-nt-connect facade.

use thiserror::Error;

/// Result alias for facade operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Facade errors (never include passwords in messages).
#[derive(Debug, Error)]
pub enum Error {
    /// Incomplete or invalid session configuration.
    #[error("config error: {0}")]
    Config(String),

    /// Session is not connected / plant missing (legacy string form).
    #[error("session error: {0}")]
    Session(String),

    /// Session already connected (idempotent connect is safe to suppress).
    #[error("already connected")]
    AlreadyConnected,

    /// Plant subscription lagged; consumer must resync.
    #[error("channel lagged: {plant}")]
    ChannelLagged {
        /// Plant name (`ticker`, `pnl`, `order`).
        plant: &'static str,
        /// Messages skipped by the broadcast receiver.
        skipped: u64,
    },

    /// Plant subscription channel closed.
    #[error("channel closed: {plant}")]
    ChannelClosed {
        /// Plant name (`ticker`, `pnl`, `order`).
        plant: &'static str,
    },

    /// Required plant is not connected.
    #[error("not connected: {plant}")]
    NotConnected {
        /// Plant name (`ticker`, `pnl`, `order`, `history`).
        plant: &'static str,
    },

    /// Underlying rithmic-rs / plant error.
    #[error("rithmic error: {0}")]
    Rithmic(String),

    /// Order-history recon cannot be answered authoritatively.
    ///
    /// Rithmic order history has no completion signal (rows stream on the
    /// subscription channel with no end marker) and replays cap at 10,000
    /// records with no truncation indication. A silence-window drain therefore
    /// can never prove "venue has N and I got all N". Callers must fail closed
    /// rather than treat a lossy drain as authoritative "venue empty".
    #[error("order-history reconciliation unavailable: {0}")]
    ReconciliationUnavailable(String),

    /// Unexpected message shape.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// The gateway parent answered with a typed error (`code` is the wire code,
    /// e.g. `trading_disabled`, `parent_not_running`).
    #[error("gateway error: {code}: {message}")]
    Gateway {
        /// Stable machine code from the wire.
        code: String,
        /// Human-readable message (never contains secrets).
        message: String,
    },

    /// The venue definitively refused a state-changing request (a reject it sent
    /// back). The order is not live; unlike an unknown outcome this does not
    /// need a recon cycle.
    #[error("venue rejected: {0}")]
    Rejected(String),

    /// A state-changing request failed before anything was written (invalid
    /// argument, no trade route, plant missing, session disconnecting). The
    /// order is not live and was never at risk.
    #[error("not sent: {0}")]
    NotSent(String),

    /// A state-changing request was written to the gateway and its outcome is
    /// unknown (transport loss or timeout after send). Never retried and never
    /// a venue rejection: the order may or may not be live.
    #[error("outcome unknown: {0}")]
    OutcomeUnknown(String),

    /// Session-wide transport failure (login loss or a gateway socket failure).
    #[error("transport error: {0}")]
    Transport(String),
}
