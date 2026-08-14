//! Which Rithmic plants a session connects.

use crate::error::{Error, Result};

/// Plants to attach on [`crate::session::RithmicSession::connect`].
///
/// Order plant is never part of this set: it stays lazy via `ensure_order_plant`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlantSet {
    /// Ticker plant (live MD, front month, reference data).
    pub ticker: bool,
    /// History plant (tick / bar replay).
    pub history: bool,
    /// PnL plant (only when the session has an account triple).
    pub pnl: bool,
}

impl PlantSet {
    /// Ticker + history. Data clients and historical loads use this.
    pub const MARKET_DATA: Self = Self {
        ticker: true,
        history: true,
        pnl: false,
    };

    /// Ticker + history + PnL (PnL skipped when no account). Execution clients.
    pub const EXECUTION: Self = Self {
        ticker: true,
        history: true,
        pnl: true,
    };

    /// Parse `"market_data"` / `"execution"`.
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim() {
            "market_data" | "data" | "md" => Ok(Self::MARKET_DATA),
            "execution" | "exec" => Ok(Self::EXECUTION),
            other => Err(Error::Config(format!(
                "invalid plants {other:?}; expected market_data or execution"
            ))),
        }
    }
}

impl Default for PlantSet {
    fn default() -> Self {
        Self::MARKET_DATA
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_aliases() {
        assert_eq!(PlantSet::parse("market_data").unwrap(), PlantSet::MARKET_DATA);
        assert_eq!(PlantSet::parse("execution").unwrap(), PlantSet::EXECUTION);
        assert!(PlantSet::parse("orders").is_err());
    }
}
