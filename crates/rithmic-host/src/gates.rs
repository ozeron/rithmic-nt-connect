//! Capability gates supplied by the composition root.

/// Parent-side capability gates.
#[derive(Debug, Clone, Copy, Default)]
pub struct ParentGates {
    pub trading_enabled: bool,
    pub cancel_all_enabled: bool,
}

impl ParentGates {
    pub fn allow_place(&self) -> bool {
        self.trading_enabled
    }

    pub fn allow_cancel_all(&self) -> bool {
        self.cancel_all_enabled
    }

    pub fn scopes(&self) -> Vec<String> {
        let mut s = vec!["md".into(), "history".into(), "pnl".into()];
        if self.trading_enabled {
            s.push("trade".into());
        }
        if self.cancel_all_enabled {
            s.push("cancel_all".into());
        }
        s
    }
}
