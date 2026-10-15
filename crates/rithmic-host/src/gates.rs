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

/// Effective consumer-facing gate snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Gates {
    pub trading: bool,
    pub cancel_all: bool,
}

/// Host state: scopes can only grow during one host incarnation.
pub struct HostGates {
    trading: std::sync::atomic::AtomicBool,
    cancel_all: std::sync::atomic::AtomicBool,
}
impl HostGates {
    pub fn new(gates: ParentGates) -> Self {
        Self {
            trading: gates.trading_enabled.into(),
            cancel_all: gates.cancel_all_enabled.into(),
        }
    }
    pub fn snapshot(&self) -> Gates {
        use std::sync::atomic::Ordering::SeqCst;
        Gates {
            trading: self.trading.load(SeqCst),
            cancel_all: self.cancel_all.load(SeqCst),
        }
    }
    pub fn open(&self, trading: bool, cancel_all: bool) -> Gates {
        use std::sync::atomic::Ordering::SeqCst;
        if trading {
            self.trading.store(true, SeqCst);
        }
        if cancel_all {
            self.cancel_all.store(true, SeqCst);
        }
        self.snapshot()
    }
    pub fn scopes(&self) -> Vec<String> {
        let gates = self.snapshot();
        ParentGates {
            trading_enabled: gates.trading,
            cancel_all_enabled: gates.cancel_all,
        }
        .scopes()
    }
}
