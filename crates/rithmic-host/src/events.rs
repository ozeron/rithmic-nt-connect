//! Session lifecycle events: the one recovery signal consumers see in both modes.
//!
//! A session reports `Interrupted` when the path from it to the venue breaks (a
//! plant the host is recovering, or a dropped gateway socket), then `Resumed`
//! once its intents are live again, or `RecoveryFailed` when recovery gives up.
//! Consumers reconcile on these events; they never reset plants or re-issue
//! intents themselves (design D3, ADR 0004).

use std::sync::OnceLock;

use tokio::sync::broadcast;

/// A session lifecycle event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEvent {
    /// The path to the venue broke; `generation` is the one in force at the break.
    Interrupted {
        generation: u64,
        cause: Option<String>,
    },
    /// Intents are live again; `generation` is the new one (previous + 1).
    Resumed { generation: u64 },
    /// Recovery gave up after `attempts`.
    RecoveryFailed {
        generation: u64,
        attempts: u32,
        cause: Option<String>,
    },
}

/// Capacity of a session's event channel. Events are rare (one per recovery),
/// so a lagging listener only ever misses a burst of flaps, never real data.
pub const SESSION_EVENT_CAPACITY: usize = 64;

/// A receiver that never yields and never closes: the events of a session that
/// has no recovery of its own (a bare venue used as a session).
pub fn no_session_events() -> broadcast::Receiver<SessionEvent> {
    static NEVER: OnceLock<broadcast::Sender<SessionEvent>> = OnceLock::new();
    NEVER.get_or_init(|| broadcast::channel(1).0).subscribe()
}
