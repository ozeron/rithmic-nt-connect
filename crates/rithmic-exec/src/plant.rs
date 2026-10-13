//! Order-plant lifecycle state machine and trading policy.
//!
//! The readiness/recovery protocol is an explicit machine, not scattered state
//! assignments. The state moves only through the transition methods below, so
//! the set of possible transitions is finite and pinned by the exhaustive table
//! in the tests.
//!
//! `Latched` is a real state: an operation's venue outcome is unknown and
//! commands are blocked until a recon cycle (a successful re-arm). Only
//! [`PlantPolicy::latch`] enters it. [`PlantPolicy::rearm`] is the only arming
//! transition: it arms only from `Connecting` with a live poll task, so any
//! anomaly during the barrier (a newer latch, a stream failure, a mid-drain
//! resync) leaves the plant un-armed.

/// Order-plant lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PlantState {
    /// Transport down / not armed; commands blocked.
    Disconnected,
    /// Re-arm barrier in progress; not yet armed.
    Connecting,
    /// Armed; submit / modify / cancel allowed.
    Live,
    /// Mid-session transport resync; cancels allowed.
    Resyncing,
    /// Blocked pending a recon cycle (re-arm).
    Latched,
}

impl PlantState {
    pub const ALL: [PlantState; 5] = [
        PlantState::Disconnected,
        PlantState::Connecting,
        PlantState::Live,
        PlantState::Resyncing,
        PlantState::Latched,
    ];

    /// Stable wire name (also the text used in reject reasons).
    pub fn as_str(self) -> &'static str {
        match self {
            PlantState::Disconnected => "disconnected",
            PlantState::Connecting => "connecting",
            PlantState::Live => "live",
            PlantState::Resyncing => "resyncing",
            PlantState::Latched => "latched",
        }
    }

    pub fn parse(name: &str) -> Option<PlantState> {
        PlantState::ALL.into_iter().find(|s| s.as_str() == name)
    }
}

/// Owns every order-plant state transition and the trading policy.
#[derive(Clone, Debug)]
pub struct PlantPolicy {
    state: PlantState,
}

impl Default for PlantPolicy {
    fn default() -> Self {
        Self::new(PlantState::Disconnected)
    }
}

impl PlantPolicy {
    pub fn new(state: PlantState) -> Self {
        Self { state }
    }

    pub fn state(&self) -> PlantState {
        self.state
    }

    /// True while commands are blocked pending a recon cycle (a pure function of state).
    pub fn latched(&self) -> bool {
        self.state == PlantState::Latched
    }

    // --- transitions ---

    /// Enter the re-arm barrier (transport subscribed, not yet armed).
    pub fn begin_connect(&mut self) {
        self.state = PlantState::Connecting;
    }

    /// Transport down / not armed: commands blocked.
    pub fn disconnect(&mut self) {
        self.state = PlantState::Disconnected;
    }

    /// An anomaly that needs a recon cycle: commands blocked until a successful
    /// re-arm. A mid-session resync never clears it.
    pub fn latch(&mut self) {
        self.state = PlantState::Latched;
    }

    /// Begin a mid-session transport resync (cancels stay available).
    ///
    /// From `Latched` the plant stays blocked; from `Disconnected` it stays down
    /// (a dead stream must not re-arm via a resync); from `Connecting` it
    /// latches (a channel error during the barrier means the drain may predate
    /// the drop, so it must not enter the cancel-enabled `Resyncing` path whose
    /// completion would arm the plant mid-barrier).
    pub fn resync_start(&mut self) {
        self.state = match self.state {
            PlantState::Latched | PlantState::Disconnected => return,
            PlantState::Connecting => PlantState::Latched,
            PlantState::Live | PlantState::Resyncing => PlantState::Resyncing,
        };
    }

    /// A mid-session resync succeeded: live again (unless latched, down, or in
    /// the barrier, where `rearm` is the only arming transition).
    pub fn resync_complete(&mut self) {
        if self.state == PlantState::Resyncing {
            self.state = PlantState::Live;
        }
    }

    /// A mid-session resync failed: transport down, commands blocked. A latched
    /// plant stays latched (recon still pending).
    pub fn resync_failed(&mut self) {
        if self.state != PlantState::Latched {
            self.state = PlantState::Disconnected;
        }
    }

    /// Finish the re-arm barrier: armed (`Live`) only if still `Connecting`
    /// (no newer latch or stream failure during the drain) and the poll task is
    /// alive. A newer latch survives untouched; any other raced state falls to
    /// `Disconnected`. Returns false when the plant stays un-armed.
    pub fn rearm(&mut self, poll_alive: bool) -> bool {
        match self.state {
            PlantState::Latched => false,
            PlantState::Connecting if poll_alive => {
                self.state = PlantState::Live;
                true
            }
            _ => {
                // A dead poll task drained over a stream that cannot deliver;
                // any non-barrier state means the barrier raced.
                self.state = PlantState::Disconnected;
                false
            }
        }
    }

    // --- policy ---

    pub fn allow_submit(&self) -> bool {
        self.state == PlantState::Live
    }

    pub fn allow_modify(&self) -> bool {
        self.state == PlantState::Live
    }

    /// Cancels remain available during resync (risk-reducing); blocked when
    /// down, un-armed, or latched.
    pub fn allow_cancel(&self) -> bool {
        matches!(self.state, PlantState::Live | PlantState::Resyncing)
    }

    pub fn reject_reason(&self, action: &str) -> String {
        format!("order plant {}; {action} blocked", self.state.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::PlantState::*;
    use super::*;

    #[derive(Clone, Copy, Debug)]
    enum Event {
        BeginConnect,
        Disconnect,
        Latch,
        RearmAlive,
        RearmDead,
        ResyncStart,
        ResyncComplete,
        ResyncFailed,
    }

    const EVENTS: [Event; 8] = [
        Event::BeginConnect,
        Event::Disconnect,
        Event::Latch,
        Event::RearmAlive,
        Event::RearmDead,
        Event::ResyncStart,
        Event::ResyncComplete,
        Event::ResyncFailed,
    ];

    fn apply(plant: &mut PlantPolicy, event: Event) {
        match event {
            Event::BeginConnect => plant.begin_connect(),
            Event::Disconnect => plant.disconnect(),
            Event::Latch => plant.latch(),
            Event::RearmAlive => {
                plant.rearm(true);
            }
            Event::RearmDead => {
                plant.rearm(false);
            }
            Event::ResyncStart => plant.resync_start(),
            Event::ResyncComplete => plant.resync_complete(),
            Event::ResyncFailed => plant.resync_failed(),
        }
    }

    /// The executable spec of the lifecycle, written as data and independent of
    /// the implementation (ported from the Python transition table).
    fn expected(state: PlantState, event: Event) -> PlantState {
        match (state, event) {
            // begin_connect: any state enters the barrier.
            (_, Event::BeginConnect) => Connecting,
            // disconnect: transport down / not armed.
            (_, Event::Disconnect) => Disconnected,
            // latch: an anomaly that needs a recon cycle.
            (_, Event::Latch) => Latched,
            // rearm: the ONLY arming transition — Connecting + live poll task.
            (Connecting, Event::RearmAlive) => Live,
            (Latched, Event::RearmAlive | Event::RearmDead) => Latched,
            (_, Event::RearmAlive | Event::RearmDead) => Disconnected,
            // resync_start
            (Live | Resyncing, Event::ResyncStart) => Resyncing,
            (Connecting | Latched, Event::ResyncStart) => Latched,
            (Disconnected, Event::ResyncStart) => Disconnected,
            // resync_complete: only Resyncing arms; the barrier is untouched.
            (Resyncing, Event::ResyncComplete) => Live,
            (s, Event::ResyncComplete) => s,
            // resync_failed: transport down; a latched plant stays latched.
            (Latched, Event::ResyncFailed) => Latched,
            (_, Event::ResyncFailed) => Disconnected,
        }
    }

    #[test]
    fn every_state_event_pair_matches_the_table() {
        for state in PlantState::ALL {
            for event in EVENTS {
                let mut plant = PlantPolicy::new(state);
                apply(&mut plant, event);
                assert_eq!(
                    plant.state(),
                    expected(state, event),
                    "{state:?} --{event:?}-->"
                );
            }
        }
    }

    #[test]
    fn rearm_reports_whether_the_plant_armed() {
        for state in PlantState::ALL {
            for alive in [true, false] {
                let mut plant = PlantPolicy::new(state);
                assert_eq!(plant.rearm(alive), state == Connecting && alive);
            }
        }
    }

    #[test]
    fn the_latch_bit_follows_state() {
        for state in PlantState::ALL {
            assert_eq!(PlantPolicy::new(state).latched(), state == Latched);
        }
        let mut plant = PlantPolicy::new(Latched);
        plant.begin_connect();
        assert!(
            !plant.latched(),
            "the barrier is recon in progress, not blocked"
        );
        assert!(plant.rearm(true));
        assert_eq!(plant.state(), Live);
        assert!(!plant.latched());
    }

    #[test]
    fn policy_is_a_pure_function_of_state() {
        for state in PlantState::ALL {
            let plant = PlantPolicy::new(state);
            assert_eq!(plant.allow_submit(), state == Live);
            assert_eq!(plant.allow_modify(), state == Live);
            assert_eq!(plant.allow_cancel(), matches!(state, Live | Resyncing));
        }
    }

    #[test]
    fn a_newer_latch_survives_the_barrier() {
        let mut plant = PlantPolicy::default();
        plant.begin_connect();
        plant.latch();
        assert!(!plant.rearm(true));
        assert_eq!(plant.state(), Latched);
    }

    #[test]
    fn a_latch_survives_every_resync_step() {
        let mut plant = PlantPolicy::new(Latched);
        plant.resync_start();
        plant.resync_complete();
        plant.resync_failed();
        assert_eq!(plant.state(), Latched);
    }

    #[test]
    fn a_down_plant_cannot_rearm_via_resync() {
        let mut plant = PlantPolicy::new(Live);
        plant.resync_start();
        plant.resync_failed();
        assert_eq!(plant.state(), Disconnected);
        plant.resync_start();
        plant.resync_complete();
        assert_eq!(plant.state(), Disconnected);
        assert!(!plant.allow_submit());
    }

    #[test]
    fn a_mid_barrier_resync_cannot_arm() {
        let mut plant = PlantPolicy::default();
        plant.begin_connect();
        plant.resync_start();
        assert_eq!(plant.state(), Latched);
        plant.resync_complete();
        assert_eq!(plant.state(), Latched);
        assert!(!plant.allow_submit() && !plant.allow_modify() && !plant.allow_cancel());
        assert!(!plant.rearm(true));
        assert_eq!(plant.state(), Latched);
    }

    #[test]
    fn state_names_round_trip_and_reasons_use_them() {
        for state in PlantState::ALL {
            assert_eq!(PlantState::parse(state.as_str()), Some(state));
        }
        assert_eq!(PlantState::parse("armed"), None);
        assert_eq!(
            PlantPolicy::new(Latched).reject_reason("submit"),
            "order plant latched; submit blocked"
        );
    }
}
