//! Retry held market-data intents without consumer replay or another rebuild.
use std::collections::{hash_map::RandomState, HashMap};
use std::hash::BuildHasher;
use std::time::Duration;

use tokio::time::Instant;

use crate::host::HostState;
use crate::intents::MdIntent;
use crate::local::subscriptions::{rejoin_degraded, TopicIntent};
use crate::Venue;

struct Retry {
    at: Instant,
    backoff: Duration,
    attempts: u32,
}

impl Retry {
    fn new() -> Self {
        let backoff = Duration::from_secs(1);
        Self {
            at: Instant::now() + backoff,
            backoff,
            attempts: 0,
        }
    }

    fn failed(&mut self, intent: &MdIntent, jitter: &RandomState) {
        self.backoff = (self.backoff * 2).min(Duration::from_secs(60));
        self.attempts = self.attempts.saturating_add(1);
        // 80-100% jitter, seeded per pump, keeps the maximum pause at 60s.
        let permille = 800 + jitter.hash_one((intent, self.attempts)) % 201;
        self.at = Instant::now() + self.backoff.mul_f64(permille as f64 / 1000.0);
    }
}

pub(super) async fn retry<V: Venue + ?Sized + 'static>(state: &HostState<V>) {
    let mut pending = HashMap::<MdIntent, Retry>::new();
    let jitter = RandomState::new();
    loop {
        let changed = state.reconnect.degraded_changed.notified();
        let held = state.reconnect.degraded_intents().await;
        pending.retain(|intent, _| held.contains(intent));
        for intent in held {
            pending.entry(intent).or_insert_with(Retry::new);
        }
        let Some(next) = pending.values().map(|retry| retry.at).min() else {
            changed.await;
            continue;
        };
        tokio::select! {
            _ = changed => continue,
            _ = tokio::time::sleep_until(next) => {}
        }
        let due: Vec<_> = pending
            .iter()
            .filter(|(_, retry)| retry.at <= Instant::now())
            .map(|(intent, _)| intent.clone())
            .collect();
        for intent in due {
            // The shared join path holds the topic lock and rechecks degradation.
            // A detach or a successful consumer retry can never be resurrected.
            match rejoin_degraded(state, TopicIntent::from_md(&intent)).await {
                Ok(()) => {
                    pending.remove(&intent);
                }
                Err(_) => {
                    pending.get_mut(&intent).unwrap().failed(&intent, &jitter);
                }
            }
        }
    }
}
