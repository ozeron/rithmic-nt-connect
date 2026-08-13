//! Parent Rithmic reconnect + subscription intent restore.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::subscriptions::{FanoutHub, SubKey};

/// Tracks refcounted MD intent that must be restored after reconnect.
#[derive(Default)]
pub struct IntentStore {
    md: HashSet<SubKey>,
}

impl IntentStore {
    pub fn note_sub(&mut self, key: SubKey) {
        self.md.insert(key);
    }

    pub fn note_unsub(&mut self, key: &SubKey) {
        self.md.remove(key);
    }

    pub fn md_keys(&self) -> impl Iterator<Item = &SubKey> {
        self.md.iter()
    }

    pub fn clear(&mut self) {
        self.md.clear();
    }
}

/// Reconnect controller — restores fan-out interest after a synthetic plant drop.
///
/// Live `RithmicSession` reconnect is wired by the bin; this type owns the
/// intent bookkeeping and is unit-tested without the venue.
pub struct ReconnectController {
    intent: Mutex<IntentStore>,
    hub: Arc<FanoutHub>,
}

impl ReconnectController {
    pub fn new(hub: Arc<FanoutHub>) -> Self {
        Self {
            intent: Mutex::new(IntentStore::default()),
            hub,
        }
    }

    /// Record subscribe intent and bump hub refcount. Returns `true` when
    /// this was the first interest in `key` (0→1) — caller must issue the
    /// venue subscribe. Returns `false` when a peer is already subscribed.
    pub async fn on_subscribe(&self, key: SubKey) -> bool {
        self.intent.lock().await.note_sub(key.clone());
        self.hub.add_interest(key).await
    }

    /// Drop subscribe intent and decrement hub refcount. Returns `true` when
    /// this was the last interest in `key` (→0) — caller must issue the
    /// venue unsubscribe. Intent is only cleared from the restore set when
    /// the last peer detaches, so a reconnect never resurrects an interest
    /// no client asked for anymore.
    pub async fn on_unsubscribe(&self, key: &SubKey) -> bool {
        let last = self.hub.remove_interest(key).await;
        if last {
            self.intent.lock().await.note_unsub(key);
        }
        last
    }

    /// After plants are back, re-add hub interest for every remembered key.
    /// Returns keys that need a venue re-subscribe (refcount was 0).
    pub async fn restore_after_reconnect(&self) -> Vec<SubKey> {
        let intent = self.intent.lock().await;
        let mut need_venue = Vec::new();
        for key in intent.md_keys() {
            if self.hub.add_interest(key.clone()).await {
                need_venue.push(key.clone());
            }
        }
        need_venue
    }

    pub async fn remembered_count(&self) -> usize {
        self.intent.lock().await.md_keys().count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn restore_replays_intent() {
        let hub = Arc::new(FanoutHub::new(16));
        let ctl = ReconnectController::new(hub.clone());
        let key = SubKey {
            symbol: "NQ".into(),
            exchange: "CME".into(),
        };
        ctl.on_subscribe(key.clone()).await;
        assert_eq!(ctl.remembered_count().await, 1);
        // Simulate plant drop wiping hub state by removing interest until empty.
        assert!(hub.remove_interest(&key).await);
        let restored = ctl.restore_after_reconnect().await;
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0], key);
    }
}
