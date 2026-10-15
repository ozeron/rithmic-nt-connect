//! Parent Rithmic reconnect + typed subscription intent restore.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::fanout::{FanoutHub, SubKey};

/// Live time-bar join remembered across plant reconnect.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TimeBarIntent {
    pub symbol: String,
    pub exchange: String,
    pub bar_type: i32,
    pub period: i32,
}

/// A market-data join, as a hashable key (what a restore re-issues and what can
/// be left degraded).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MdIntent {
    Ticker(SubKey),
    Book(SubKey),
    TimeBars(TimeBarIntent),
}

/// Venue joins that must be re-issued after plants reconnect.
#[derive(Debug, Clone, Default)]
pub struct RestorePlan {
    pub ticker: Vec<SubKey>,
    pub book: Vec<SubKey>,
    pub time_bars: Vec<TimeBarIntent>,
    pub pnl: bool,
    pub order: bool,
    pub brackets: bool,
}

/// Tracks typed venue intent (refcount) separate from hub fan-out interest.
#[derive(Default)]
pub struct IntentStore {
    ticker: HashMap<SubKey, usize>,
    book: HashMap<SubKey, usize>,
    time_bars: HashMap<TimeBarIntent, usize>,
    pnl: usize,
    order: usize,
    brackets: usize,
    /// Market-data joins the venue lost in a plant rebuild and a restore could not
    /// re-establish (for example an expired contract). The intent stays noted for
    /// every client that holds it; the next subscribe from one of them retries the
    /// venue join and clears this on success.
    degraded: HashSet<MdIntent>,
}

/// Count one more holder of `key`; `true` on the first (0→1).
fn bump<K: Hash + Eq>(map: &mut HashMap<K, usize>, key: K) -> bool {
    let e = map.entry(key).or_insert(0);
    *e += 1;
    *e == 1
}

/// Count one holder of `key` less; `true` when it was the last (1→0).
fn drop_count<K: Hash + Eq>(map: &mut HashMap<K, usize>, key: &K) -> bool {
    let Some(e) = map.get_mut(key) else {
        return false;
    };
    *e = e.saturating_sub(1);
    if *e == 0 {
        map.remove(key);
        return true;
    }
    false
}

fn bump_flag(n: &mut usize) -> bool {
    *n += 1;
    *n == 1
}

fn drop_flag(n: &mut usize) -> bool {
    if *n == 0 {
        return false;
    }
    *n -= 1;
    *n == 0
}

impl IntentStore {
    pub fn note_ticker(&mut self, key: SubKey) -> bool {
        bump(&mut self.ticker, key)
    }

    pub fn forget_ticker(&mut self, key: &SubKey) -> bool {
        let last = drop_count(&mut self.ticker, key);
        if last {
            self.degraded.remove(&MdIntent::Ticker(key.clone()));
        }
        last
    }

    pub fn note_book(&mut self, key: SubKey) -> bool {
        bump(&mut self.book, key)
    }

    pub fn forget_book(&mut self, key: &SubKey) -> bool {
        let last = drop_count(&mut self.book, key);
        if last {
            self.degraded.remove(&MdIntent::Book(key.clone()));
        }
        last
    }

    pub fn note_time_bar(&mut self, intent: TimeBarIntent) -> bool {
        bump(&mut self.time_bars, intent)
    }

    pub fn forget_time_bar(&mut self, intent: &TimeBarIntent) -> bool {
        let last = drop_count(&mut self.time_bars, intent);
        if last {
            self.degraded.remove(&MdIntent::TimeBars(intent.clone()));
        }
        last
    }

    pub fn note_pnl(&mut self) -> bool {
        bump_flag(&mut self.pnl)
    }

    pub fn forget_pnl(&mut self) -> bool {
        drop_flag(&mut self.pnl)
    }

    pub fn note_order(&mut self) -> bool {
        bump_flag(&mut self.order)
    }

    pub fn forget_order(&mut self) -> bool {
        drop_flag(&mut self.order)
    }

    pub fn note_brackets(&mut self) -> bool {
        bump_flag(&mut self.brackets)
    }

    pub fn forget_brackets(&mut self) -> bool {
        drop_flag(&mut self.brackets)
    }

    pub fn restore_plan(&self) -> RestorePlan {
        RestorePlan {
            ticker: self.ticker.keys().cloned().collect(),
            book: self.book.keys().cloned().collect(),
            time_bars: self.time_bars.keys().cloned().collect(),
            pnl: self.pnl > 0,
            order: self.order > 0,
            brackets: self.brackets > 0,
        }
    }

    /// Mark a join degraded. A no-op when no client holds the intent any more (the
    /// last holder left while the restore was retrying): a stale flag would make a
    /// later, unrelated subscribe do a spurious second venue join.
    pub fn mark_degraded(&mut self, intent: MdIntent) {
        let held = match &intent {
            MdIntent::Ticker(k) => self.ticker.contains_key(k),
            MdIntent::Book(k) => self.book.contains_key(k),
            MdIntent::TimeBars(t) => self.time_bars.contains_key(t),
        };
        if held {
            self.degraded.insert(intent);
        }
    }

    pub fn clear_degraded(&mut self, intent: &MdIntent) -> bool {
        self.degraded.remove(intent)
    }

    pub fn is_degraded(&self, intent: &MdIntent) -> bool {
        self.degraded.contains(intent)
    }

    pub fn degraded_count(&self) -> usize {
        self.degraded.len()
    }

    pub fn remembered_count(&self) -> usize {
        self.ticker.len()
            + self.book.len()
            + self.time_bars.len()
            + usize::from(self.pnl > 0)
            + usize::from(self.order > 0)
            + usize::from(self.brackets > 0)
    }

    /// Continuous MD joins that need the event pump (ticker / book / time bars).
    /// History `Load*` must not run while any of these are live — the session
    /// mutex would starve the pump (2026-08-25 MY043 RC2.3).
    pub fn has_live_md_intents(&self) -> bool {
        !self.ticker.is_empty() || !self.book.is_empty() || !self.time_bars.is_empty()
    }
}

/// `IntentStore` methods, each taken under the controller's lock.
macro_rules! locked {
    ($($(#[$doc:meta])* fn $name:ident($($arg:ident: $ty:ty),*) -> $ret:ty;)*) => {
        $(
            $(#[$doc])*
            pub async fn $name(&self, $($arg: $ty),*) -> $ret {
                self.intent.lock().await.$name($($arg),*)
            }
        )*
    };
}

/// Reconnect controller — typed intent + hub fan-out bookkeeping.
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

    /// Bump hub fan-out interest. Returns `true` on 0→1 (topic created).
    pub async fn add_hub_interest(&self, key: SubKey) -> bool {
        self.hub.add_interest(key).await
    }

    /// Drop hub fan-out interest. Returns `true` when topic is gone (→0).
    pub async fn remove_hub_interest(&self, key: &SubKey) -> bool {
        self.hub.remove_interest(key).await
    }

    locked! {
        fn note_ticker(key: SubKey) -> bool;
        fn forget_ticker(key: &SubKey) -> bool;
        fn note_book(key: SubKey) -> bool;
        fn forget_book(key: &SubKey) -> bool;
        fn note_time_bar(intent: TimeBarIntent) -> bool;
        fn forget_time_bar(intent: &TimeBarIntent) -> bool;
        fn note_pnl() -> bool;
        fn forget_pnl() -> bool;
        fn note_order() -> bool;
        fn forget_order() -> bool;
        fn note_brackets() -> bool;
        fn forget_brackets() -> bool;
        fn mark_degraded(intent: MdIntent) -> ();
        fn clear_degraded(intent: &MdIntent) -> bool;
        fn is_degraded(intent: &MdIntent) -> bool;
        /// Market-data joins currently left degraded after a rebuild.
        fn degraded_count() -> usize;
        fn remembered_count() -> usize;
        /// True when ticker, book, or live time-bar intents are present.
        fn has_live_md_intents() -> bool;
    }

    /// After plants are back, return every typed venue join to re-issue.
    ///
    /// Does **not** touch hub refcounts: live clients keep fan-out interest
    /// across a plant drop.
    pub async fn restore_plan(&self) -> RestorePlan {
        self.intent.lock().await.restore_plan()
    }

    /// Intent counts for GetLiveMdState (ticker, book, time_bars).
    pub async fn live_md_intent_counts(&self) -> (u32, u32, u32) {
        let g = self.intent.lock().await;
        (
            g.ticker.len() as u32,
            g.book.len() as u32,
            g.time_bars.len() as u32,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn restore_replays_typed_intent_even_when_hub_intact() {
        let hub = Arc::new(FanoutHub::new(16));
        let ctl = ReconnectController::new(hub.clone());
        let key = SubKey {
            symbol: "NQ".into(),
            exchange: "CME".into(),
        };
        assert!(ctl.note_ticker(key.clone()).await);
        assert!(ctl.add_hub_interest(key.clone()).await);
        assert_eq!(ctl.remembered_count().await, 1);
        let plan = ctl.restore_plan().await;
        assert_eq!(plan.ticker.len(), 1);
        assert_eq!(plan.ticker[0], key);
        assert!(plan.book.is_empty());
        assert!(!plan.pnl);
        // Hub interest must not have been double-bumped by restore.
        assert!(!hub.add_interest(key.clone()).await);
        assert!(!hub.remove_interest(&key).await);
        assert!(hub.remove_interest(&key).await);
    }

    #[tokio::test]
    async fn degraded_marks_only_apply_to_held_intents() {
        let ctl = ReconnectController::new(Arc::new(FanoutHub::new(16)));
        let key = SubKey {
            symbol: "NQ".into(),
            exchange: "CME".into(),
        };
        let intent = MdIntent::Ticker(key.clone());
        // Everyone left while the restore was retrying: the late mark is dropped.
        ctl.mark_degraded(intent.clone()).await;
        assert!(!ctl.is_degraded(&intent).await);

        ctl.note_ticker(key.clone()).await;
        ctl.mark_degraded(intent.clone()).await;
        assert!(ctl.is_degraded(&intent).await);
        ctl.forget_ticker(&key).await;
        assert!(!ctl.is_degraded(&intent).await);
    }

    #[tokio::test]
    async fn restore_includes_book_bars_pnl_order() {
        let hub = Arc::new(FanoutHub::new(16));
        let ctl = ReconnectController::new(hub);
        let key = SubKey {
            symbol: "NQ".into(),
            exchange: "CME".into(),
        };
        ctl.note_book(key.clone()).await;
        ctl.note_time_bar(TimeBarIntent {
            symbol: "NQ".into(),
            exchange: "CME".into(),
            bar_type: 2,
            period: 1,
        })
        .await;
        ctl.note_pnl().await;
        ctl.note_order().await;
        ctl.note_brackets().await;
        let plan = ctl.restore_plan().await;
        assert_eq!(plan.book, vec![key]);
        assert_eq!(plan.time_bars.len(), 1);
        assert!(plan.pnl);
        assert!(plan.order);
        assert!(plan.brackets);
        assert!(plan.ticker.is_empty());
    }

    #[tokio::test]
    async fn ticker_refcount_last_clears_restore() {
        let hub = Arc::new(FanoutHub::new(16));
        let ctl = ReconnectController::new(hub);
        let key = SubKey {
            symbol: "NQ".into(),
            exchange: "CME".into(),
        };
        assert!(ctl.note_ticker(key.clone()).await);
        assert!(!ctl.note_ticker(key.clone()).await);
        assert!(!ctl.forget_ticker(&key).await);
        assert!(ctl.forget_ticker(&key).await);
        assert!(ctl.restore_plan().await.ticker.is_empty());
    }

    #[tokio::test]
    async fn has_live_md_intents_tracks_ticker_book_time_bars_not_pnl() {
        let hub = Arc::new(FanoutHub::new(16));
        let ctl = ReconnectController::new(hub);
        assert!(!ctl.has_live_md_intents().await);
        ctl.note_pnl().await;
        assert!(
            !ctl.has_live_md_intents().await,
            "pnl alone is not continuous MD for history refuse"
        );
        let key = SubKey {
            symbol: "MNQ".into(),
            exchange: "CME".into(),
        };
        ctl.note_ticker(key.clone()).await;
        assert!(ctl.has_live_md_intents().await);
        ctl.forget_ticker(&key).await;
        assert!(!ctl.has_live_md_intents().await);
        ctl.note_book(key.clone()).await;
        assert!(ctl.has_live_md_intents().await);
        ctl.forget_book(&key).await;
        ctl.note_time_bar(TimeBarIntent {
            symbol: "MNQ".into(),
            exchange: "CME".into(),
            bar_type: 2,
            period: 1,
        })
        .await;
        assert!(ctl.has_live_md_intents().await);
    }
}
