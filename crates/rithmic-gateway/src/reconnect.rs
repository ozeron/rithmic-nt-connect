//! Parent Rithmic reconnect + typed subscription intent restore.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::subscriptions::{FanoutHub, SubKey};

/// Live time-bar join remembered across plant reconnect.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TimeBarIntent {
    pub symbol: String,
    pub exchange: String,
    pub bar_type: i32,
    pub period: i32,
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
}

fn bump(map: &mut HashMap<SubKey, usize>, key: SubKey) -> bool {
    let e = map.entry(key).or_insert(0);
    *e += 1;
    *e == 1
}

fn drop_count(map: &mut HashMap<SubKey, usize>, key: &SubKey) -> bool {
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

fn bump_tb(map: &mut HashMap<TimeBarIntent, usize>, key: TimeBarIntent) -> bool {
    let e = map.entry(key).or_insert(0);
    *e += 1;
    *e == 1
}

fn drop_tb(map: &mut HashMap<TimeBarIntent, usize>, key: &TimeBarIntent) -> bool {
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
        drop_count(&mut self.ticker, key)
    }

    pub fn note_book(&mut self, key: SubKey) -> bool {
        bump(&mut self.book, key)
    }

    pub fn forget_book(&mut self, key: &SubKey) -> bool {
        drop_count(&mut self.book, key)
    }

    pub fn note_time_bar(&mut self, intent: TimeBarIntent) -> bool {
        bump_tb(&mut self.time_bars, intent)
    }

    pub fn forget_time_bar(&mut self, intent: &TimeBarIntent) -> bool {
        drop_tb(&mut self.time_bars, intent)
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

    pub async fn note_ticker(&self, key: SubKey) -> bool {
        self.intent.lock().await.note_ticker(key)
    }

    pub async fn forget_ticker(&self, key: &SubKey) -> bool {
        self.intent.lock().await.forget_ticker(key)
    }

    pub async fn note_book(&self, key: SubKey) -> bool {
        self.intent.lock().await.note_book(key)
    }

    pub async fn forget_book(&self, key: &SubKey) -> bool {
        self.intent.lock().await.forget_book(key)
    }

    pub async fn note_time_bar(&self, intent: TimeBarIntent) -> bool {
        self.intent.lock().await.note_time_bar(intent)
    }

    pub async fn forget_time_bar(&self, intent: &TimeBarIntent) -> bool {
        self.intent.lock().await.forget_time_bar(intent)
    }

    pub async fn note_pnl(&self) -> bool {
        self.intent.lock().await.note_pnl()
    }

    pub async fn forget_pnl(&self) -> bool {
        self.intent.lock().await.forget_pnl()
    }

    pub async fn note_order(&self) -> bool {
        self.intent.lock().await.note_order()
    }

    pub async fn forget_order(&self) -> bool {
        self.intent.lock().await.forget_order()
    }

    pub async fn note_brackets(&self) -> bool {
        self.intent.lock().await.note_brackets()
    }

    pub async fn forget_brackets(&self) -> bool {
        self.intent.lock().await.forget_brackets()
    }

    /// After plants are back, return every typed venue join to re-issue.
    ///
    /// Does **not** touch hub refcounts: live clients keep fan-out interest
    /// across a plant drop.
    pub async fn restore_plan(&self) -> RestorePlan {
        self.intent.lock().await.restore_plan()
    }

    /// Keys that need a ticker venue re-subscribe (compat helper for tests).
    pub async fn restore_after_reconnect(&self) -> Vec<SubKey> {
        self.restore_plan().await.ticker
    }

    pub async fn remembered_count(&self) -> usize {
        self.intent.lock().await.remembered_count()
    }

    /// True when ticker, book, or live time-bar intents are present.
    pub async fn has_live_md_intents(&self) -> bool {
        self.intent.lock().await.has_live_md_intents()
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

    /// Test/helper: hub + ticker intent in one call (0→1 hub when first).
    pub async fn on_subscribe(&self, key: SubKey) -> bool {
        let first_ticker = self.note_ticker(key.clone()).await;
        let _ = self.add_hub_interest(key).await;
        first_ticker
    }

    /// Test/helper: hub + ticker intent drop.
    pub async fn on_unsubscribe(&self, key: &SubKey) -> bool {
        let last_ticker = self.forget_ticker(key).await;
        let last_hub = self.remove_hub_interest(key).await;
        last_ticker || last_hub
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
