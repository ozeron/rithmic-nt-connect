//! Integration tests: reconnect intent restore.

use std::sync::Arc;

use rithmic_gateway::reconnect::{ReconnectController, TimeBarIntent};
use rithmic_gateway::subscriptions::{FanoutHub, SubKey};

#[tokio::test]
async fn restore_after_drop_replays_keys() {
    let hub = Arc::new(FanoutHub::new(16));
    let ctl = ReconnectController::new(hub.clone());
    let a = SubKey {
        symbol: "NQ".into(),
        exchange: "CME".into(),
    };
    let b = SubKey {
        symbol: "ES".into(),
        exchange: "CME".into(),
    };
    ctl.note_ticker(a.clone()).await;
    ctl.note_ticker(b.clone()).await;
    ctl.note_book(a.clone()).await;
    ctl.note_time_bar(TimeBarIntent {
        symbol: "NQ".into(),
        exchange: "CME".into(),
        bar_type: 2,
        period: 1,
    })
    .await;
    ctl.note_pnl().await;
    ctl.note_order().await;
    assert_eq!(ctl.remembered_count().await, 6);

    let plan = ctl.restore_plan().await;
    let mut ticker = plan.ticker;
    ticker.sort_by(|x, y| x.symbol.cmp(&y.symbol));
    assert_eq!(ticker.len(), 2);
    assert_eq!(ticker[0].symbol, "ES");
    assert_eq!(ticker[1].symbol, "NQ");
    assert_eq!(plan.book.len(), 1);
    assert_eq!(plan.time_bars.len(), 1);
    assert!(plan.pnl);
    assert!(plan.order);
}

#[tokio::test]
async fn restore_plan_covers_every_intent_channel() {
    // Documents the contract restore_intents walks: ticker, book, time_bars, pnl, order.
    // Full reconnect_loop + mock RithmicSession remains a follow-up integration.
    let hub = Arc::new(FanoutHub::new(8));
    let ctl = ReconnectController::new(hub);
    let key = SubKey {
        symbol: "MNQ".into(),
        exchange: "CME".into(),
    };
    ctl.note_ticker(key.clone()).await;
    ctl.note_book(key.clone()).await;
    ctl.note_time_bar(TimeBarIntent {
        symbol: "MNQ".into(),
        exchange: "CME".into(),
        bar_type: 2,
        period: 15,
    })
    .await;
    ctl.note_pnl().await;
    ctl.note_order().await;
    let plan = ctl.restore_plan().await;
    assert_eq!(plan.ticker.len(), 1);
    assert_eq!(plan.book.len(), 1);
    assert_eq!(plan.time_bars.len(), 1);
    assert_eq!(plan.time_bars[0].period, 15);
    assert!(plan.pnl && plan.order);
    // Clearing last peer intent empties the plan (refcount → 0).
    assert!(ctl.forget_ticker(&key).await);
    assert!(ctl.forget_book(&key).await);
    assert!(
        ctl.forget_time_bar(&TimeBarIntent {
            symbol: "MNQ".into(),
            exchange: "CME".into(),
            bar_type: 2,
            period: 15,
        })
        .await
    );
    assert!(ctl.forget_pnl().await);
    assert!(ctl.forget_order().await);
    let empty = ctl.restore_plan().await;
    assert!(empty.ticker.is_empty());
    assert!(empty.book.is_empty());
    assert!(empty.time_bars.is_empty());
    assert!(!empty.pnl && !empty.order);
}
