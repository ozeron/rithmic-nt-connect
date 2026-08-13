//! Integration tests: reconnect intent restore.

use std::sync::Arc;

use rithmic_gateway::reconnect::ReconnectController;
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
    ctl.on_subscribe(a.clone()).await;
    ctl.on_subscribe(b.clone()).await;
    assert_eq!(ctl.remembered_count().await, 2);

    // Simulate plant wipe of hub interest.
    assert!(hub.remove_interest(&a).await);
    assert!(hub.remove_interest(&b).await);

    let mut restored = ctl.restore_after_reconnect().await;
    restored.sort_by(|x, y| x.symbol.cmp(&y.symbol));
    assert_eq!(restored.len(), 2);
    assert_eq!(restored[0].symbol, "ES");
    assert_eq!(restored[1].symbol, "NQ");
}
