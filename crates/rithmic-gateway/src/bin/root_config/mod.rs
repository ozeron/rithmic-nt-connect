use rithmic_host::{ParentGates, Timing};
use std::time::Duration;
fn truthy(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}
pub fn gates() -> ParentGates {
    ParentGates {
        trading_enabled: truthy("RITHMIC_ENABLE_TRADING"),
        cancel_all_enabled: truthy("RITHMIC_GATEWAY_CANCEL_ALL"),
    }
}
pub fn timing() -> Timing {
    let mut timing = Timing::default();
    if let Some(ms) = std::env::var("RITHMIC_GATEWAY_RESTORE_BACKOFF_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
    {
        timing.restore_backoff = Duration::from_millis(ms);
    }
    timing
}
