//! Integration tests: credential flock exclusivity.

use rithmic_gateway::singleton::{SessionLock, SingletonError};

#[test]
fn second_process_style_acquire_fails() {
    let user = format!("itest-{}", std::process::id());
    let system = "LucidTrading";
    let url = "wss://rprotocol.rithmic.com:443";
    let first = SessionLock::try_acquire(&user, system, url).expect("first");
    match SessionLock::try_acquire(&user, system, url) {
        Err(SingletonError::AlreadyHeld { .. }) => {}
        other => panic!("expected AlreadyHeld, got {other:?}"),
    }
    drop(first);
    let _second = SessionLock::try_acquire(&user, system, url).expect("after release");
}
