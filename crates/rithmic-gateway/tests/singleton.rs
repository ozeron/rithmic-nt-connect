//! Integration tests: credential flock exclusivity.

use rithmic_gateway::singleton::{SessionLock, SingletonError};

#[test]
fn second_process_style_acquire_fails() {
    let user = format!("itest-{}", std::process::id());
    let system = "LucidTrading";
    let url = "wss://rprotocol.rithmic.com:443";
    let env = "Live";
    let first = SessionLock::try_acquire(&user, system, url, env).expect("first");
    match SessionLock::try_acquire(&user, system, url, env) {
        Err(SingletonError::AlreadyHeld { .. }) => {}
        other => panic!("expected AlreadyHeld, got {other:?}"),
    }
    drop(first);
    let _second = SessionLock::try_acquire(&user, system, url, env).expect("after release");
}
