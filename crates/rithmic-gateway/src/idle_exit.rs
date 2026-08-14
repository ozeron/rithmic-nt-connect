//! Idle-exit after the last Ready client leaves (optional grace).

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, Notify};
use tokio::time::{Instant, sleep_until};

/// How long the parent stays up after peer count hits zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleExitPolicy {
    /// Stay until SIGTERM / process kill (standalone parents).
    Never,
    /// Exit after this grace once peers reach zero (`Duration::ZERO` = immediate).
    After(Duration),
}

/// Parse `RITHMIC_GATEWAY_IDLE_EXIT_SEC`.
///
/// - unset / empty → [`IdleExitPolicy::Never`]
/// - `N` ≥ 0 → grace of `N` seconds (`0` = immediate)
/// - negative → [`IdleExitPolicy::Never`]
pub fn parse_idle_exit_sec(raw: Option<&str>) -> Result<IdleExitPolicy, String> {
    let Some(s) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(IdleExitPolicy::Never);
    };
    let n: i64 = s
        .parse()
        .map_err(|_| format!("RITHMIC_GATEWAY_IDLE_EXIT_SEC must be an integer, got {s:?}"))?;
    if n < 0 {
        Ok(IdleExitPolicy::Never)
    } else {
        Ok(IdleExitPolicy::After(Duration::from_secs(n as u64)))
    }
}

struct IdleExitInner {
    peers: usize,
    /// Absolute deadline when armed; `None` when not waiting to exit.
    armed_deadline: Option<Instant>,
}

/// Tracks Ready peer count and arms/cancels idle-exit.
pub struct IdleExit {
    policy: IdleExitPolicy,
    inner: Mutex<IdleExitInner>,
    notify: Notify,
    /// Set when idle-exit is claimed; new Handshakes must not attach.
    shutting_down: AtomicBool,
}

impl IdleExit {
    pub fn new(policy: IdleExitPolicy) -> Self {
        Self {
            policy,
            inner: Mutex::new(IdleExitInner {
                peers: 0,
                armed_deadline: None,
            }),
            notify: Notify::new(),
            shutting_down: AtomicBool::new(false),
        }
    }

    pub fn policy(&self) -> IdleExitPolicy {
        self.policy
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    /// After Handshake→Ready: cancel idle arm and bump peer count.
    /// Returns `false` if the parent is shutting down (caller must close).
    pub async fn peer_attached(&self) -> bool {
        if self.shutting_down.load(Ordering::SeqCst) {
            return false;
        }
        let mut g = self.inner.lock().await;
        if self.shutting_down.load(Ordering::SeqCst) {
            return false;
        }
        g.peers = g.peers.saturating_add(1);
        g.armed_deadline = None;
        drop(g);
        self.notify.notify_waiters();
        true
    }

    /// After client teardown: drop peer count; arm idle when last peer leaves.
    pub async fn peer_detached(&self) {
        let mut g = self.inner.lock().await;
        g.peers = g.peers.saturating_sub(1);
        if g.peers == 0 {
            if let IdleExitPolicy::After(grace) = self.policy {
                g.armed_deadline = Some(Instant::now() + grace);
            }
        }
        drop(g);
        self.notify.notify_waiters();
    }

    /// Resolves when the parent should stop accepting and exit.
    ///
    /// Never returns when policy is [`IdleExitPolicy::Never`].
    pub async fn wait_until_should_exit(&self) {
        if matches!(self.policy, IdleExitPolicy::Never) {
            std::future::pending::<()>().await;
            return;
        }
        loop {
            let deadline = {
                let g = self.inner.lock().await;
                g.armed_deadline
            };
            match deadline {
                None => {
                    self.notify.notified().await;
                }
                Some(deadline) => {
                    let now = Instant::now();
                    if now < deadline {
                        tokio::select! {
                            _ = sleep_until(deadline) => {}
                            _ = self.notify.notified() => {}
                        }
                        continue;
                    }
                    // Deadline reached — claim exit under the lock.
                    let mut g = self.inner.lock().await;
                    if g.peers != 0 {
                        g.armed_deadline = None;
                        continue;
                    }
                    match g.armed_deadline {
                        Some(d) if d <= Instant::now() => {
                            g.armed_deadline = None;
                            self.shutting_down.store(true, Ordering::SeqCst);
                            // Final peer check after flag (attach races).
                            if g.peers != 0 {
                                self.shutting_down.store(false, Ordering::SeqCst);
                                continue;
                            }
                            return;
                        }
                        _ => continue,
                    }
                }
            }
        }
    }

    /// Test helper: current Ready peer count.
    #[cfg(test)]
    pub async fn peer_count_for_test(&self) -> usize {
        self.inner.lock().await.peers
    }

    /// Test helper: whether an idle deadline is armed.
    #[cfg(test)]
    pub async fn is_armed_for_test(&self) -> bool {
        self.inner.lock().await.armed_deadline.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_idle_exit_matrix() {
        assert_eq!(parse_idle_exit_sec(None).unwrap(), IdleExitPolicy::Never);
        assert_eq!(parse_idle_exit_sec(Some("")).unwrap(), IdleExitPolicy::Never);
        assert_eq!(parse_idle_exit_sec(Some("  ")).unwrap(), IdleExitPolicy::Never);
        assert_eq!(parse_idle_exit_sec(Some("-1")).unwrap(), IdleExitPolicy::Never);
        assert_eq!(
            parse_idle_exit_sec(Some("0")).unwrap(),
            IdleExitPolicy::After(Duration::ZERO)
        );
        assert_eq!(
            parse_idle_exit_sec(Some("5")).unwrap(),
            IdleExitPolicy::After(Duration::from_secs(5))
        );
        assert!(parse_idle_exit_sec(Some("nope")).is_err());
    }

    #[tokio::test]
    async fn attach_cancels_arm() {
        let idle = IdleExit::new(IdleExitPolicy::After(Duration::from_secs(30)));
        assert!(idle.peer_attached().await);
        idle.peer_detached().await;
        assert!(idle.is_armed_for_test().await);
        assert_eq!(idle.peer_count_for_test().await, 0);
        assert!(idle.peer_attached().await);
        assert!(!idle.is_armed_for_test().await);
        assert_eq!(idle.peer_count_for_test().await, 1);
    }

    #[tokio::test]
    async fn never_policy_never_arms() {
        let idle = IdleExit::new(IdleExitPolicy::Never);
        assert!(idle.peer_attached().await);
        idle.peer_detached().await;
        assert!(!idle.is_armed_for_test().await);
    }

    #[tokio::test(start_paused = true)]
    async fn grace_zero_exits_promptly() {
        let idle = IdleExit::new(IdleExitPolicy::After(Duration::ZERO));
        assert!(idle.peer_attached().await);
        idle.peer_detached().await;
        tokio::time::timeout(Duration::from_millis(50), idle.wait_until_should_exit())
            .await
            .expect("idle-exit should fire immediately for grace 0");
        assert!(idle.is_shutting_down());
    }

    #[tokio::test(start_paused = true)]
    async fn grace_waits_then_exits() {
        let idle = IdleExit::new(IdleExitPolicy::After(Duration::from_secs(5)));
        assert!(idle.peer_attached().await);
        idle.peer_detached().await;
        let wait = idle.wait_until_should_exit();
        tokio::pin!(wait);
        tokio::select! {
            biased;
            _ = &mut wait => panic!("exited before grace"),
            _ = tokio::time::advance(Duration::from_secs(4)) => {}
        }
        // Still armed; advance past grace.
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::timeout(Duration::from_millis(50), wait)
            .await
            .expect("idle-exit after grace");
        assert!(idle.is_shutting_down());
    }

    #[tokio::test(start_paused = true)]
    async fn reattach_during_grace_prevents_exit() {
        let idle = IdleExit::new(IdleExitPolicy::After(Duration::from_secs(5)));
        assert!(idle.peer_attached().await);
        idle.peer_detached().await;
        let wait = idle.wait_until_should_exit();
        tokio::pin!(wait);
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(idle.peer_attached().await);
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::select! {
            biased;
            _ = &mut wait => panic!("must not exit while peer attached"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        assert!(!idle.is_shutting_down());
    }
}
