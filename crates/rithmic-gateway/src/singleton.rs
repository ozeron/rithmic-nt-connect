//! Credential flock so only one direct/gateway process opens Rithmic plants.
//!
//! The exclusive flock is released automatically when the holding process exits
//! (OS closes the fd), so a second process can acquire after a crash without
//! needing pid liveness heuristics.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use fs2::FileExt;

use crate::runtime_dir::runtime_base_dir;
pub use rithmic_host::flock::{credential_key_hash, SingletonError};

/// Root-resolved lock location; the host owns the actual flock implementation.
#[derive(Debug)]
pub struct SessionLock(rithmic_host::SessionLock);

impl SessionLock {
    /// Path to the lock file for this credential fingerprint.
    pub fn lock_path(user: &str, system_name: &str, url: &str, env: &str) -> PathBuf {
        rithmic_host::flock::credential_lock_path(&runtime_base_dir(), user, system_name, url, env)
    }

    /// Try to acquire exclusive flock. Fails if another live process holds it.
    pub fn try_acquire(
        user: &str,
        system_name: &str,
        url: &str,
        env: &str,
    ) -> Result<Self, SingletonError> {
        rithmic_host::SessionLock::try_acquire_path(Self::lock_path(user, system_name, url, env))
            .map(Self)
    }

    pub fn path(&self) -> &Path {
        self.0.path()
    }

    /// Like [`Self::try_acquire`], but retries `AlreadyHeld` for up to `grace`.
    ///
    /// Clients probe the flock (see [`flock_held`]) by briefly taking it, so a
    /// parent that starts at that instant would otherwise refuse to start. A
    /// lock that stays held past `grace` is a real second process and still fails.
    pub fn try_acquire_patient(
        user: &str,
        system_name: &str,
        url: &str,
        env: &str,
        grace: std::time::Duration,
    ) -> Result<Self, SingletonError> {
        let deadline = std::time::Instant::now() + grace;
        loop {
            match Self::try_acquire(user, system_name, url, env) {
                Err(SingletonError::AlreadyHeld { .. }) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                other => return other,
            }
        }
    }

    /// [`Self::try_acquire_patient`] for async startup: the retry sleeps run on a
    /// blocking thread instead of stalling an executor worker.
    pub async fn acquire_patient(
        user: &str,
        system_name: &str,
        url: &str,
        env: &str,
        grace: std::time::Duration,
    ) -> Result<Self, SingletonError> {
        let (user, system_name, url, env) = (
            user.to_string(),
            system_name.to_string(),
            url.to_string(),
            env.to_string(),
        );
        tokio::task::spawn_blocking(move || {
            Self::try_acquire_patient(&user, &system_name, &url, &env, grace)
        })
        .await
        .expect("flock acquisition task panicked")
    }
}

/// How long a parent waits out a transient flock probe before refusing to start.
pub const STARTUP_LOCK_GRACE: std::time::Duration = std::time::Duration::from_millis(250);

/// True when another live process holds the credential flock.
///
/// Probes without taking ownership: it never truncates the lock file or writes
/// a pid (unlike [`SessionLock::try_acquire`]), so a client can attest that a
/// parent is alive without disturbing it.
pub fn flock_held(user: &str, system_name: &str, url: &str, env: &str) -> bool {
    let path = SessionLock::lock_path(user, system_name, url, env);
    let Ok(file) = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
    else {
        return false;
    };
    match file.try_lock_exclusive() {
        Ok(()) => {
            let _ = fs2::FileExt::unlock(&file);
            false
        }
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_acquire_fails_until_drop() {
        let user = format!("test-user-{}", std::process::id());
        let system = "LucidTrading";
        let url = "wss://rprotocol.rithmic.com:443";
        let env = "Live";
        let a = SessionLock::try_acquire(&user, system, url, env).expect("first lock");
        let err = SessionLock::try_acquire(&user, system, url, env).unwrap_err();
        assert!(matches!(err, SingletonError::AlreadyHeld { .. }));
        drop(a);
        let _b = SessionLock::try_acquire(&user, system, url, env).expect("after drop");
    }

    #[test]
    fn patient_acquire_rides_out_a_transient_probe_but_not_a_real_holder() {
        let user = format!("patient-user-{}", std::process::id());
        let (system, url, env) = ("LucidTrading", "wss://patient.example", "Live");
        let grace = std::time::Duration::from_millis(500);

        // A holder that lets go after 60ms (a probe): the patient acquire waits.
        let holder = SessionLock::try_acquire(&user, system, url, env).expect("holder");
        let release = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(60));
            drop(holder);
        });
        let started = std::time::Instant::now();
        let lock = SessionLock::try_acquire_patient(&user, system, url, env, grace)
            .expect("acquired after the probe released it");
        assert!(started.elapsed() >= std::time::Duration::from_millis(50));
        release.join().unwrap();

        // A real holder never lets go: it fails, but only after the grace.
        let started = std::time::Instant::now();
        let err = SessionLock::try_acquire_patient(
            &user,
            system,
            url,
            env,
            std::time::Duration::from_millis(100),
        )
        .unwrap_err();
        assert!(matches!(err, SingletonError::AlreadyHeld { .. }));
        assert!(started.elapsed() >= std::time::Duration::from_millis(90));
        drop(lock);
    }

    #[test]
    fn flock_held_probes_without_taking_the_lock() {
        let user = format!("probe-user-{}", std::process::id());
        let (system, url, env) = ("LucidTrading", "wss://probe.example", "Live");
        assert!(!flock_held(&user, system, url, env), "free before acquire");
        // Probing must leave the lock acquirable.
        let lock = SessionLock::try_acquire(&user, system, url, env).expect("acquire");
        assert!(flock_held(&user, system, url, env), "held while owned");
        let pid = std::fs::read_to_string(lock.path()).expect("pid file");
        assert_eq!(
            pid,
            std::process::id().to_string(),
            "probe must not rewrite the pid"
        );
        drop(lock);
        assert!(!flock_held(&user, system, url, env), "free after release");
    }

    #[test]
    fn a_losing_contender_leaves_the_holders_pid() {
        let user = format!("contender-user-{}", std::process::id());
        let (system, url, env) = ("LucidTrading", "wss://contender.example", "Live");
        let lock = SessionLock::try_acquire(&user, system, url, env).expect("acquire");
        let err = SessionLock::try_acquire(&user, system, url, env).unwrap_err();
        assert!(matches!(err, SingletonError::AlreadyHeld { .. }));
        let pid = std::fs::read_to_string(lock.path()).expect("pid file");
        assert_eq!(
            pid,
            std::process::id().to_string(),
            "contender wiped the pid"
        );
        drop(lock);
    }

    #[test]
    fn live_and_demo_do_not_share_lock_path() {
        let live = SessionLock::lock_path("u", "LucidTrading", "wss://x", "Live");
        let demo = SessionLock::lock_path("u", "LucidTrading", "wss://x", "Demo");
        assert_ne!(live, demo);
    }
}
