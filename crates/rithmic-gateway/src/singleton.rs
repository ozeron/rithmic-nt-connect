//! Credential flock so only one direct/gateway process opens Rithmic plants.
//!
//! The exclusive flock is released automatically when the holding process exits
//! (OS closes the fd), so a second process can acquire after a crash without
//! needing pid liveness heuristics.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use thiserror::Error;

use crate::runtime_dir::{canon_env, runtime_base_dir};

/// Errors acquiring the credential flock.
#[derive(Debug, Error)]
pub enum SingletonError {
    #[error("session already held by another process at {path}")]
    AlreadyHeld { path: PathBuf },
    #[error("io error on flock {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Held exclusive flock for one credential set.
#[derive(Debug)]
pub struct SessionLock {
    _file: File,
    path: PathBuf,
}

impl SessionLock {
    /// Path to the lock file for this credential fingerprint.
    pub fn lock_path(user: &str, system_name: &str, url: &str, env: &str) -> PathBuf {
        let base = runtime_base_dir();
        let hash = credential_key_hash(user, system_name, url, env);
        let path = base.join(format!("rgw-{hash}.lock"));
        crate::runtime_dir::clamp_lock_path(path, hash)
    }

    /// Try to acquire exclusive flock. Fails if another live process holds it.
    pub fn try_acquire(
        user: &str,
        system_name: &str,
        url: &str,
        env: &str,
    ) -> Result<Self, SingletonError> {
        let path = Self::lock_path(user, system_name, url, env);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| SingletonError::Io {
                path: path.clone(),
                source,
            })?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|source| SingletonError::Io {
                path: path.clone(),
                source,
            })?;

        let mut perms = file
            .metadata()
            .map(|m| m.permissions())
            .unwrap_or_else(|_| std::fs::Permissions::from_mode(0o600));
        perms.set_mode(0o600);
        let _ = std::fs::set_permissions(&path, perms);

        match file.try_lock_exclusive() {
            Ok(()) => {
                let pid = std::process::id();
                file.set_len(0).ok();
                write!(file, "{pid}").map_err(|source| SingletonError::Io {
                    path: path.clone(),
                    source,
                })?;
                file.sync_all().ok();
                Ok(Self { _file: file, path })
            }
            Err(_) => Err(SingletonError::AlreadyHeld { path }),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn simple_hash(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// FNV-1a over `user|system_name|url|canon_env` — shared by lock and unix listen paths.
pub(crate) fn credential_key_hash(user: &str, system_name: &str, url: &str, env: &str) -> u64 {
    simple_hash(&format!(
        "{user}|{system_name}|{url}|{}",
        canon_env(env)
    ))
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
    fn live_and_demo_do_not_share_lock_path() {
        let live = SessionLock::lock_path("u", "LucidTrading", "wss://x", "Live");
        let demo = SessionLock::lock_path("u", "LucidTrading", "wss://x", "Demo");
        assert_ne!(live, demo);
    }
}
