//! Credential flock so only one direct/gateway process opens Rithmic plants.
//!
//! The exclusive flock is released automatically when the holding process exits
//! (OS closes the fd), so a second process can acquire after a crash without
//! needing pid liveness heuristics.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use thiserror::Error;

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
    pub fn lock_path(user: &str, system_name: &str, url: &str) -> PathBuf {
        let base = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        let key = format!("{user}|{system_name}|{url}");
        let hash = simple_hash(&key);
        base.join(format!("rithmic-gateway-{hash}.lock"))
    }

    /// Try to acquire exclusive flock. Fails if another live process holds it.
    pub fn try_acquire(user: &str, system_name: &str, url: &str) -> Result<Self, SingletonError> {
        let path = Self::lock_path(user, system_name, url);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_acquire_fails_until_drop() {
        let user = format!("test-user-{}", std::process::id());
        let system = "LucidTrading";
        let url = "wss://rprotocol.rithmic.com:443";
        let a = SessionLock::try_acquire(&user, system, url).expect("first lock");
        let err = SessionLock::try_acquire(&user, system, url).unwrap_err();
        assert!(matches!(err, SingletonError::AlreadyHeld { .. }));
        drop(a);
        let _b = SessionLock::try_acquire(&user, system, url).expect("after drop");
    }
}
