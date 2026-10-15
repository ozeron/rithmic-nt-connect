//! Credential flock shared by both composition roots. No environment reads.
use crate::HostFingerprint;
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use thiserror::Error;

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

#[derive(Debug)]
pub struct SessionLock {
    _file: File,
    path: PathBuf,
}
impl SessionLock {
    pub fn try_acquire_in(dir: &Path, key: &HostFingerprint) -> Result<Self, SingletonError> {
        let hash = credential_key_hash(&key.user, &key.system_name, &key.url, &key.env);
        Self::try_acquire_path(dir.join(format!("rgw-{hash}.lock")))
    }
    pub fn try_acquire_path(path: PathBuf) -> Result<Self, SingletonError> {
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
            // Never truncate here: a contender that loses the lock below must not
            // wipe the holder's pid. The winner truncates once it holds it.
            .truncate(false)
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
/// FNV-1a over the stable cross-process credential identity. Never includes a password.
pub fn credential_key_hash(user: &str, system_name: &str, url: &str, env: &str) -> u64 {
    let env = match env.to_ascii_lowercase().as_str() {
        "demo" | "development" => "demo",
        "test" => "test",
        _ => "live",
    };
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in format!("{user}|{system_name}|{url}|{env}").bytes() {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Match the gateway and lake client's historical socket-sibling lock path.
pub fn credential_lock_path(dir: &Path, user: &str, system: &str, url: &str, env: &str) -> PathBuf {
    let hash = credential_key_hash(user, system, url, env);
    let path = dir.join(format!("rgw-{hash}.lock"));
    if path.as_os_str().len() <= 103 {
        return path;
    }
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    let dir = PathBuf::from("/tmp").join(format!("rgw-{uid}"));
    let _ = std::fs::create_dir_all(&dir);
    let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
    dir.join(format!("{:08x}.lock", hash as u32))
}
