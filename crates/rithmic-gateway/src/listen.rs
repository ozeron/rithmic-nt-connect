//! Listen URL parsing and unix bind helpers.

use std::fs::OpenOptions;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use thiserror::Error;
use tokio::net::UnixListener;

/// Errors from listen URL / bind.
#[derive(Debug, Error)]
pub enum ListenError {
    #[error("unsupported listen URL scheme (v1 supports unix:// only): {0}")]
    UnsupportedScheme(String),
    #[error("invalid listen URL: {0}")]
    Invalid(String),
    #[error("tcp/tls listen is not implemented yet (see docs/references/gateway-remote.md): {0}")]
    RemoteNotImplemented(String),
    #[error("io error binding {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Parsed listen endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListenEndpoint {
    /// Absolute unix socket path.
    Unix(PathBuf),
}

impl ListenEndpoint {
    /// Parse `unix:///abs/path.sock` or `unix://rel` (resolved absolute).
    /// Rejects `tcp://` / `tls://` with a clear v2 error.
    pub fn parse(raw: &str) -> Result<Self, ListenError> {
        let raw = raw.trim();
        if raw.is_empty() {
            return Err(ListenError::Invalid("empty listen URL".into()));
        }
        if let Some(rest) = raw.strip_prefix("unix://") {
            let path = if rest.starts_with('/') {
                PathBuf::from(rest)
            } else if let Some(stripped) = rest.strip_prefix("//") {
                // unix:////tmp/x.sock → /tmp/x.sock after strip_prefix unix:// leaves //tmp
                PathBuf::from(format!("/{stripped}"))
            } else {
                // relative — make absolute from cwd
                std::env::current_dir()
                    .map_err(|source| ListenError::Io {
                        path: PathBuf::from(rest),
                        source,
                    })?
                    .join(rest)
            };
            if !path.is_absolute() {
                return Err(ListenError::Invalid(format!(
                    "unix listen path must be absolute, got {}",
                    path.display()
                )));
            }
            return Ok(Self::Unix(path));
        }
        if raw.starts_with("tcp://") || raw.starts_with("tls://") {
            return Err(ListenError::RemoteNotImplemented(raw.into()));
        }
        // Bare path convenience → unix
        if raw.starts_with('/') {
            return Ok(Self::Unix(PathBuf::from(raw)));
        }
        Err(ListenError::UnsupportedScheme(raw.into()))
    }
}

/// Default unix listen path for credentials.
pub fn default_unix_path(user: &str, system_name: &str, url: &str, env: &str) -> PathBuf {
    let base = crate::runtime_dir::runtime_base_dir();
    let hash = crate::singleton::credential_key_hash(user, system_name, url, env);
    // Short filename (`rgw-<decimal>.sock`) keeps paths under macOS SUN_LEN.
    let path = base.join(format!("rgw-{hash}.sock"));
    crate::runtime_dir::clamp_unix_path(path, hash)
}

/// Bind a unix listener at `path` with mode 0600.
///
/// Refuses to steal a live socket: if `path` already accepts connections,
/// returns an error. Stale (non-listening) paths are unlinked first.
///
/// A sibling `.bindlock` flock serializes the connect-or-unlink + bind
/// critical section so two parents cannot race past each other.
pub async fn bind_unix(path: &Path) -> Result<UnixListener, ListenError> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| ListenError::Io {
                path: path.to_path_buf(),
                source,
            })?;
    }

    let bindlock_path = bindlock_path(path);
    let path_buf = path.to_path_buf();
    tokio::task::spawn_blocking(move || bind_unix_locked(&path_buf, &bindlock_path))
        .await
        .map_err(|e| ListenError::Io {
            path: path.to_path_buf(),
            source: std::io::Error::other(format!("bindlock join: {e}")),
        })?
}

fn bindlock_path(sock: &Path) -> PathBuf {
    let mut s = sock.as_os_str().to_os_string();
    s.push(".bindlock");
    PathBuf::from(s)
}

fn bind_unix_locked(path: &Path, bindlock_path: &Path) -> Result<UnixListener, ListenError> {
    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(bindlock_path)
        .map_err(|source| ListenError::Io {
            path: bindlock_path.to_path_buf(),
            source,
        })?;
    lock_file
        .lock_exclusive()
        .map_err(|source| ListenError::Io {
            path: bindlock_path.to_path_buf(),
            source,
        })?;
    // Keep lock_file in scope until bind completes.
    let _bindlock = lock_file;

    if path.exists() {
        match std::os::unix::net::UnixStream::connect(path) {
            Ok(_live) => {
                return Err(ListenError::Invalid(format!(
                    "unix listen path {} is already accepting connections; refuse to steal",
                    path.display()
                )));
            }
            Err(_) => {
                std::fs::remove_file(path).map_err(|source| ListenError::Io {
                    path: path.to_path_buf(),
                    source,
                })?;
            }
        }
    }

    // Owner-only create: tighten umask around bind so the sock is never
    // briefly world/group-accessible before chmod.
    // SAFETY: umask is process-global; restore immediately after bind.
    let old_umask = unsafe { libc::umask(0o077) };
    let bind_result = UnixListener::bind(path);
    unsafe {
        libc::umask(old_umask);
    }
    let listener = bind_result.map_err(|source| ListenError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut perms = std::fs::metadata(path)
        .map_err(|source| ListenError::Io {
            path: path.to_path_buf(),
            source,
        })?
        .permissions();
    perms.set_mode(0o600);
    std::fs::set_permissions(path, perms).map_err(|source| ListenError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(listener)
}

#[cfg(test)]
mod tests {
    use super::*;

#[test]
fn parses_unix_absolute() {
    let ep = ListenEndpoint::parse("unix:///tmp/rithmic.sock").unwrap();
    assert_eq!(ep, ListenEndpoint::Unix(PathBuf::from("/tmp/rithmic.sock")));
}

#[test]
fn default_unix_path_matches_python_fnv_fixture() {
    // Keep in sync with tests/test_rithmic_gateway_client.py::test_default_unix_path_rust_parity
    let path = default_unix_path(
        "alice",
        "LucidTrading",
        "wss://rprotocol.rithmic.com:443",
        "Live",
    );
    let name = path.file_name().unwrap().to_string_lossy();
    assert_eq!(name, "rgw-13146466402466778522.sock");
}

#[test]
fn rejects_tls_until_v2() {
        let err = ListenEndpoint::parse("tls://0.0.0.0:7600").unwrap_err();
        assert!(matches!(err, ListenError::RemoteNotImplemented(_)));
    }

    #[test]
    fn rejects_tcp_until_v2() {
        let err = ListenEndpoint::parse("tcp://127.0.0.1:7600").unwrap_err();
        assert!(matches!(err, ListenError::RemoteNotImplemented(_)));
    }
}
