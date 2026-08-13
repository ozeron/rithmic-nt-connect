//! Listen URL parsing and unix bind helpers.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

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
pub fn default_unix_path(user: &str, system_name: &str, url: &str) -> PathBuf {
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    let key = format!("{user}|{system_name}|{url}");
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in key.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    base.join(format!("rithmic-gateway-{hash}.sock"))
}

/// Bind a unix listener at `path` with mode 0600. Unlinks stale socket first.
pub async fn bind_unix(path: &Path) -> Result<UnixListener, ListenError> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| ListenError::Io {
                path: path.to_path_buf(),
                source,
            })?;
    }
    if path.exists() {
        tokio::fs::remove_file(path)
            .await
            .map_err(|source| ListenError::Io {
                path: path.to_path_buf(),
                source,
            })?;
    }
    let listener = UnixListener::bind(path).map_err(|source| ListenError::Io {
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
