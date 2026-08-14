//! Runtime directory helpers for sock / lock paths (prefer private dirs).

use std::ffi::OsStr;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

/// macOS `sockaddr_un.sun_path` is 104 bytes incl. NUL → max usable path 103.
pub const UNIX_PATH_MAX: usize = 103;

/// Prefer `XDG_RUNTIME_DIR`; else `$TMPDIR/rgw-$UID` (or `/tmp/...`)
/// created as `0700` owned by this uid. Never use bare world-sticky `/tmp`
/// as the *directory* for defaults (single files under `/tmp` are only a
/// last-resort clamp when the runtime path exceeds [`UNIX_PATH_MAX`]).
pub fn runtime_base_dir() -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_RUNTIME_DIR") {
        let p = PathBuf::from(xdg);
        if !p.as_os_str().is_empty() {
            return p;
        }
    }
    let tmp = std::env::var_os("TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    // Short dir name keeps sock paths under macOS SUN_LEN with long TMPDIR.
    let dir = tmp.join(format!("rgw-{uid}"));
    ensure_private_dir(&dir).unwrap_or(dir)
}

fn ensure_private_dir(dir: &Path) -> std::io::Result<PathBuf> {
    match fs::create_dir_all(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    let meta = fs::metadata(dir)?;
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    if meta.uid() != uid {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "runtime dir {} not owned by uid {uid}",
                dir.display()
            ),
        ));
    }
    let mut perms = meta.permissions();
    perms.set_mode(0o700);
    fs::set_permissions(dir, perms)?;
    Ok(dir.to_path_buf())
}

/// Canonical env token for path hashing (Live/Demo/Test aliases).
pub fn canon_env(env: &str) -> &'static str {
    match env.to_ascii_lowercase().as_str() {
        "live" | "production" => "live",
        "demo" | "development" => "demo",
        "test" => "test",
        _ => "live",
    }
}

/// If `path` would exceed unix socket path limits, rewrite under a private
/// `/tmp/rgw-$UID/` directory (0700) with a short name — never a bare file in
/// sticky `/tmp` (local squat risk).
pub fn clamp_unix_path(path: PathBuf, hash: u64) -> PathBuf {
    if os_len(&path) <= UNIX_PATH_MAX {
        return path;
    }
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    let dir = PathBuf::from("/tmp").join(format!("rgw-{uid}"));
    let _ = ensure_private_dir(&dir);
    dir.join(format!("{:08x}.sock", hash as u32))
}

/// Sibling lock path for a sock path (same stem, `.lock` suffix), clamped.
pub fn clamp_lock_path(path: PathBuf, hash: u64) -> PathBuf {
    if os_len(&path) <= UNIX_PATH_MAX {
        return path;
    }
    // SAFETY: getuid has no preconditions.
    let uid = unsafe { libc::getuid() };
    let dir = PathBuf::from("/tmp").join(format!("rgw-{uid}"));
    let _ = ensure_private_dir(&dir);
    dir.join(format!("{:08x}.lock", hash as u32))
}

fn os_len(path: &Path) -> usize {
    OsStr::len(path.as_os_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canon_env_aliases() {
        assert_eq!(canon_env("Live"), "live");
        assert_eq!(canon_env("production"), "live");
        assert_eq!(canon_env("Demo"), "demo");
        assert_eq!(canon_env("test"), "test");
    }

    #[test]
    fn clamp_shortens_oversized_paths() {
        let long = PathBuf::from(format!(
            "/var/folders/09/p1d_rff504sfyjjc4hs600v00000gn/T/very-long-prefix-{}",
            "x".repeat(40)
        ))
        .join("rgw-16844568880994123637.sock");
        assert!(
            os_len(&long) > UNIX_PATH_MAX,
            "fixture path len={} expected > {}",
            os_len(&long),
            UNIX_PATH_MAX
        );
        let clamped = clamp_unix_path(long, 0x1684_4568_8809_9412);
        assert!(os_len(&clamped) <= UNIX_PATH_MAX);
        let s = clamped.to_string_lossy();
        // SAFETY: getuid has no preconditions.
        let uid = unsafe { libc::getuid() };
        let prefix = format!("/tmp/rgw-{uid}/");
        assert!(
            s.starts_with(&prefix) && s.ends_with(".sock"),
            "clamped={clamped:?} must live under {prefix}"
        );
    }
}
