//! Sync the maturin `wheel-data` snapshot of the pure-Python gateway client
//! before the wheel is assembled.
//!
//! maturin's `python-source` only packages the `rithmic_nt_connect` module, so
//! the shared `rithmic_gateway` client rides into the wheel via the `data` dir
//! (`wheel-data/purelib/rithmic_gateway`). Copying here — instead of relying on
//! `scripts/build_wheel.sh` alone — keeps every maturin build (uv, CI, the
//! script) fresh; a stale `wheel-data` is how a protobuf-7.34.1
//! `session_pb2.py` and a pre-Windows `spawn.py` shipped.
//!
//! The destination is gitignored build output. `scripts/build_wheel.sh`
//! additionally guarantees the release-built native binary is present and
//! fails loudly when it is not.

use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let root = root.canonicalize().unwrap_or(root);
    let src = root.join("python/rithmic_gateway");
    if !src.is_dir() {
        return; // non-repo build (sdist): nothing to copy
    }
    println!("cargo:rerun-if-changed={}", src.display());

    let dest = root.join("wheel-data/purelib/rithmic_gateway");
    let _ = fs::remove_dir_all(&dest);
    copy_tree(&src, &dest);

    // Native gateway binary: prefer release, fall back to debug (e.g. after
    // `cargo test` in CI). Missing => the wheel has no `bin/` and
    // `resolve_gateway_bin` falls back to PATH / `target/`.
    let target = target_dir(&root);
    for profile in ["release", "debug"] {
        for name in ["rithmic-gateway", "rithmic-gateway.exe"] {
            let bin = target.join(profile).join(name);
            if bin.is_file() {
                let dest_bin = dest.join("bin");
                fs::create_dir_all(&dest_bin).expect("create wheel-data bin dir");
                fs::copy(&bin, dest_bin.join(name)).expect("copy gateway binary");
                println!("cargo:rerun-if-changed={}", bin.display());
                return;
            }
        }
    }
}

/// Copy a tree of Python sources into `dest`, skipping bytecode caches.
fn copy_tree(src: &Path, dest: &Path) {
    fs::create_dir_all(dest).expect("create wheel-data dir");
    for entry in fs::read_dir(src).expect("read gateway source") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        let file_name = entry.file_name();
        if path.is_dir() {
            if file_name != "__pycache__" {
                copy_tree(&path, &dest.join(file_name));
            }
        } else {
            fs::copy(&path, dest.join(file_name)).expect("copy gateway file");
        }
    }
}

/// The cargo target dir: `CARGO_TARGET_DIR` if set (honored by
/// `scripts/build_wheel.sh` too), else `<repo>/target`.
fn target_dir(root: &Path) -> PathBuf {
    match std::env::var_os("CARGO_TARGET_DIR") {
        Some(path) => {
            let path = PathBuf::from(path);
            if path.is_absolute() {
                path
            } else {
                root.join(path)
            }
        }
        None => root.join("target"),
    }
}
