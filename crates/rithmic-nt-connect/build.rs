//! Populate the maturin `wheel-data` gateway-client snapshot before the wheel
//! is assembled.
//!
//! maturin's `python-source` only packages the module matching the lib name
//! (`rithmic_nt_connect`), so the shared pure-Python `rithmic_gateway` client
//! (and the native gateway binary, when a build has produced one) is carried
//! into the wheel through the `data` dir (`wheel-data/purelib/rithmic_gateway`).
//!
//! Copying here — instead of relying solely on `scripts/build_wheel.sh` —
//! means *every* maturin build (`uv sync`, `uv run pytest`, CI, the script)
//! produces a wheel with a fresh client. Ad-hoc builds can never ship a stale
//! `session_pb2` / `spawn.py` or a missing `rithmic_gateway` package, which is
//! exactly what happened when `wheel-data` was only refreshed by the script.
//!
//! The destination is gitignored build output; `scripts/build_wheel.sh`
//! additionally guarantees the release-built native binary is present and
//! fails loudly when it is not.

use std::fs;
use std::path::PathBuf;

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let root = root.canonicalize().unwrap_or(root);
    let src = root.join("python/rithmic_gateway");
    let dest = root.join("wheel-data/purelib/rithmic_gateway");

    if !src.is_dir() {
        // Non-repo builds (e.g. a packaged sdist) have no python tree to copy.
        return;
    }

    // Re-run this script whenever the client source changes so a stale mirror
    // can never survive a rebuild.
    println!("cargo:rerun-if-changed={}", src.display());

    // Fresh snapshot: mirror *all* current source, dropping anything stale.
    let _ = fs::remove_dir_all(&dest);
    fs::create_dir_all(&dest).expect("create wheel-data gateway dir");
    for entry in fs::read_dir(&src).expect("read python/rithmic_gateway") {
        let entry = entry.expect("gateway dir entry");
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|e| e.to_str()) == Some("py") {
            fs::copy(&path, dest.join(entry.file_name())).expect("copy gateway py module");
        }
    }

    // The `v1` package holds the generated protobuf bindings — always copied
    // fresh so the wheel can never lag the regenerated `session_pb2.py`.
    let src_v1 = src.join("v1");
    let dest_v1 = dest.join("v1");
    fs::create_dir_all(&dest_v1).expect("create wheel-data v1 dir");
    for entry in fs::read_dir(&src_v1).expect("read python/rithmic_gateway/v1") {
        let entry = entry.expect("v1 dir entry");
        if entry.path().is_file() {
            fs::copy(entry.path(), dest_v1.join(entry.file_name())).expect("copy v1 module");
        }
    }
    println!("cargo:rerun-if-changed={}", src_v1.display());

    // Native gateway binary: prefer a release build, fall back to debug (e.g.
    // after `cargo test` in CI). Absent => the wheel simply has no `bin/` and
    // `resolve_gateway_bin` falls back to PATH / `target/` — same as before.
    let target = root.join("target");
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
