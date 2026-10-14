"""Packaging guards: the maturin wheel-data mirror must stay in sync.

`wheel-data/purelib/rithmic_gateway` is the snapshot maturin bundles into the
wheel (see `crates/rithmic-nt-connect/build.rs`). If it drifts from
`python/rithmic_gateway`, the shipped wheel carries stale bindings or client
code — the exact failure that shipped a protobuf-7.34.1 `session_pb2.py` and a
pre-Windows `spawn.py`. These tests fail on any drift.
"""

from __future__ import annotations

import subprocess  # nosec B404 - runs the launcher under test
import sys
import tempfile
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "python" / "rithmic_gateway"
MIRROR = REPO_ROOT / "wheel-data" / "purelib" / "rithmic_gateway"


def test_wheel_data_mirrors_gateway_client_sources() -> None:
    if not MIRROR.is_dir():
        pytest.skip("wheel-data not populated yet (run a maturin/cargo build first)")

    src_files = {p.relative_to(SRC) for p in SRC.rglob("*.py")}
    mirror_files = {p.relative_to(MIRROR) for p in MIRROR.rglob("*.py")}

    assert mirror_files == src_files, (
        "wheel-data mirror diverged from python/rithmic_gateway; "
        "missing: "
        f"{sorted(src_files - mirror_files) or 'none'}, "
        f"orphaned: {sorted(mirror_files - src_files) or 'none'}"
    )

    for rel in sorted(src_files):
        assert (MIRROR / rel).read_bytes() == (SRC / rel).read_bytes(), (
            f"stale wheel-data mirror: {rel}"
        )


def test_wheel_data_pb2_matches_regenerated_bindings() -> None:
    """The wheel must carry the same session_pb2 gencode the source tree does."""
    if not MIRROR.is_dir():
        pytest.skip("wheel-data not populated yet (run a maturin/cargo build first)")

    mirror = MIRROR / "v1" / "session_pb2.py"
    source = SRC / "v1" / "session_pb2.py"
    assert mirror.is_file() and source.is_file()
    assert mirror.read_bytes() == source.read_bytes()
    # Sanity: the shipped bindings must be the 5.29.6-compatible generation.
    assert b"Protobuf Python Version: 5.29.6" in mirror.read_bytes()


def test_bundled_gateway_binary_is_launchable_via_the_launcher() -> None:
    """``python -m rithmic_gateway`` execs the binary bundled in the wheel layout.

    The credential-free parent refuses to start (``RITHMIC_USER required``); that
    refusal comes from the real binary, which proves the launcher resolved and
    exec'd it without cargo or a ``target/`` directory.
    """
    bundled = MIRROR / "bin"
    if not any(
        (bundled / n).is_file() for n in ("rithmic-gateway", "rithmic-gateway.exe")
    ):
        pytest.skip("no bundled gateway binary (run scripts/build_wheel.sh)")

    proc = subprocess.run(  # nosec B603 - fixed argv, test-controlled env
        [sys.executable, "-m", "rithmic_gateway"],
        # Import the wheel layout, not the source tree; empty env, no cargo target.
        env={"PYTHONPATH": str(MIRROR.parent), "PATH": "/usr/bin:/bin"},
        cwd=Path(tempfile.gettempdir()),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert "rithmic_gateway:" not in proc.stderr  # launcher did not fail to resolve
    assert "RITHMIC_USER" in proc.stderr  # the real binary ran and refused to start
    assert proc.returncode != 0


def test_wheel_bundles_the_production_gateway_and_never_the_fake() -> None:
    """The fake-plant gateway is test-only: never built, bundled, or selectable."""
    manifest = (REPO_ROOT / "crates" / "rithmic-gateway" / "Cargo.toml").read_text()
    # The fake bin only builds with the `testing` feature.
    fake_bin = manifest.split('name = "rithmic-gateway-fake"')[1].split("[[bin]]")[0]
    assert 'required-features = ["testing"]' in fake_bin

    # The wheel script builds the production binary only, without features.
    script = (REPO_ROOT / "scripts" / "build_wheel.sh").read_text()
    assert "cargo build -p rithmic-gateway --release" in script
    assert "fake" not in script.lower()
    build_line = script.split("cargo build -p rithmic-gateway")[1].split("\n")[0]
    assert "--features" not in build_line

    # The production binary has no switch that selects a fake plant.
    prod = REPO_ROOT / "crates/rithmic-gateway/src/bin/rithmic_gateway.rs"
    assert "fake" not in prod.read_text().lower()

    # Whatever is bundled (mirror) or built into a wheel contains no fake binary.
    bin_dir = MIRROR / "bin"
    if bin_dir.is_dir():
        assert not [p.name for p in bin_dir.iterdir() if "fake" in p.name]
    for wheel in sorted(
        (REPO_ROOT / "target" / "wheels").glob("rithmic_nt_connect-*.whl")
    ):
        with zipfile.ZipFile(wheel) as zf:
            names = zf.namelist()
        assert not [n for n in names if "rithmic-gateway-fake" in n], wheel
