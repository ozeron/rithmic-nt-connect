"""Packaging guards: the maturin wheel-data mirror must stay in sync.

`wheel-data/purelib/rithmic_gateway` is the snapshot maturin bundles into the
wheel (see `crates/rithmic-nt-connect/build.rs`). If it drifts from
`python/rithmic_gateway`, the shipped wheel carries stale bindings or client
code — the exact failure that shipped a protobuf-7.34.1 `session_pb2.py` and a
pre-Windows `spawn.py`. These tests fail on any drift.
"""

from __future__ import annotations

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
