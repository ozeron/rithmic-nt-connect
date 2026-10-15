"""Packaging guards for the two wheels.

`rithmic-gateway` (`python/pyproject.toml`) is a maturin ``bindings = "bin"``
wheel, like ruff / uv: the `rithmic-gateway` binary lands in the venv's scripts
dir next to the pure-Python `rithmic_gateway` client. `rithmic-nt-connect`
(root `pyproject.toml`) carries only the adapter and its extension; it never
bundles the gateway (that copy drifted once and shipped a protobuf-7.34.1
`session_pb2.py`). These tests fail if either layout regresses or a fake-plant
binary could ship.
"""

from __future__ import annotations

import tomllib
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GATEWAY_SRC = REPO_ROOT / "python" / "rithmic_gateway"


def _toml(path: Path) -> dict:
    return tomllib.loads(path.read_text())


def test_gateway_wheel_is_a_maturin_bin_wheel_with_the_production_binary_only() -> None:
    cfg = _toml(REPO_ROOT / "python" / "pyproject.toml")
    assert cfg["build-system"]["build-backend"] == "maturin"
    maturin = cfg["tool"]["maturin"]
    assert maturin["bindings"] == "bin"
    assert maturin["module-name"] == "rithmic_gateway"
    assert maturin["manifest-path"] == "../crates/rithmic-gateway/Cargo.toml"
    assert maturin["targets"] == [{"name": "rithmic-gateway", "kind": "bin"}]
    assert "features" not in maturin
    # The lake client must stay installable without Nautilus.
    assert not [d for d in cfg["project"]["dependencies"] if "nautilus" in d]


def test_adapter_wheel_does_not_bundle_the_gateway() -> None:
    cfg = _toml(REPO_ROOT / "pyproject.toml")
    assert "data" not in cfg["tool"]["maturin"]
    assert cfg["project"]["optional-dependencies"]["gateway"] == ["rithmic-gateway"]
    assert not (REPO_ROOT / "wheel-data").exists()
    assert not (REPO_ROOT / "crates" / "rithmic-nt-connect" / "build.rs").exists()


def test_fake_gateway_is_test_only() -> None:
    manifest = (REPO_ROOT / "crates" / "rithmic-gateway" / "Cargo.toml").read_text()
    fake_bin = manifest.split('name = "rithmic-gateway-fake"')[1].split("[[bin]]")[0]
    assert 'required-features = ["testing"]' in fake_bin

    # The production binary has no switch that selects a fake plant.
    prod = REPO_ROOT / "crates/rithmic-gateway/src/bin/rithmic_gateway.rs"
    assert "fake" not in prod.read_text().lower()


def test_shipped_pb2_is_the_5_29_6_generation() -> None:
    pb2 = (GATEWAY_SRC / "v1" / "session_pb2.py").read_bytes()
    assert b"Protobuf Python Version: 5.29.6" in pb2


def test_built_wheels_never_contain_the_fake_gateway() -> None:
    """Any wheel a local or CI build left under ``target/wheels`` is checked."""
    for wheel in sorted((REPO_ROOT / "target" / "wheels").glob("rithmic_*.whl")):
        with zipfile.ZipFile(wheel) as zf:
            names = zf.namelist()
        assert not [n for n in names if "rithmic-gateway-fake" in n], wheel
        editable = any(n.endswith(".pth") for n in names)  # `uv sync` dev build
        if wheel.name.startswith("rithmic_gateway-") and not editable:
            assert [n for n in names if n.endswith("/scripts/rithmic-gateway")], wheel
            assert "rithmic_gateway/client.py" in names, wheel
