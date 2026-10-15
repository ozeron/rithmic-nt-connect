"""Locate the ``rithmic-gateway`` parent binary (used by the launcher and tests).

The client library never starts the parent; this module only resolves where the
binary lives so ``python -m rithmic_gateway`` can exec it.
"""

from __future__ import annotations

import os
import shutil
import sys
import sysconfig
from pathlib import Path


class GatewayBinaryError(RuntimeError):
    """The ``rithmic-gateway`` binary could not be located."""


_GATEWAY_BIN_NAME = "rithmic-gateway"


def _checkout_root(start: Path) -> Path | None:
    """The rithmic-connect workspace this module was imported from, if any (a
    source checkout: ``Cargo.toml`` plus ``crates/rithmic-gateway``)."""
    for root in [start, *start.parents]:
        if (root / "Cargo.toml").is_file() and (
            root / "crates" / "rithmic-gateway"
        ).is_dir():
            return root
    return None


def _cargo_target_candidates() -> list[Path]:
    """The dev fallback: ``target/{release,debug}/rithmic-gateway`` of the source
    checkout this package was imported from (and ``CARGO_TARGET_DIR``). Never the
    working directory: running the launcher from an untrusted directory must not
    exec a binary found there."""
    out: list[Path] = []
    cargo_target = os.environ.get("CARGO_TARGET_DIR")
    if cargo_target:
        base = Path(cargo_target).expanduser()
        out += [
            base / "release" / _GATEWAY_BIN_NAME,
            base / "debug" / _GATEWAY_BIN_NAME,
        ]
    root = _checkout_root(Path(__file__).resolve().parent)
    if root is not None:
        out += [
            root / "target" / "release" / _GATEWAY_BIN_NAME,
            root / "target" / "debug" / _GATEWAY_BIN_NAME,
        ]
    return out


def _wheel_bin() -> str | None:
    """The binary the ``rithmic-gateway`` wheel installed into this environment's
    scripts dir (next to the interpreter for a venv), if present."""
    dirs = {sysconfig.get_path("scripts"), str(Path(sys.executable).parent)}
    for base in dirs:
        path = Path(base) / _GATEWAY_BIN_NAME
        if path.is_file() and os.access(path, os.X_OK):
            return str(path.resolve())
    return None


def _resolve_user_bin(raw: str | None, label: str) -> str | None:
    """Resolve a user-supplied binary path or ``None`` when unset;
    ``GatewayBinaryError`` if missing.
    """
    if not raw:
        return None
    path = Path(raw).expanduser()
    if not path.is_file():
        raise GatewayBinaryError(f"{label} not found: {path}")
    return str(path.resolve())


def resolve_gateway_bin(explicit: str | None = None) -> str:
    """Resolve ``rithmic-gateway``.

    Search order: explicit → ``RITHMIC_GATEWAY_BIN`` → the binary the
    ``rithmic-gateway`` wheel installed in this environment → ``PATH`` → the
    ``target/{release,debug}`` of the source checkout this package was imported
    from (and ``CARGO_TARGET_DIR``). Never runs ``cargo build``.
    """
    for label, raw in (
        ("gateway bin", explicit),
        ("RITHMIC_GATEWAY_BIN", os.environ.get("RITHMIC_GATEWAY_BIN")),
    ):
        resolved = _resolve_user_bin(raw, label)
        if resolved is not None:
            return resolved
    installed = _wheel_bin()
    if installed is not None:
        return installed
    found = shutil.which(_GATEWAY_BIN_NAME)
    if found:
        return found
    for candidate in _cargo_target_candidates():
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate.resolve())
    raise GatewayBinaryError(
        "rithmic-gateway not found: install the rithmic-gateway wheel (it carries "
        "the binary), or set RITHMIC_GATEWAY_BIN, or in a source checkout run "
        "`cargo build -p rithmic-gateway --release`"
    )
