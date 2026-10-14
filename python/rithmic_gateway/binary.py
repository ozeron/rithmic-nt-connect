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


# Cargo emits ``rithmic-gateway.exe`` on Windows and ``rithmic-gateway`` elsewhere.
_GATEWAY_BIN_NAME = "rithmic-gateway"
_GATEWAY_BIN_NAMES = (_GATEWAY_BIN_NAME, _GATEWAY_BIN_NAME + ".exe")


def _bin_search_starts() -> list[Path]:
    """Roots to walk for a source checkout's cargo ``target/`` binaries (the
    dev fallback; installed wheels carry the binary in their scripts dir)."""
    starts = [Path.cwd(), Path(__file__).resolve().parent]
    try:
        import rithmic_nt_connect

        starts.append(Path(rithmic_nt_connect.__file__).resolve().parent)
    except ImportError:
        pass
    return starts


def _cargo_target_candidates(start: Path) -> list[Path]:
    """Look for ``target/{release,debug}/rithmic-gateway[.exe]`` under
    ``start`` ancestors.
    """
    out: list[Path] = []
    cargo_target = os.environ.get("CARGO_TARGET_DIR")
    if cargo_target:
        base = Path(cargo_target).expanduser()
        for name in _GATEWAY_BIN_NAMES:
            out.append(base / "release" / name)
            out.append(base / "debug" / name)
    cur = start.resolve()
    for root in [cur, *cur.parents]:
        for name in _GATEWAY_BIN_NAMES:
            out.append(root / "target" / "release" / name)
            out.append(root / "target" / "debug" / name)
        if (root / "Cargo.toml").is_file() and (root / "crates").is_dir():
            break
    return out


def _wheel_bin() -> str | None:
    """The binary the ``rithmic-gateway`` wheel installed into this environment's
    scripts dir (next to the interpreter for a venv), if present."""
    dirs = {sysconfig.get_path("scripts"), str(Path(sys.executable).parent)}
    for base in dirs:
        for name in _GATEWAY_BIN_NAMES:
            path = Path(base) / name
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
    ``rithmic-gateway`` wheel installed in this environment → ``PATH`` → a
    source checkout's ``target/{release,debug}`` (and ``CARGO_TARGET_DIR``).
    Never runs ``cargo build``.
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
    seen: set[str] = set()
    for start in _bin_search_starts():
        for candidate in _cargo_target_candidates(start):
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return str(candidate.resolve())
    raise GatewayBinaryError(
        "rithmic-gateway not found: install the rithmic-gateway wheel (it carries "
        "the binary), or set RITHMIC_GATEWAY_BIN, or in a source checkout run "
        "`cargo build -p rithmic-gateway --release`"
    )
