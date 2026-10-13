"""Locate the ``rithmic-gateway`` parent binary (used by the launcher and tests).

The client library never starts the parent; this module only resolves where the
binary lives so ``python -m rithmic_gateway`` can exec it.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path


class GatewayBinaryError(RuntimeError):
    """The ``rithmic-gateway`` binary could not be located."""


# Cargo emits ``rithmic-gateway.exe`` on Windows and ``rithmic-gateway`` elsewhere.
_GATEWAY_BIN_NAME = "rithmic-gateway"
_GATEWAY_BIN_NAMES = (_GATEWAY_BIN_NAME, _GATEWAY_BIN_NAME + ".exe")


def _bin_search_starts() -> list[Path]:
    """Roots to walk for cargo ``target/`` binaries.

    Include the editable ``rithmic_nt_connect`` tree. The installed
    ``rithmic_gateway`` wheel carries its own ``bin/`` (resolved separately via
    :func:`_bundled_bin`) and has no ``target/``.
    """
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


def _bundled_bin() -> str | None:
    """Return the gateway binary bundled inside the installed package, if present.

    Wheels built via ``scripts/build_wheel.sh`` carry ``rithmic_gateway/bin/``
    alongside the Python client, so an installed wheel self-contains the native
    binary with no ``cargo build`` and no ``target/`` on the consumer's disk.
    """
    base = Path(__file__).resolve().parent / "bin"
    for name in _GATEWAY_BIN_NAMES:
        path = base / name
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
    """Resolve ``rithmic-gateway`` from explicit path, env, bundled, PATH, or
    cargo targets.

    Does not run ``cargo build`` — set ``RITHMIC_GATEWAY_BIN`` or build the
    binary first.
    Search order: explicit → ``RITHMIC_GATEWAY_BIN`` → bundled ``rithmic_gateway/bin`` →
    ``PATH`` → cwd then package ``target/{release,debug}`` (and ``CARGO_TARGET_DIR``).
    """
    for label, raw in (
        ("gateway bin", explicit),
        ("RITHMIC_GATEWAY_BIN", os.environ.get("RITHMIC_GATEWAY_BIN")),
    ):
        resolved = _resolve_user_bin(raw, label)
        if resolved is not None:
            return resolved
    bundled = _bundled_bin()
    if bundled is not None:
        return bundled
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
        "rithmic-gateway not on PATH or under target/{release,debug}; "
        "set RITHMIC_GATEWAY_BIN or build with "
        "`cargo build -p rithmic-gateway --release`"
    )
