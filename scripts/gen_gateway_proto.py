#!/usr/bin/env python
"""Regenerate ``python/rithmic_gateway/v1/session_pb2.py`` from ``session.proto``.

The Python gencode must carry the ``5.29.6`` marker (the repo pins
``protobuf==5.29.6`` and ``tests/test_packaging.py`` asserts it), so the protoc
version used for regeneration is itself pinned — via ``grpcio-tools==1.71.0``
in the uv dev dependency group, which bundles protoc 29.0 — instead of whatever
system ``protoc`` happens to be installed.

protoc 29.0 and 29.6 emit byte-identical gencode for this proto: only the
version header and the ``ValidateProtobufRuntimeVersion`` patch argument differ
(the exact fix commit ``ebcc2d2`` applied by hand). This script runs the pinned
protoc and restores those two lines. It fails loudly if the gencode shape ever
drifts (e.g. after a grpcio-tools bump), so a version change is a deliberate
edit, not a silent regen.

Usage (from the repo root):

    uv run python scripts/gen_gateway_proto.py

The Rust gateway build still needs a system ``protoc`` (``prost-build`` invokes
it from PATH); this script only pins the *Python* bindings generator.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROTO = ROOT / "proto" / "rithmic_gateway" / "v1" / "session.proto"
OUT = ROOT / "python" / "rithmic_gateway" / "v1" / "session_pb2.py"

# protoc 29.0 emits "5.29.0"; the repo pins protobuf==5.29.6. Only these two
# lines differ from 29.6 gencode, so restore the 5.29.6 marker.
_VERSION_COMMENT = "# Protobuf Python Version: 5.29.0"
_RUNTIME_PATCH = re.compile(
    r"(ValidateProtobufRuntimeVersion\(\s*\n\s*_runtime_version\.Domain\.PUBLIC,"
    r"\s*\n\s*5,\s*\n\s*29,\s*\n\s*)0,"
)


def main() -> int:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            "-I",
            str(ROOT / "proto"),
            f"--python_out={ROOT / 'python'}",
            str(PROTO.relative_to(ROOT / "proto")),
        ],
        check=True,
    )
    text = OUT.read_text(encoding="utf-8")
    if _VERSION_COMMENT in text:
        text = text.replace(_VERSION_COMMENT, "# Protobuf Python Version: 5.29.6")
    text, n = _RUNTIME_PATCH.subn(lambda m: f"{m.group(1)}6,", text)
    if n != 1:
        raise SystemExit(
            "unexpected gencode shape: expected exactly one "
            f"ValidateProtobufRuntimeVersion(5, 29, 0) marker, patched {n} — "
            "bump scripts/gen_gateway_proto.py deliberately"
        )
    OUT.write_text(text, encoding="utf-8")
    print(f"regenerated {OUT.relative_to(ROOT)} (protobuf 5.29.6 marker)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
