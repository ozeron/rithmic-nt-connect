"""The committed Python gencode must describe the same schema as the .proto.

CI regenerates only the Rust side (``prost``); ``session_pb2.py`` is committed
(``scripts/gen_gateway_proto.py``), so editing ``session.proto`` and forgetting
to regenerate would otherwise pass until a Python test touched the new message.
"""

from __future__ import annotations

import re
from pathlib import Path

from rithmic_gateway.v1 import session_pb2 as pb

PROTO = Path(__file__).resolve().parents[1] / "proto/rithmic_gateway/v1/session.proto"

_MESSAGE = re.compile(r"^message\s+(\w+)\s*\{")
_FIELD = re.compile(r"(\w+)\s*=\s*(\d+)\s*[;\[]")
_NOT_A_FIELD = ("option ", "reserved ", "extensions ")


def _proto_schema() -> dict[str, dict[str, int]]:
    """``{message: {field_name: field_number}}`` for every top-level message."""
    schema: dict[str, dict[str, int]] = {}
    current: dict[str, int] | None = None
    depth = 0
    for raw in PROTO.read_text().splitlines():
        line = raw.split("//", 1)[0].strip()
        if not line:
            continue
        if depth == 0 and (match := _MESSAGE.match(line)):
            current = schema.setdefault(match.group(1), {})
        elif (
            current is not None
            and not line.startswith(_NOT_A_FIELD)
            and (match := _FIELD.search(line))
        ):
            current[match.group(1)] = int(match.group(2))
        depth += line.count("{") - line.count("}")
        if depth == 0:
            current = None
    return schema


def test_gencode_has_every_proto_message_and_field() -> None:
    expected = _proto_schema()
    assert expected, "parsed no messages from session.proto"
    described = pb.DESCRIPTOR.message_types_by_name

    assert set(described) == set(expected), (
        "session_pb2.py is stale: re-run scripts/gen_gateway_proto.py "
        f"(messages differ: {sorted(set(described) ^ set(expected))})"
    )
    for name, fields in expected.items():
        generated = {f.name: f.number for f in described[name].fields}
        assert generated == fields, (
            f"session_pb2.py is stale for message {name}: "
            f"proto={fields} generated={generated}"
        )
