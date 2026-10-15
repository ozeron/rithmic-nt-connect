"""Gateway protobufs with safe credential-bearing message displays.

Keep this outside protoc output so regeneration preserves the policy. Protobuf
text formatting does not call nested messages' __repr__, so Frame needs the
same treatment as Handshake. Serialization deliberately retains the real token.
"""

from typing import Any

from google.protobuf.text_format import MessageToString

from . import session_pb2


def _safe_display(message: Any) -> str:
    shown = type(message)()
    shown.CopyFrom(message)
    if shown.DESCRIPTOR.name == "Handshake":
        if shown.auth_token:
            shown.auth_token = "***"
    elif shown.HasField("handshake") and shown.handshake.auth_token:
        shown.handshake.auth_token = "***"
    return MessageToString(shown)


for _message_type in (session_pb2.Handshake, session_pb2.Frame):
    _message_type.__repr__ = _safe_display
    _message_type.__str__ = _safe_display
