"""Additive transport events remain safe for current and pre-field-12 clients."""

from __future__ import annotations

import socket

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from rithmic_gateway.client import GatewayClient, _event_to_dict
from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.framing import encode_frame
from rithmic_gateway.v1 import session_pb2 as pb


def test_lake_client_keeps_reading_after_transport_interrupted() -> None:
    interrupted = pb.Frame(
        event=pb.Event(transport_interrupted=pb.TransportInterrupted(generation=3))
    )
    trade = pb.Frame(
        event=pb.Event(last_trade=pb.LastTrade(symbol="NQU6", exchange="CME"))
    )
    client = GatewayClient(
        GatewayConfig(
            user="u", system_name="test", url="wss://example", attest_flock=False
        )
    )
    ours, theirs = socket.socketpair()
    client._sock = ours
    try:
        theirs.sendall(
            encode_frame(interrupted.SerializeToString())
            + encode_frame(trade.SerializeToString())
        )
        assert client.poll_event(timeout_ms=500) == {
            "type": "transport_interrupted",
            "generation": "3",
        }
        event = client.poll_event(timeout_ms=500)
        assert event is not None and event["type"] == "last_trade"
        assert event["symbol"] == "NQU6"
    finally:
        client.disconnect()
        theirs.close()


def test_old_protobuf_schema_ignores_unknown_event_body() -> None:
    # Freeze the old reader's schema by removing the sole additive field and
    # message. Use an isolated pool so the installed new schema cannot leak in.
    # Protobuf constructs these descriptor classes dynamically at import time.
    schema = descriptor_pb2.FileDescriptorProto.FromString(  # ty: ignore[unresolved-attribute]
        pb.DESCRIPTOR.serialized_pb
    )
    event_schema = next(msg for msg in schema.message_type if msg.name == "Event")
    field = next(field for field in event_schema.field if field.number == 12)
    event_schema.field.remove(field)
    message = next(
        msg for msg in schema.message_type if msg.name == "TransportInterrupted"
    )
    schema.message_type.remove(message)
    pool = descriptor_pool.DescriptorPool()  # ty: ignore[possibly-missing-implicit-call]
    pool.Add(schema)
    old_frame = message_factory.GetMessageClass(
        pool.FindMessageTypeByName(pb.Frame.DESCRIPTOR.full_name)
    )

    frame = pb.Frame(
        event=pb.Event(transport_interrupted=pb.TransportInterrupted(generation=3))
    )
    decoded = old_frame.FromString(frame.SerializeToString())
    assert decoded.WhichOneof("body") == "event"
    assert decoded.event.WhichOneof("body") is None
    assert _event_to_dict(decoded.event) == {"type": "empty"}
    # Python protobuf retains unknown fields, even though the oneof is unset.
    assert pb.Frame.FromString(decoded.SerializeToString()) == frame

    trade = pb.Frame(event=pb.Event(last_trade=pb.LastTrade(symbol="NQU6")))
    decoded_trade = old_frame.FromString(trade.SerializeToString())
    assert _event_to_dict(decoded_trade.event)["symbol"] == "NQU6"
