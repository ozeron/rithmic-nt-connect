"""GatewayClient history reads: request framing and response shaping.

The direct path returns ``fill_history`` / ``order_notification`` dicts with
``type`` set and uint64 fields as ints. The gateway client must return the same
shape: proto-JSON renders uint64 as a string, so the normalizers must undo that.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from rithmic_gateway.client import GatewayClient
from rithmic_gateway.config import GatewayConfig
from rithmic_gateway.framed_conn import GatewayError
from rithmic_gateway.v1 import session_pb2 as pb


def _client() -> GatewayClient:
    return GatewayClient(
        GatewayConfig(user="u", system_name="s", url="wss://x", env="Test")
    )


def _answer_with(
    monkeypatch: pytest.MonkeyPatch, client: GatewayClient, response: pb.Frame
) -> list[pb.Frame]:
    """Replace the RPC round-trip; record the request frames the client sends."""
    sent: list[pb.Frame] = []

    def fake_rpc(frame: pb.Frame, **_kwargs: object) -> pb.Frame:
        sent.append(frame)
        return response

    monkeypatch.setattr(client, "_rpc", fake_rpc)
    return sent


def test_load_fill_history_frames_request_and_shapes_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    response = pb.Frame(
        request_id=1,
        load_fill_history_response=pb.LoadFillHistoryResponse(
            rows=[
                pb.FillHistory(
                    account_id="A1",
                    fill_id="F1",
                    symbol="NQU6",
                    fill_size=3,
                    fill_price=101.25,
                    ssboe=1_700_000_000,
                    usecs=5,
                )
            ]
        ),
    )
    sent = _answer_with(monkeypatch, client, response)

    rows = client.load_fill_history(20260901, 20260930)

    assert sent[0].WhichOneof("body") == "load_fill_history"
    assert sent[0].load_fill_history.start_date == 20260901
    assert sent[0].load_fill_history.finish_date == 20260930
    assert rows == [
        {
            "type": "fill_history",
            "account_id": "A1",
            "fill_id": "F1",
            "symbol": "NQU6",
            "fill_size": 3,
            "fill_price": 101.25,
            "ssboe": 1_700_000_000,
            "usecs": 5,
        }
    ]
    assert isinstance(rows[0]["fill_size"], int)


def test_load_fill_history_wrong_body_is_protocol_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    _answer_with(monkeypatch, client, pb.Frame(request_id=1, ack=pb.Ack()))

    with pytest.raises(GatewayError) as excinfo:
        client.load_fill_history(20260901, 20260930)
    assert excinfo.value.code == "protocol"


def test_load_order_history_dates_returns_venue_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    sent = _answer_with(
        monkeypatch,
        client,
        pb.Frame(
            request_id=1,
            load_order_history_dates_response=pb.LoadOrderHistoryDatesResponse(
                dates=["20260929", "20260930"]
            ),
        ),
    )

    assert client.load_order_history_dates() == ["20260929", "20260930"]
    assert sent[0].WhichOneof("body") == "load_order_history_dates"


def test_load_order_history_frames_date_and_shapes_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    sent = _answer_with(
        monkeypatch,
        client,
        pb.Frame(
            request_id=1,
            load_order_history_response=pb.LoadOrderHistoryResponse(
                events=[
                    pb.OrderNotification(
                        source="rithmic",
                        basket_id="B1",
                        ts_event_ns=1_234_567_890_123,
                    )
                ]
            ),
        ),
    )

    events = client.load_order_history("20260930")

    assert sent[0].load_order_history.date == "20260930"
    assert events == [
        {
            "type": "order_notification",
            "source": "rithmic",
            "basket_id": "B1",
            "ts_event_ns": 1_234_567_890_123,
        }
    ]


@pytest.mark.parametrize(
    ("call", "expected_body"),
    [
        (
            lambda c: c.load_order_history_dates(),
            "load_order_history_dates_response",
        ),
        (
            lambda c: c.load_order_history("20260930"),
            "load_order_history_response",
        ),
    ],
    ids=["dates", "order_history"],
)
def test_history_reads_reject_wrong_body(
    monkeypatch: pytest.MonkeyPatch,
    call: Callable[[GatewayClient], object],
    expected_body: str,
) -> None:
    client = _client()
    _answer_with(monkeypatch, client, pb.Frame(request_id=1, ack=pb.Ack()))

    with pytest.raises(GatewayError, match=expected_body) as excinfo:
        call(client)
    assert excinfo.value.code == "protocol"
