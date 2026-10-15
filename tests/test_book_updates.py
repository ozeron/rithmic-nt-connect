"""Order-book messages follow the venue ``update_type``.

Fixtures are venue-shaped: the dict the extension builds from a Rithmic
``OrderBook`` (template 156), with the venue's ``update_type`` values
(1 clear, 2 no book, 3 snapshot image, 4 begin, 5 middle, 6 end, 7 solo).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import pytest
from _stubs import _TestDataClient
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.enums import BookAction, BookType, RecordFlag
from nautilus_trader.model.identifiers import InstrumentId
from rithmic_nt_connect._convert import (
    ConvertError,
    bbo_to_fields,
    last_trade_to_fields,
    order_book_to_fields,
    time_bar_to_fields,
)
from rithmic_nt_connect.data import BookAssembler, fields_to_order_book_deltas

F_LAST = int(RecordFlag.F_LAST.value)
F_SNAPSHOT = int(RecordFlag.F_SNAPSHOT.value)


@pytest.fixture
def snapshot_without_timestamp() -> dict[str, Any]:
    # Rithmic Test NQZ6/CME, 2026-10-15: snapshot image has no time keys.
    # Arrays are truncated from the capture; field names and omission are exact.
    return {
        "type": "order_book",
        "symbol": "NQZ6",
        "exchange": "CME",
        "update_type": 3,
        "bid_price": [27000.25, 29797.0, 30023.25, 30030.5],
        "bid_size": [1, 62, 1, 1],
        "ask_price": [30607.25, 30613.0, 30613.5],
        "ask_size": [41, 1, 2],
    }


def test_timestamp_less_snapshot_uses_adapter_clock_and_snapshot_flags(
    snapshot_without_timestamp: dict[str, Any],
) -> None:
    ts_init = 1_792_065_600_000_000_123
    fields = order_book_to_fields(snapshot_without_timestamp, ts_init=ts_init)
    out = fields_to_order_book_deltas(fields, ts_init=ts_init)

    assert fields["ts_event"] == ts_init
    assert [d.action for d in out.deltas] == [BookAction.CLEAR] + [BookAction.ADD] * 7
    assert [d.flags for d in out.deltas] == [F_SNAPSHOT] * 7 + [F_SNAPSHOT | F_LAST]
    assert all(d.ts_event == d.ts_init == ts_init for d in out.deltas)


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        ({"ssboe": 1_792_065_600, "usecs": 123}, 1_792_065_600_000_123_000),
        (
            {"ts_event_ns": 1_792_065_600_000_123_456, "ssboe": 1},
            1_792_065_600_000_123_456,
        ),
    ],
)
def test_incremental_keeps_venue_timestamp(
    timestamp: dict[str, int], expected: int
) -> None:
    row = _venue_book(7, bids=[(30030.5, 9)]) | timestamp
    ts_init = expected + 1_000_000
    fields = order_book_to_fields(row, ts_init=ts_init)
    out = BookAssembler().feed(fields, ts_init=ts_init)
    assert out is not None
    assert out.ts_event == expected
    assert out.ts_init == ts_init


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"symbol": None}, "missing required fields: symbol"),
        ({"bid_size": []}, "length mismatch"),
        ({"ask_price": [None, 30613.0, 30613.5]}, "null price/size"),
        ({"update_type": 9}, "unknown order book update_type"),
    ],
)
def test_timestamp_fallback_still_rejects_malformed_book(
    snapshot_without_timestamp: dict[str, Any],
    changes: dict[str, Any],
    error: str,
) -> None:
    with pytest.raises(ConvertError, match=error):
        order_book_to_fields(snapshot_without_timestamp | changes, ts_init=5)


def test_non_book_events_still_require_venue_timestamp() -> None:
    with pytest.raises(ConvertError, match="missing timestamp fields"):
        last_trade_to_fields(
            {"symbol": "NQZ6", "trade_price": 30030.5, "trade_size": 1}
        )
    with pytest.raises(ConvertError, match="missing timestamp fields"):
        time_bar_to_fields(
            {
                "symbol": "NQZ6",
                "open_price": 30030.5,
                "high_price": 30030.5,
                "low_price": 30030.5,
                "close_price": 30030.5,
                "volume": 1,
            }
        )
    assert (
        bbo_to_fields(
            {
                "symbol": "NQZ6",
                "bid_price": 30030.5,
                "bid_size": 1,
                "ask_price": 30607.25,
                "ask_size": 41,
            }
        )
        is None
    )


def test_data_client_initializes_book_from_timestamp_less_snapshot(
    snapshot_without_timestamp: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    ts_init = 1_792_065_600_000_000_123
    client = _TestDataClient.__new__(_TestDataClient)
    client._clock = Mock(timestamp_ns=Mock(return_value=ts_init))
    client._log = Mock()
    client._books = BookAssembler()
    client._skip_counts = {}
    client._skip_last_flush = float("inf")
    client._skip_log_interval_secs = 60
    received: list[Any] = []
    monkeypatch.setattr(client, "_handle_data", received.append)
    monkeypatch.setattr(client, "_price_precision", lambda _: 2)

    client._dispatch_event(snapshot_without_timestamp)
    assert len(received) == 1
    book = OrderBook(InstrumentId.from_str("NQZ6-CME.RITHMIC"), BookType.L2_MBP)
    book.apply_deltas(received[0])
    assert book.best_bid_price().as_double() == pytest.approx(30030.5)
    assert book.best_ask_size().as_double() == 41
    assert len(book.bids()) == 4
    assert len(book.asks()) == 3
    assert received[0].ts_event == ts_init

    client._dispatch_event(
        {
            "type": "order_book",
            "symbol": "NQZ6",
            "exchange": "CME",
            "update_type": 7,
            "bid_price": [30030.5],
            "bid_size": [9],
            "ask_price": [30607.25],
            "ask_size": [0],
            "ssboe": 1_792_065_600,
            "usecs": 123,
        }
    )
    assert len(received) == 2
    book.apply_deltas(received[1])
    book.check_integrity()
    assert book.best_bid_size().as_double() == 9
    assert book.best_ask_price().as_double() == pytest.approx(30613.0)
    assert book.best_ask_size().as_double() == 1
    assert len(book.bids()) == 4
    assert len(book.asks()) == 2
    assert received[1].ts_event == 1_792_065_600_000_123_000
    assert received[1].ts_init == ts_init
    assert client._skip_counts == {}


def _venue_book(
    update_type: int | None,
    bids: tuple[tuple[float, int], ...] | list[tuple[float, int]] = (),
    asks: tuple[tuple[float, int], ...] | list[tuple[float, int]] = (),
) -> dict[str, Any]:
    return {
        "type": "order_book",
        "symbol": "NQU6",
        "exchange": "CME",
        "update_type": update_type,
        "bid_price": [p for p, _ in bids],
        "bid_size": [s for _, s in bids],
        "ask_price": [p for p, _ in asks],
        "ask_size": [s for _, s in asks],
        "ssboe": 1_790_000_000,
        "usecs": 0,
    }


def _feed(asm: BookAssembler, event: dict[str, Any]) -> Any:
    return asm.feed(order_book_to_fields(event), ts_init=5, price_precision=2)


def test_a_snapshot_image_replaces_the_book() -> None:
    out = _feed(
        BookAssembler(),
        _venue_book(3, bids=[(21000.0, 3)], asks=[(21000.25, 4)]),
    )
    actions = [d.action for d in out.deltas]
    assert actions == [BookAction.CLEAR, BookAction.ADD, BookAction.ADD]
    assert all(d.flags & F_SNAPSHOT for d in out.deltas)
    assert out.deltas[-1].flags & F_LAST


def test_a_solo_update_changes_levels_without_clearing() -> None:
    out = _feed(BookAssembler(), _venue_book(7, bids=[(21000.0, 5), (20999.75, 0)]))
    actions = [d.action for d in out.deltas]
    assert BookAction.CLEAR not in actions
    assert actions == [BookAction.UPDATE, BookAction.DELETE]
    assert out.deltas[-1].flags & F_LAST
    assert not out.deltas[0].flags & F_LAST
    assert not any(d.flags & F_SNAPSHOT for d in out.deltas)


def test_a_multi_part_update_is_one_batch_ending_with_f_last() -> None:
    asm = BookAssembler()
    assert _feed(asm, _venue_book(4, bids=[(21000.0, 5)])) is None
    assert _feed(asm, _venue_book(5, asks=[(21000.5, 2)])) is None
    out = _feed(asm, _venue_book(6, asks=[(21000.25, 0)]))
    assert [d.action for d in out.deltas] == [
        BookAction.UPDATE,
        BookAction.UPDATE,
        BookAction.DELETE,
    ]
    assert [bool(d.flags & F_LAST) for d in out.deltas] == [False, False, True]


def test_an_end_with_no_levels_still_closes_the_batch() -> None:
    asm = BookAssembler()
    _feed(asm, _venue_book(4, bids=[(21000.0, 5)]))
    out = _feed(asm, _venue_book(6))
    assert [d.action for d in out.deltas] == [BookAction.UPDATE]
    assert out.deltas[-1].flags & F_LAST


def test_clear_and_no_book_empty_the_book() -> None:
    for update_type in (1, 2):
        out = _feed(BookAssembler(), _venue_book(update_type))
        assert [d.action for d in out.deltas] == [BookAction.CLEAR]
        assert out.deltas[0].flags & F_LAST


def test_a_snapshot_discards_a_half_received_update() -> None:
    asm = BookAssembler()
    _feed(asm, _venue_book(4, bids=[(21000.0, 5)]))
    _feed(asm, _venue_book(3, bids=[(20000.0, 1)]))
    # The stale BEGIN part does not leak into the next batch.
    out = _feed(asm, _venue_book(6, asks=[(20000.25, 1)]))
    assert [d.action for d in out.deltas] == [BookAction.UPDATE]


def test_an_unknown_update_type_is_rejected() -> None:
    with pytest.raises(ConvertError, match="update_type"):
        order_book_to_fields(_venue_book(9, bids=[(21000.0, 1)]))


def test_a_stream_reset_discards_a_half_received_update() -> None:
    asm = BookAssembler()
    assert _feed(asm, _venue_book(4, bids=[(21000.0, 1)])) is None
    asm.reset()  # the ticker stream was reset mid-update
    out = _feed(asm, _venue_book(6, asks=[(21000.25, 2)]))
    assert [d.order.price.as_double() for d in out.deltas] == [21000.25]
    assert out.deltas[-1].flags & F_LAST
