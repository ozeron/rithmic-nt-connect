"""Order-book messages follow the venue ``update_type``.

Fixtures are venue-shaped: the dict the extension builds from a Rithmic
``OrderBook`` (template 156), with the venue's ``update_type`` values
(1 clear, 2 no book, 3 snapshot image, 4 begin, 5 middle, 6 end, 7 solo).
"""

from __future__ import annotations

from typing import Any

import pytest
from nautilus_trader.model.enums import BookAction, RecordFlag
from rithmic_nt_connect._convert import ConvertError, order_book_to_fields
from rithmic_nt_connect.data import BookAssembler

F_LAST = int(RecordFlag.F_LAST.value)
F_SNAPSHOT = int(RecordFlag.F_SNAPSHOT.value)


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
